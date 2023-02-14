package streambridge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-bridge/blockparse"
	"github.com/aurora-is-near/stream-bridge/blockwriter"
	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streambridge/metrics"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/aurora-is-near/stream-bridge/util"
)

var (
	errInputConnProblem  = errors.New("input conn problem")
	errOutputConnProblem = errors.New("output conn problem")
	errCanceled          = errors.New("canceled")
)

type StreamBridge struct {
	Mode               string
	Input              *stream.Opts
	Output             *stream.Opts
	Reader             *stream.ReaderOpts
	Writer             *blockwriter.Opts
	InputStartSequence uint64
	InputEndSequenece  uint64
	RestartDelayMs     uint
	ToleranceWindow    uint
	Metrics            *metrics.Metrics

	unverified bool
	parseBlock blockparse.ParseBlockFn
}

func (sb *StreamBridge) Run() error {
	var err error

	sb.Mode = strings.ToLower(sb.Mode)
	sb.unverified = (sb.Mode == "unverified")
	if sb.parseBlock, err = blockparse.GetParseBlockFn(sb.Mode); err != nil {
		return err
	}
	if sb.InputEndSequenece > 0 && sb.InputStartSequence >= sb.InputEndSequenece {
		return fmt.Errorf("it doesn't make sense to have InputStartSequence >= InputEndSequence")
	}
	if sb.InputEndSequenece == 1 {
		return fmt.Errorf("InputEndSequence can't be equal to 1")
	}

	if err := sb.Metrics.Start(); err != nil {
		return err
	}
	defer sb.Metrics.Stop()

	if sb.InputStartSequence > 0 {
		sb.Metrics.InputStartSeq.Set(float64(sb.InputStartSequence))
	}
	if sb.InputEndSequenece > 0 {
		sb.Metrics.InputEndSeq.Set(float64(sb.InputEndSequenece))
	}
	sb.Metrics.ToleranceWindow.Set(float64(sb.ToleranceWindow))

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	var cancelErr error
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-sb.Metrics.Closed():
			log.Printf("Metrics server has died, stopping program: %v", err)
			cancelErr = err
		case <-interrupt:
			log.Printf("Got interruption signal, stopping program...")
		}
		cancel()
	}()

	loopErr := sb.loop(ctx)
	cancel()
	if cancelErr != nil {
		return cancelErr
	}
	if loopErr == errCanceled {
		return nil
	}
	return loopErr
}

func (sb *StreamBridge) loop(ctx context.Context) error {
	var input stream.StreamWrapperInterface
	var output stream.StreamWrapperInterface

	defer func() {
		if input != nil {
			sb.Metrics.InputStream.StopObserving()
			input.Disconnect()
		}
		if output != nil {
			sb.Metrics.OutputStream.StopObserving()
			output.Disconnect()
		}
	}()

	for {
		if input == nil {
			if ctx.Err() != nil {
				return errCanceled
			}
			input, _ = stream.ConnectStream(sb.Input)
			if input != nil {
				sb.Metrics.InputStream.StartObserving(input)
			}
		}
		if output == nil {
			if ctx.Err() != nil {
				return errCanceled
			}
			output, _ = stream.ConnectStream(sb.Output)
			if output != nil {
				sb.Metrics.OutputStream.StartObserving(output)
			}
		}

		if input != nil && output != nil {
			log.Printf("Starting sync...")
			switch err := sb.sync(ctx, input, output); err {
			case errInputConnProblem:
				sb.Metrics.InputStream.StopObserving()
				input.Disconnect()
				input = nil
			case errOutputConnProblem:
				sb.Metrics.OutputStream.StopObserving()
				output.Disconnect()
				output = nil
			default:
				return err
			}
		}

		if sb.RestartDelayMs > 0 {
			log.Printf("Waiting for %vms before next connection...", sb.RestartDelayMs)
			if !util.CtxSleep(ctx, time.Millisecond*time.Duration(sb.RestartDelayMs)) {
				return errCanceled
			}
		}
	}
}

func (sb *StreamBridge) sync(ctx context.Context, input, output stream.StreamWrapperInterface) error {
	if ctx.Err() != nil {
		return errCanceled
	}

	log.Printf("Starting writer...")
	writer, tip, err := blockwriter.NewBlockWriter(sb.Writer, output, sb.parseBlock)
	if err == blockwriter.ErrCorruptedTip {
		return errors.New("unable to parse last output block, see logs for detailed error")
	}
	if err != nil {
		log.Printf("Can't start writer: %v, will restart output connection", err)
		return errOutputConnProblem
	}
	if tip != nil {
		sb.Metrics.WriterTipHeight.Set(float64(tip.Height))
	}

	if tip == nil {
		log.Printf("Current tip: absent")
	} else {
		log.Printf("Current tip: seq=%v, height=%v", tip.Sequence, tip.Height)
	}

	log.Printf("Figuring out the best input seq to start from...")
	startSeq, err := sb.calculateStartSeq(ctx, input, tip)
	if err != nil {
		return err
	}

	if ctx.Err() != nil {
		return errCanceled
	}

	log.Printf("Starting reading from seq=%d", startSeq)
	reader, err := stream.StartReader(sb.Reader, input, startSeq, sb.InputEndSequenece)
	if err != nil {
		log.Printf("Unable to start input stream reading: %v", err)
		return errInputConnProblem
	}
	defer reader.Stop()

	consecutiveWrongBlocksCount := 0
	lastBlockWasWrong := false
	for {
		if lastBlockWasWrong {
			consecutiveWrongBlocksCount++
			if consecutiveWrongBlocksCount >= int(sb.ToleranceWindow) {
				return errors.New("tolerance window exceeded")
			}
			lastBlockWasWrong = false
		} else {
			consecutiveWrongBlocksCount = 0
		}
		sb.Metrics.ConsecutiveWrongBlocks.Set(float64(consecutiveWrongBlocksCount))

		if ctx.Err() != nil {
			return errCanceled
		}

		select {
		case <-ctx.Done():
			return errCanceled
		case out, ok := <-reader.Output():
			if !ok {
				return nil
			}
			if out.Error != nil {
				log.Printf("Input reader error: %v, will restart input connection", out.Error)
				return errInputConnProblem
			}
			sb.Metrics.ReadsCount.Inc()
			sb.Metrics.ReaderSeq.Set(float64(out.Metadata.Sequence.Stream))

			if sb.InputStartSequence > 0 && out.Metadata.Sequence.Stream < sb.InputStartSequence {
				sb.Metrics.LowSeqSkipsCount.Inc()
				continue
			}

			block, err := sb.parseBlock(out.Msg.Data, out.Msg.Header)
			if err != nil {
				log.Printf("Can't parse input block at seq %d: %v", out.Metadata.Sequence.Stream, err)
				sb.Metrics.CorruptedBlockSkips.Inc()
				lastBlockWasWrong = true
				continue
			}
			sb.Metrics.ReaderHeight.Set(float64(block.Height))

			ack, err := writer.Write(ctx, block, out.Msg.Data)
			if tip, _, _ = writer.GetTip(time.Hour * 24 * 365); tip != nil {
				sb.Metrics.WriterTipHeight.Set(float64(tip.Height))
			}
			switch err {
			case nil:
				sb.Metrics.WritesCount.Inc()
				sb.Metrics.LastWrittenHeight.Set(float64(block.Height))
				sb.Metrics.LastWrittenInputSeq.Set(float64(out.Metadata.Sequence.Stream))
				sb.Metrics.LastWrittenOutputSeq.Set(float64(ack.Sequence))
			case blockwriter.ErrCanceled:
				return errCanceled
			case blockwriter.ErrCorruptedTip:
				return errors.New("unable to parse last output block, see logs for detailed error")
			case blockwriter.ErrLowHeight:
				sb.Metrics.LowHeightSkipsCount.Inc()
			case blockwriter.ErrHashMismatch:
				sb.Metrics.HashMismatchSkipsCount.Inc()
				lastBlockWasWrong = true
			default:
				log.Printf("Output writer error: %v, will restart output connection", err)
				return errOutputConnProblem
			}
		}
	}
}

func (sb *StreamBridge) calculateStartSeq(ctx context.Context, input stream.StreamWrapperInterface, lastOutputBlock *types.AbstractBlock) (uint64, error) {
	log.Printf("Figuring out the best input seq to start from...")

	if ctx.Err() != nil {
		return 0, errCanceled
	}

	inputInfo, _, err := input.GetInfo(0)
	if err != nil {
		log.Printf("Unable to get input stream info: %v, will restart input connection", err)
		return 0, errInputConnProblem
	}
	if inputInfo.State.LastSeq == 0 {
		return 1, nil
	}

	lowerBound := inputInfo.State.FirstSeq
	if sb.InputStartSequence > 0 {
		lowerBound = sb.InputStartSequence
		if sb.InputStartSequence < inputInfo.State.FirstSeq {
			log.Printf(
				"Warning: InputStartSequence (%d) < inputInfo.State.FirstSeq (%d)",
				sb.InputStartSequence,
				inputInfo.State.FirstSeq,
			)
			lowerBound = inputInfo.State.FirstSeq
		}
		if sb.InputStartSequence > inputInfo.State.LastSeq {
			log.Printf(
				"Warning: InputStartSequence (%d) > inputInfo.State.LastSeq (%d)",
				sb.InputStartSequence,
				inputInfo.State.LastSeq,
			)
			lowerBound = inputInfo.State.LastSeq
		}
	}

	upperBound := inputInfo.State.LastSeq
	if sb.InputEndSequenece > 0 {
		upperBound = sb.InputEndSequenece - 1
		if sb.InputEndSequenece-1 < inputInfo.State.FirstSeq {
			log.Printf(
				"Warning: InputEndSequenece-1 (%d) < inputInfo.State.FirstSeq (%d)",
				sb.InputEndSequenece-1,
				inputInfo.State.FirstSeq,
			)
			upperBound = inputInfo.State.FirstSeq
		}
		if sb.InputEndSequenece-1 > inputInfo.State.LastSeq {
			log.Printf(
				"Warning: InputEndSequenece-1 (%d) > inputInfo.State.LastSeq (%d)",
				sb.InputEndSequenece-1,
				inputInfo.State.LastSeq,
			)
			upperBound = inputInfo.State.LastSeq
		}
	}

	log.Printf("lowerBound=%d, upperBound=%d", lowerBound, upperBound)

	if lastOutputBlock == nil {
		return lowerBound, nil
	}

	cur := upperBound
	for {
		if ctx.Err() != nil {
			return 0, errCanceled
		}

		log.Printf("Checking the height of the block (seq=%d)", cur)
		block, _, err := sb.getBlock(input, cur)
		if err != nil {
			log.Printf("Falling back to lower bound")
			return lowerBound, nil
		}

		if block.Height <= lastOutputBlock.Height {
			break
		}

		maxJump := cur - lowerBound
		jump := util.Min(block.Height-lastOutputBlock.Height, maxJump)
		log.Printf("Block (seq=%d) height is too high, will jump %d seqs down", cur, jump)
		cur -= jump
	}

	return cur, nil
}

func (sb *StreamBridge) getBlock(stream stream.StreamWrapperInterface, seq uint64) (block *types.AbstractBlock, corrupted bool, err error) {
	msg, err := stream.Get(seq)
	if err != nil {
		log.Printf("Unable to fetch block (seq=%d): %v", seq, err)
		return nil, false, err
	}

	b, err := sb.parseBlock(msg.Data, msg.Header)
	if err != nil {
		log.Printf("Unable to parse block (seq=%d): %v", seq, err)
		return nil, true, err
	}

	return b, false, nil
}
