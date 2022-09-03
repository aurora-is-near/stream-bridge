package streambridge

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-bridge/stream"
	"github.com/aurora-is-near/stream-bridge/streambridge/metrics"
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/nats-io/nats.go"
)

type StreamBridge struct {
	Mode                     string
	Input                    *stream.Stream
	Output                   *stream.Stream
	Reader                   *stream.ReaderOpts
	InputStartSequence       uint64
	InputEndSequenece        uint64
	RestartDelayMs           uint
	ForceRestartAfterSeconds uint
	ToleranceWindow          uint
	MaxPushAttempts          uint
	PushRetryWaitMs          uint
	Metrics                  *metrics.Metrics

	blockParseFn func(data []byte) (*types.AbstractBlock, error)
	stop         chan bool
	stopped      chan error
}

func (sb *StreamBridge) defineBlockParseFn() error {
	sb.Mode = strings.ToLower(sb.Mode)
	switch sb.Mode {
	case "near":
		sb.blockParseFn = func(data []byte) (*types.AbstractBlock, error) {
			block, err := types.DecodeNearBlock(data)
			if err != nil {
				return nil, err
			}
			return block.ToAbstractBlock(), nil
		}
	case "aurora":
		sb.blockParseFn = func(data []byte) (*types.AbstractBlock, error) {
			block, err := types.DecodeAuroraBlock(data)
			if err != nil {
				return nil, err
			}
			return block.ToAbstractBlock(), nil
		}
	default:
		return fmt.Errorf("mode should be one of ['near', 'aurora']")
	}
	return nil
}

func (sb *StreamBridge) Run() error {
	if err := sb.defineBlockParseFn(); err != nil {
		return err
	}

	if sb.InputEndSequenece > 0 && sb.InputStartSequence >= sb.InputEndSequenece {
		return fmt.Errorf("it doesn't make sense to have InputStartSequence >= InputEndSequence")
	}
	if sb.InputEndSequenece == 1 {
		return fmt.Errorf("InputEndSequence can't be equal to 1")
	}

	if sb.MaxPushAttempts == 0 {
		sb.MaxPushAttempts = 3
	}

	if err := sb.Metrics.Start(); err != nil {
		return err
	}
	defer sb.Metrics.Stop()

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	sb.stop = make(chan bool)
	sb.stopped = make(chan error)

	go sb.loop()

	select {
	case err := <-sb.Metrics.Closed():
		log.Printf("Metrics server has died, stopping program: %v", err)
		for {
			select {
			case sb.stop <- true:
			case <-sb.stopped:
				return err
			}
		}
	case <-interrupt:
		log.Printf("Got interruption signal, stopping program...")
		for {
			select {
			case sb.stop <- true:
			case <-sb.stopped:
				return nil
			}
		}
	case err, ok := <-sb.stopped:
		if ok {
			return err
		}
		return nil
	}
}

func (sb *StreamBridge) loop() {
	for first := true; true; first = false {
		if sb.checkStop() {
			close(sb.stopped)
			return
		}

		if !first {
			log.Printf("Waiting for %vms before next start...", sb.RestartDelayMs)
			timer := time.NewTimer(time.Millisecond * time.Duration(sb.RestartDelayMs))
			select {
			case <-sb.stop:
				timer.Stop()
				close(sb.stopped)
				return
			case <-timer.C:
			}
		}

		log.Printf("Starting...")
		finished, err := sb.run()
		if err != nil {
			sb.stopped <- err
		}
		if err != nil || finished {
			close(sb.stopped)
			return
		}
	}
}

func (sb *StreamBridge) run() (bool, error) {
	var err error

	if sb.checkStop() {
		return true, nil
	}

	var forceRestart <-chan time.Time
	if sb.ForceRestartAfterSeconds != 0 {
		forceRestartTimer := time.NewTimer(time.Second * time.Duration(sb.ForceRestartAfterSeconds))
		defer forceRestartTimer.Stop()
		forceRestart = forceRestartTimer.C
	} else {
		forceRestart = make(chan time.Time)
	}

	inputInfo, inputErr := sb.Input.Connect()
	defer sb.Input.Disconnect()
	sb.Metrics.InputStreamConnected.Set(boolMetric(inputErr == nil))

	if sb.checkStop() {
		return true, nil
	}

	outputInfo, outputErr := sb.Output.Connect()
	sb.Metrics.OutputStreamConnected.Set(boolMetric(outputErr == nil))
	defer sb.Output.Disconnect()

	if sb.checkStop() {
		return true, nil
	}

	if inputErr != nil || outputErr != nil {
		return false, nil
	}

	log.Printf("Checking output stream state...")
	sb.Metrics.OutputStreamSequenceNumber.Set(float64(outputInfo.State.LastSeq))
	var lastPushedBlock *types.AbstractBlock
	if outputInfo.State.LastSeq != 0 {
		lastPushedMsg, err := sb.Output.Get(outputInfo.State.LastSeq)
		if err != nil {
			log.Printf("Unable to get last output block (seq=%d): %v", outputInfo.State.LastSeq, err)
			return false, nil
		}
		lastPushedBlock, err = sb.blockParseFn(lastPushedMsg.Data)
		if err != nil {
			err := fmt.Errorf("unable to parse last output block (seq=%d): %v", outputInfo.State.LastSeq, err)
			log.Print(err)
			return false, err
		}
		sb.Metrics.OutputStreamBlockHeight.Set(float64(lastPushedBlock.Height))
	}

	if sb.checkStop() {
		return true, nil
	}

	startSeq := sb.calculateStartSeq(inputInfo, lastPushedBlock)

	if sb.checkStop() {
		return true, nil
	}

	log.Printf("Starting reading from seq=%d", startSeq)
	reader, err := stream.StartReader(sb.Reader, sb.Input, startSeq, sb.InputEndSequenece)
	if err != nil {
		log.Printf("Unable to start input stream reading: %v", err)
		return false, nil
	}
	defer reader.Stop()

	consecutiveWrongBlocks := uint(0)
	for {
		// Prioritized stop check
		select {
		case <-sb.stop:
			return true, nil
		case <-forceRestart:
			log.Printf("Doing forced restart")
			return false, nil
		default:
		}

		select {
		case <-sb.stop:
			return true, nil
		case <-forceRestart:
			log.Printf("Doing forced restart")
			return false, nil
		case out, ok := <-reader.Output():
			if !ok {
				log.Printf("Finished")
				return true, nil
			}
			if out.Error != nil {
				log.Printf("Input reading error: %v", out.Error)
				return false, nil
			}

			seq := out.Metadata.Sequence.Stream
			sb.Metrics.InputStreamSequenceNumber.Set(float64(seq))
			if sb.InputStartSequence > 0 && seq < sb.InputStartSequence {
				sb.Metrics.CatchUpSkips.Inc()
				consecutiveWrongBlocks = 0
				continue
			}

			wrongBlock := false
			block, err := sb.blockParseFn(out.Msg.Data)
			if err != nil {
				sb.Metrics.CorruptedSkips.Inc()
				log.Printf("Found corrupted block at input seq=%d: %v", seq, err)
				wrongBlock = true
			} else {
				sb.Metrics.InputStreamBlockHeight.Set(float64(block.Height))
				if lastPushedBlock != nil {
					if block.Height <= lastPushedBlock.Height {
						sb.Metrics.CatchUpSkips.Inc()
						consecutiveWrongBlocks = 0
						continue
					}
					if block.PrevHash != lastPushedBlock.Hash {
						sb.Metrics.HashMismatchSkips.Inc()
						wrongBlock = true
					}
				}
			}
			if wrongBlock {
				consecutiveWrongBlocks++
				if consecutiveWrongBlocks > sb.ToleranceWindow {
					err := fmt.Errorf("tolerance window exceeded")
					log.Println(err)
					return false, err
				}
				continue
			}
			consecutiveWrongBlocks = 0

			pushed := false
			for i := 1; i <= int(sb.MaxPushAttempts); i++ {
				// Prioritized stop check
				select {
				case <-sb.stop:
					return true, nil
				case <-forceRestart:
					log.Printf("Doing forced restart")
					return false, nil
				default:
				}

				if i > 1 && sb.PushRetryWaitMs > 0 {
					log.Printf("Waiting for %dms before next push retry", sb.PushRetryWaitMs)
					pushRetryTimer := time.NewTimer(time.Millisecond * time.Duration(sb.PushRetryWaitMs))
					select {
					case <-sb.stop:
						pushRetryTimer.Stop()
						return true, nil
					case <-forceRestart:
						pushRetryTimer.Stop()
						log.Printf("Doing forced restart")
						return false, nil
					case <-pushRetryTimer.C:
					}
				}

				ack, err := sb.Output.Write(out.Msg.Data, strconv.FormatUint(block.Height, 10))
				if err == nil {
					sb.Metrics.OutputStreamSequenceNumber.Set(float64(ack.Sequence))
					sb.Metrics.OutputStreamBlockHeight.Set(float64(block.Height))
					lastPushedBlock = block
					pushed = true
					break
				}
				log.Printf(
					"Push (input_seq=%d, height=%d) [Attempt: %d/%d] did not succeed: %v",
					seq,
					block.Height,
					i,
					sb.MaxPushAttempts,
					err,
				)
			}

			if !pushed {
				log.Printf("Unable to push block after %d retries", sb.MaxPushAttempts)
				return false, nil
			}
		}
	}
}

func (sb *StreamBridge) checkStop() bool {
	select {
	case <-sb.stop:
		return true
	default:
	}
	return false
}

func (sb *StreamBridge) calculateStartSeq(inputInfo *nats.StreamInfo, lastPushedBlock *types.AbstractBlock) uint64 {
	log.Printf("Figuring out the best input seq to start from...")

	if inputInfo.State.LastSeq == 0 {
		return 1
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

	if lastPushedBlock == nil {
		return lowerBound
	}

	log.Printf("Checking the height of the lowerBound block (seq=%d)", lowerBound)
	msg, err := sb.Input.Get(lowerBound)
	if err != nil {
		log.Printf("Unable to get input block (seq=%d), will fall back to lower bound: %v", lowerBound, err)
		return lowerBound
	}
	block, err := sb.blockParseFn(msg.Data)
	if err != nil {
		log.Printf("Unable to parse input block (seq=%d), will fall back to lower bound: %v", lowerBound, err)
		return lowerBound
	}

	if block.Height > lastPushedBlock.Height {
		return lowerBound
	}

	startSeq := Min(lowerBound+(lastPushedBlock.Height+1-block.Height), upperBound)

	for startSeq > lowerBound {
		if sb.checkStop() {
			return lowerBound
		}

		log.Printf("Checking the height of the input block (seq=%d)", startSeq)
		msg, err = sb.Input.Get(startSeq)
		if err != nil {
			log.Printf("Unable to get input block (seq=%d), will fall back to lower bound: %v", startSeq, err)
			return lowerBound
		}
		block, err = sb.blockParseFn(msg.Data)
		if err != nil {
			log.Printf("Unable to parse input block (seq=%d), will fall back to lower bound: %v", startSeq, err)
			return lowerBound
		}

		if block.Height <= lastPushedBlock.Height+1 {
			return startSeq
		}

		maxJump := startSeq - lowerBound
		jump := Min(block.Height-(lastPushedBlock.Height+1), maxJump)
		log.Printf("Block (seq=%d) height is too high, will jump %d seqs down", startSeq, jump)
		startSeq -= jump
	}

	return startSeq
}
