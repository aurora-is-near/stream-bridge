package stream

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/nats-io/nats.go"
)

const readerMinRps = 0.1
const minPushSubLifetime = time.Minute

var errStopped = errors.New("stopped")

type ReaderOpts struct {
	MaxRps                       float64
	BufferSize                   uint
	MaxRequestBatchSize          uint
	FetchTimeoutMs               uint
	LastSeqUpdateIntervalSeconds uint
	Durable                      string
	StrictStart                  bool
	MaxSilenceSeconds            uint
}

type ReaderOutput struct {
	Msg      *nats.Msg
	Metadata *nats.MsgMetadata
	Error    error
}

type Reader struct {
	opts     *ReaderOpts
	stream   *Stream
	startSeq uint64
	endSeq   uint64

	pullTick              time.Duration
	fetchTimeout          time.Duration
	lastSeqUpdateInterval time.Duration
	maxSilence            time.Duration

	stopper *util.Stopper
	output  chan *ReaderOutput

	pullSub    *nats.Subscription
	pullTicker *time.Ticker

	pushSub     *nats.Subscription
	pushChan    chan *nats.Msg
	pushSubTime time.Time
}

func (opts ReaderOpts) FillMissingFields() *ReaderOpts {
	if opts.MaxRps < readerMinRps {
		opts.MaxRps = readerMinRps
	}
	if opts.MaxRequestBatchSize == 0 {
		opts.MaxRequestBatchSize = 100
	}
	if opts.FetchTimeoutMs == 0 {
		opts.FetchTimeoutMs = 10000
	}
	if opts.LastSeqUpdateIntervalSeconds == 0 {
		opts.LastSeqUpdateIntervalSeconds = 5
	}
	if opts.MaxSilenceSeconds == 0 {
		opts.MaxSilenceSeconds = 10
	}
	return &opts
}

func StartReader(opts *ReaderOpts, stream *Stream, startSeq uint64, endSeq uint64) (*Reader, error) {
	opts = opts.FillMissingFields()

	r := &Reader{
		opts:     opts,
		stream:   stream,
		startSeq: util.Max(startSeq, 1),
		endSeq:   endSeq,

		pullTick:              time.Duration(float64(time.Second) / opts.MaxRps),
		fetchTimeout:          time.Duration(opts.FetchTimeoutMs) * time.Millisecond,
		lastSeqUpdateInterval: time.Duration(opts.LastSeqUpdateIntervalSeconds) * time.Second,
		maxSilence:            time.Duration(opts.MaxSilenceSeconds) * time.Second,

		stopper: util.NewStopper(),
		output:  make(chan *ReaderOutput, opts.BufferSize),
	}

	r.log("running...")
	r.stopper.Add(1)
	go r.run()

	return r, nil
}

func (r *Reader) Output() <-chan *ReaderOutput {
	return r.output
}

func (r *Reader) Stop() {
	r.log("stopping...")
	r.stopper.Stop()
}

func (r *Reader) run() {
	defer r.stopper.Done()
	defer close(r.output)
	defer func() {
		if r.pullSub != nil {
			r.pullUnsubscribe()
		}
		if r.pushSub != nil {
			r.pushUnsubscribe()
		}
	}()

	silence := false
	var silenceStart time.Time

	nextSeq := r.startSeq
	first := true
	for {
		if r.endSeq > 0 && nextSeq >= r.endSeq {
			r.log("finished")
			return
		}

		if r.stopper.IsStopped() {
			r.log("stopped")
			return
		}

		numUsefulPending, err := r.countNumUsefulPending(nextSeq)
		if err != nil {
			r.sendErr(fmt.Errorf("unable to count useful pending: %w", err))
			return
		}

		batch, err := r.readNext(nextSeq, numUsefulPending)
		if err == errStopped {
			r.log("stopped")
			return
		}
		if err != nil {
			r.sendErr(fmt.Errorf("unable to obtain next message: %w", err))
			return
		}

		parsedBatch := make([]*ReaderOutput, len(batch))
		for i, msg := range batch {
			meta, err := msg.Metadata()
			if err != nil {
				r.sendErr(fmt.Errorf("unable to parse message metadata: %w", err))
				return
			}
			parsedBatch[i] = &ReaderOutput{
				Msg:      msg,
				Metadata: meta,
			}
		}
		sort.Slice(parsedBatch, func(i, j int) bool {
			return parsedBatch[i].Metadata.Sequence.Stream < parsedBatch[j].Metadata.Sequence.Stream
		})

		filteredBatch := []*ReaderOutput{}
		for _, item := range parsedBatch {
			if (!first || r.opts.StrictStart) && item.Metadata.Sequence.Stream != nextSeq {
				continue
			}
			if err := item.Msg.Ack(); err != nil {
				r.log("unable to ack message [seq=%d] (will ignore): %v", nextSeq, err)
			}
			first = false
			if r.endSeq == 0 || nextSeq < r.endSeq {
				filteredBatch = append(filteredBatch, item)
			}
			nextSeq++
		}

		if len(filteredBatch) == 0 && numUsefulPending > 0 {
			if silence {
				if time.Since(silenceStart) > r.maxSilence {
					r.sendErr(fmt.Errorf("max silence exceeded"))
					return
				}
			} else {
				silence = true
				silenceStart = time.Now()
			}
		} else {
			silence = false
		}

		for _, item := range filteredBatch {
			if r.stopper.IsStopped() {
				r.log("stopped")
				return
			}
			select {
			case <-r.stopper.Stopped():
				r.log("stopped")
				return
			case r.output <- item:
			}
		}
	}
}

func (r *Reader) readNext(nextSeq uint64, numUsefulPending uint64) ([]*nats.Msg, error) {
	if r.stopper.IsStopped() {
		return nil, errStopped
	}

	enoughForBatch := numUsefulPending >= uint64(r.opts.MaxRequestBatchSize)
	pushSubLock := r.pushSub != nil && time.Since(r.pushSubTime) < minPushSubLifetime
	if r.pullSub == nil && enoughForBatch && !pushSubLock {
		if r.pushSub != nil {
			r.pushUnsubscribe()
		}
		if err := r.pullSubscribe(nextSeq); err != nil {
			return nil, err
		}
	}
	if r.pushSub == nil && !enoughForBatch {
		if r.pullSub != nil {
			r.pullUnsubscribe()
		}
		if err := r.pushSubscribe(nextSeq); err != nil {
			return nil, err
		}
	}

	if r.stopper.IsStopped() {
		return nil, errStopped
	}

	if r.pullSub != nil {
		return r.readPull(numUsefulPending)
	} else {
		return r.readPush(numUsefulPending)
	}
}

func (r *Reader) readPull(numUsefulPending uint64) ([]*nats.Msg, error) {
	batchSize := int(util.Max(util.Min(numUsefulPending, uint64(r.opts.MaxRequestBatchSize)), 1))
	select {
	case <-r.stopper.Stopped():
		return nil, errStopped
	case <-r.pullTicker.C:
	}
	if r.stopper.IsStopped() {
		return nil, errStopped
	}
	batch, err := r.pullSub.Fetch(batchSize, nats.MaxWait(r.fetchTimeout))
	if err != nil && numUsefulPending == 0 {
		return []*nats.Msg{}, nil
	}
	return batch, err
}

func (r *Reader) readPush(numUsefulPending uint64) ([]*nats.Msg, error) {
	timeoutTimer := time.NewTimer(r.fetchTimeout)
	defer timeoutTimer.Stop()
	batch := make([]*nats.Msg, 1)
	select {
	case <-r.stopper.Stopped():
		return nil, errStopped
	case <-timeoutTimer.C:
		if numUsefulPending == 0 {
			return []*nats.Msg{}, nil
		}
		return nil, fmt.Errorf("push reader timeout")
	case batch[0] = <-r.pushChan:
	}

	for i := 1; i < int(r.opts.MaxRequestBatchSize); i++ {
		if r.stopper.IsStopped() {
			return nil, errStopped
		}
		var msg *nats.Msg
		select {
		case msg = <-r.pushChan:
			batch = append(batch, msg)
		default:
			return batch, nil
		}
	}
	return batch, nil
}

func (r *Reader) deleteConsumer() error {
	r.log("making sure that previous consumer is deleted...")
	err := r.stream.js.DeleteConsumer(r.stream.opts.Stream, r.opts.Durable)
	if err != nil && err != nats.ErrConsumerNotFound {
		return fmt.Errorf("can't delete previous consumer: %w", err)
	}
	return nil
}

func (r *Reader) pullSubscribe(startSeq uint64) error {
	r.log("pull-subscribing...")
	err := r.deleteConsumer()
	if err != nil {
		return fmt.Errorf("unable to pull-subscribe: %w", err)
	}
	r.log("sending pull-subscribe request [startSeq=%v]...", startSeq)
	r.pullSub, err = r.stream.js.PullSubscribe(
		r.stream.opts.Subject,
		r.opts.Durable,
		nats.BindStream(r.stream.opts.Stream),
		nats.StartSequence(startSeq),
	)
	if err != nil {
		return fmt.Errorf("unable to pull-subscribe: %w", err)
	}
	r.pullTicker = time.NewTicker(r.pullTick)
	r.log("pull-subscribed")
	return nil
}

func (r *Reader) pullUnsubscribe() {
	r.log("pull-unsubscribing...")
	if err := r.pullSub.Unsubscribe(); err != nil {
		r.log("unable to pull-unsubscribe (will ignore): %v", err)
	} else {
		r.log("pull-unsubscribed")
	}
	r.pullSub = nil
	r.pullTicker.Stop()
	r.pullTicker = nil
}

func (r *Reader) pushSubscribe(startSeq uint64) error {
	r.log("push-subscribing...")
	err := r.deleteConsumer()
	if err != nil {
		return fmt.Errorf("unable to push-subscribe: %w", err)
	}
	r.pushChan = make(chan *nats.Msg, r.opts.MaxRequestBatchSize)
	r.log("sending push-subscribe request [startSeq=%v]...", startSeq)
	r.pushSub, err = r.stream.js.ChanSubscribe(
		r.stream.opts.Subject,
		r.pushChan,
		nats.BindStream(r.stream.opts.Stream),
		nats.Durable(r.opts.Durable),
		nats.StartSequence(startSeq),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		return fmt.Errorf("unable to push-subscribe: %w", err)
	}
	r.pushSubTime = time.Now()
	r.log("push-subscribed")
	return nil
}

func (r *Reader) pushUnsubscribe() {
	r.log("push-unsubscribing...")
	if err := r.pushSub.Unsubscribe(); err != nil {
		r.log("unable to push-unsubscribe (will ignore): %v", err)
	} else {
		r.log("push-unsubscribed")
	}
	r.pushSub = nil
	r.pushChan = nil
}

func (r *Reader) getLastSeq() (uint64, error) {
	info, _, err := r.stream.GetInfo(r.lastSeqUpdateInterval)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (r *Reader) countNumUsefulPending(nextExpectedSeq uint64) (uint64, error) {
	border, err := r.getLastSeq()
	if err != nil {
		return 0, fmt.Errorf("unable to fetch LastSeq: %w", err)
	}
	if r.endSeq > 0 && r.endSeq-1 < border {
		border = r.endSeq - 1
	}
	if border > nextExpectedSeq {
		return border - nextExpectedSeq, nil
	}
	return 0, nil
}

func (r *Reader) sendErr(err error) {
	r.log("%v", err)
	select {
	case <-r.stopper.Stopped():
	case r.output <- &ReaderOutput{Error: err}:
	}
}

func (r *Reader) log(format string, v ...any) {
	r.stream.log(fmt.Sprintf("reader [%s]: ", r.opts.Durable)+format, v...)
}
