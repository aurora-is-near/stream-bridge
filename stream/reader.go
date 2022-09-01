package stream

import (
	"log"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
)

const readerMinRps = 0.001

type ReaderOpts struct {
	MaxRps                       float64
	BufferSize                   uint
	MaxRequestBatchSize          uint
	SubscribeAckWaitMs           uint
	InactiveThresholdSeconds     uint
	FetchTimeoutMs               uint
	SortBatch                    bool
	LastSeqUpdateIntervalSeconds uint
	Durable                      string
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

	stop    chan bool
	stopped chan bool
	output  chan *ReaderOutput
	sub     *nats.Subscription
}

func (opts ReaderOpts) FillMissingFields() *ReaderOpts {
	if opts.MaxRps < readerMinRps {
		opts.MaxRps = readerMinRps
	}
	if opts.MaxRequestBatchSize == 0 {
		opts.MaxRequestBatchSize = 100
	}
	if opts.SubscribeAckWaitMs == 0 {
		opts.SubscribeAckWaitMs = 5000
	}
	if opts.InactiveThresholdSeconds == 0 {
		opts.InactiveThresholdSeconds = 300
	}
	if opts.FetchTimeoutMs == 0 {
		opts.FetchTimeoutMs = 10000
	}
	if opts.LastSeqUpdateIntervalSeconds == 0 {
		opts.LastSeqUpdateIntervalSeconds = 5
	}
	return &opts
}

func StartReader(opts *ReaderOpts, stream *Stream, startSeq uint64, endSeq uint64) (*Reader, error) {
	opts = opts.FillMissingFields()

	if startSeq < 1 {
		startSeq = 1
	}
	r := &Reader{
		opts:     opts,
		stream:   stream,
		startSeq: startSeq,
		endSeq:   endSeq,
		stop:     make(chan bool),
		stopped:  make(chan bool),
		output:   make(chan *ReaderOutput, opts.BufferSize),
	}

	log.Printf("Stream Reader [%v]: subscribing...", stream.Nats.LogTag)
	var err error
	r.sub, err = stream.js.PullSubscribe(
		stream.Subject,
		opts.Durable,
		nats.BindStream(stream.Stream),
		//nats.OrderedConsumer(),
		nats.StartSequence(startSeq),
		nats.InactiveThreshold(time.Second*time.Duration(opts.InactiveThresholdSeconds)),
	)
	if err != nil {
		log.Printf("Stream Reader [%v]: unable to subscribe: %v", stream.Nats.LogTag, err)
		return nil, err
	}

	log.Printf("Stream Reader [%v]: subscribed", stream.Nats.LogTag)

	log.Printf("Stream Reader [%v]: running...", stream.Nats.LogTag)
	go r.run()

	return r, nil
}

func (r *Reader) Output() <-chan *ReaderOutput {
	return r.output
}

func (r *Reader) Stop() {
	log.Printf("Stream Reader [%v]: stopping...", r.stream.Nats.LogTag)
	for {
		select {
		case r.stop <- true:
		case <-r.stopped:
			return
		}
	}
}

func (r *Reader) run() {
	curSeq := r.startSeq - 1
	lastSeq, err := r.getLastSeq()
	lastSeqUpdateTime := time.Now()
	if err != nil {
		r.finish("unable to fetch LastSeq", err)
		return
	}

	requestTicker := time.NewTicker(time.Duration(float64(time.Second) / r.opts.MaxRps))
	defer requestTicker.Stop()

	lastSeqUpdateInterval := time.Second * time.Duration(r.opts.LastSeqUpdateIntervalSeconds)
	fetchWait := nats.MaxWait(time.Millisecond * time.Duration(r.opts.FetchTimeoutMs))

	for {
		// Prioritized stop check
		select {
		case <-r.stop:
			r.finish("stopped", nil)
			return
		default:
		}

		select {
		case <-r.stop:
			r.finish("stopped", nil)
			return
		case <-requestTicker.C:
			batchSize := r.countBatchSize(curSeq, lastSeq)
			if batchSize < r.opts.MaxRequestBatchSize && time.Since(lastSeqUpdateTime) > lastSeqUpdateInterval {
				lastSeq, err = r.getLastSeq()
				lastSeqUpdateTime = time.Now()
				if err != nil {
					r.finish("unable to fetch LastSeq", err)
					return
				}
			} else {
				messages, err := r.sub.Fetch(int(batchSize), fetchWait)
				if err != nil {
					r.finish("unable to fetch messages", err)
					return
				}

				result := make([]*ReaderOutput, 0, len(messages))
				for _, msg := range messages {
					meta, err := msg.Metadata()
					if err != nil {
						r.finish("unable to parse message metadata", err)
						return
					}
					result = append(result, &ReaderOutput{
						Msg:      msg,
						Metadata: meta,
					})
				}

				if r.opts.SortBatch {
					sort.Slice(result, func(i, j int) bool {
						return result[i].Metadata.Sequence.Stream < result[j].Metadata.Sequence.Stream
					})
				}

				for _, res := range result {
					if r.endSeq > 0 && res.Metadata.Sequence.Stream >= r.endSeq {
						r.finish("finished", nil)
						return
					}

					// Prioritized stop check
					select {
					case <-r.stop:
						r.finish("stopped", nil)
						return
					default:
					}

					select {
					case <-r.stop:
						r.finish("stopped", nil)
						return
					case r.output <- res:
					}

					if r.endSeq > 0 && res.Metadata.Sequence.Stream == r.endSeq-1 {
						r.finish("finished", nil)
						return
					}
					if res.Metadata.Sequence.Stream > curSeq {
						curSeq = res.Metadata.Sequence.Stream
					}
				}
			}
		}
	}
}

func (r *Reader) countBatchSize(curSeq uint64, lastSeq uint64) uint {
	border := lastSeq
	if r.endSeq != 0 && r.endSeq-1 < border {
		border = r.endSeq - 1
	}
	residue := border - curSeq

	batchSize := r.opts.MaxRequestBatchSize
	if uint64(batchSize) > residue {
		batchSize = uint(residue)
	}
	if batchSize < 1 {
		batchSize = 1
	}

	return batchSize
}

func (r *Reader) getLastSeq() (uint64, error) {
	info, err := r.stream.GetStreamInfo()
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (r *Reader) finish(logMsg string, err error) {
	log.Printf("Stream Reader [%v]: %v: %v", r.stream.Nats.LogTag, logMsg, err)
	if err != nil {
		out := &ReaderOutput{
			Error: err,
		}
		select {
		case <-r.stop:
		case r.output <- out:
		}
	}
	close(r.output)
	r.sub.Unsubscribe()
	close(r.stopped)
}
