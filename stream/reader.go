package stream

import (
	"errors"
	"fmt"
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
	StrictStart                  bool
	WrongSeqToleranceWindow      uint
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

	log.Printf("Stream Reader [%v]: making sure that previous consumer is deleted...", stream.opts.Nats.LogTag)
	err := stream.js.DeleteConsumer(stream.opts.Stream, opts.Durable)
	if err != nil && err != nats.ErrConsumerNotFound {
		log.Printf("Stream Reader [%v]: can't delete previous consumer: %v", stream.opts.Nats.LogTag, err)
	}

	log.Printf("Stream Reader [%v]: subscribing...", stream.opts.Nats.LogTag)
	r.sub, err = stream.js.PullSubscribe(
		stream.opts.Subject,
		opts.Durable,
		nats.BindStream(stream.opts.Stream),
		//nats.OrderedConsumer(),
		nats.StartSequence(startSeq),
		nats.InactiveThreshold(time.Second*time.Duration(opts.InactiveThresholdSeconds)),
	)
	if err != nil {
		log.Printf("Stream Reader [%v]: unable to subscribe: %v", stream.opts.Nats.LogTag, err)
		return nil, err
	}

	log.Printf("Stream Reader [%v]: subscribed", stream.opts.Nats.LogTag)

	log.Printf("Stream Reader [%v]: running...", stream.opts.Nats.LogTag)
	go r.run()

	return r, nil
}

func (r *Reader) Output() <-chan *ReaderOutput {
	return r.output
}

func (r *Reader) Stop() {
	log.Printf("Stream Reader [%v]: stopping...", r.stream.opts.Nats.LogTag)
	for {
		select {
		case r.stop <- true:
		case <-r.stopped:
			return
		}
	}
}

func (r *Reader) run() {
	if r.endSeq > 0 && r.startSeq >= r.endSeq {
		r.finish("finished (r.startSeq >= r.endSeq)", nil)
		return
	}

	curSeq := r.startSeq - 1
	lastSeq, err := r.getLastSeq()
	if err != nil {
		r.finish("unable to fetch LastSeq", err)
		return
	}

	requestTicker := time.NewTicker(time.Duration(float64(time.Second) / r.opts.MaxRps))
	defer requestTicker.Stop()

	fetchWait := nats.MaxWait(time.Millisecond * time.Duration(r.opts.FetchTimeoutMs))

	first := true
	consecutiveWrongSeqCount := 0
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
			if batchSize < r.opts.MaxRequestBatchSize {
				lastSeq, err = r.getLastSeq()
				if err != nil {
					r.finish("unable to fetch LastSeq", err)
					return
				}
				batchSize = r.countBatchSize(curSeq, lastSeq)
			}

			messages, err := r.sub.Fetch(int(batchSize), fetchWait)
			if err != nil {
				if curSeq >= lastSeq {
					continue
				}
				r.finish(fmt.Sprintf("unable to fetch messages (batchSize=%d)", batchSize), err)
				return
			}

			result := make([]*ReaderOutput, 0, len(messages))
			for _, msg := range messages {
				if err := msg.Ack(); err != nil {
					log.Printf("Stream Reader [%v]: can't ack message: %v", r.stream.opts.Nats.LogTag, err)
				}
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
				if (!first || r.opts.StrictStart) && res.Metadata.Sequence.Stream != curSeq+1 {
					log.Printf(
						"Stream Reader [%v]: wrong sequence detected: %v, expected %v",
						r.stream.opts.Nats.LogTag,
						res.Metadata.Sequence.Stream,
						curSeq+1,
					)
					consecutiveWrongSeqCount++
					if consecutiveWrongSeqCount >= int(r.opts.WrongSeqToleranceWindow) {
						r.finish("error", errors.New("WrongSeqToleranceWindow exceeded"))
						return
					}
					continue
				}
				curSeq = res.Metadata.Sequence.Stream
				consecutiveWrongSeqCount = 0
				first = false

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
			}
		}
	}
}

func (r *Reader) countBatchSize(curSeq uint64, lastSeq uint64) uint {
	border := lastSeq
	if r.endSeq != 0 && r.endSeq-1 < border {
		border = r.endSeq - 1
	}
	if curSeq >= border {
		return 1
	}
	residue := border - curSeq
	if residue > uint64(r.opts.MaxRequestBatchSize) {
		return r.opts.MaxRequestBatchSize
	}
	return uint(residue)
}

func (r *Reader) getLastSeq() (uint64, error) {
	info, _, err := r.stream.GetInfo(time.Second * time.Duration(r.opts.LastSeqUpdateIntervalSeconds))
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (r *Reader) finish(logMsg string, err error) {
	log.Printf("Stream Reader [%v]: %v: %v", r.stream.opts.Nats.LogTag, logMsg, err)
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
