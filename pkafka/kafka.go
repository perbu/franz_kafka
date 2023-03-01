// Package pkafka provides a kafka producer that tries really hard never to lose messages.
// It'll retry messages that fail to send, and will buffer messages to disk, so we're sure we're not loosing them.
package pkafka

/*
 Messages flow like this:

 When the producer is called it'll just push the message into a buffered channel. This will make it async.
 On the other side of the channel there is a goroutine that will read the messages, in order, and send them to kafka.
 It'll batch up messages and send them to kafka every second.

 If a batch fails to send, it'll retry it for about 20 seconds. If it still fails it'll assume kafka is down and
 will switch to storing messages on GCS.

 It'll keep the first message in memory, and will keep on retrying endlessly.

 If it succeeds it'll remove the first message from disk and then re-ingest the next message from disk.

*/

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync/atomic"
	"time"
)

type Client struct {
	c            *kgo.Client
	failed       atomic.Bool
	failedSince  time.Time
	lastAttempt  time.Time
	persistence  *persistence
	buffer       *buffer
	syncInterval time.Duration
	msgCount     *uint64
	bufferCh     chan *kgo.Record
}

var (
	// ErrKafkaDown is returned when kafka is down.
	ErrKafkaDown = fmt.Errorf("kafka is down")
)

// New creates a new kafka client.
// It will apply the options you pass to it.
// See kgo.NewClient for more info on the options.
func New(opts ...kgo.Opt) (*Client, error) {
	producerId := "perbu"
	opts = append(opts, kgo.TransactionalID(producerId))
	k, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("initializing kafka client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = k.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("pinging kafka: %w", err)
	}
	p, err := newPersistence()
	if err != nil {
		return nil, fmt.Errorf("initializing persistence: %w", err)
	}
	bu := &buffer{
		records: make([]*kgo.Record, 0),
	}
	s := Client{
		c:            k,
		persistence:  p,
		buffer:       bu,
		msgCount:     new(uint64),
		syncInterval: time.Second * 1,
		bufferCh:     make(chan *kgo.Record, 1000),
	}
	s.failed.Store(true) // default to failed, we've passed the ping test, after all.
	return &s, nil
}

// Run starts the goroutine that will flush the buffer to kafka or GCS.
// this will reduce the need to locking, and will make the producer async.
func (c *Client) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()
	// defer c.c.Close()
	for {
		select {
		case <-ctx.Done():
			c.flush()
			return nil
		case msg := <-c.bufferCh:
			c.buffer.add(msg)
		case <-ticker.C:
			c.flush()
		}
	}
}

func (c *Client) PersistentProduce(msg kgo.Record) error {
	if msg.ProducerID != 0 {
		return fmt.Errorf("producer id must be 0")
	}
	c.bufferCh <- &msg
	return nil
}

func (c *Client) flush() {
	if c.buffer.size() == 0 {
		log.Println("flush: nothing to flush")
		return
	}
	err := c.flushToKafka()
	if err != nil {
		c.flushToGCS() // we don't care about the error here, we'll just try again next time.
	}

}

type promise func(r *kgo.Record, err error)

func (c *Client) flushToKafka() error {

	// if we're down, and it's been less than 30 seconds since the last attempt, return an error
	if c.failed.Load() && time.Since(c.lastAttempt) < time.Second*30 {
		return ErrKafkaDown
	}

	msgCount := c.buffer.size()
	log.Printf("flushing %d msgs to kafka", msgCount)
	start := time.Now()
	defer log.Printf("flushing to kafka took %v", time.Since(start))
	msgs := c.buffer.records

	retCh := make(chan error, msgCount)

	err := c.c.BeginTransaction()
	if err != nil {
		log.Println("beginning transaction: ", err)
		return fmt.Errorf("beginning transaction: %w", err)
	}

	for i, msg := range msgs {
		log.Println("submitting msg to kafka: ", i)
		c.c.Produce(context.TODO(), msg,
			func(r *kgo.Record, err error) { retCh <- err })
	}
	// all messages are sent, we're waiting for the return values.
	errs := make([]error, 0, msgCount)

	for i := 0; i < msgCount; i++ {
		err := <-retCh
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	log.Println("kafka: send: done")

	// if we had any errors, we'll ask kafka to rollback and just leave the messages in the buffer
	// to be flushed to GCS.
	if len(errs) != 0 {
		log.Println("kafka: send: some messages failed. tx rollback.")
		rollback(context.Background(), c.c)
		c.lastAttempt = time.Now()
		if !c.failed.Load() {
			// we've been up, and we went down.
			c.failedSince = time.Now()
			c.failed.Store(true)
		}
		return errors.Join(errs...)
	}

	// all messages were sent successfully.
	// we can commit the transaction.
	err = c.c.EndTransaction(context.Background(), kgo.TryCommit)
	if err != nil {
		log.Println("committing tx: ", err)
		rollback(context.Background(), c.c)
		return fmt.Errorf("committing tx: %w", err)
	}
	c.failed.Store(false)
	c.failedSince = time.Time{}
	c.buffer.clear()
	return nil

}

func rollback(ctx context.Context, client *kgo.Client) {
	if err := client.AbortBufferedRecords(ctx); err != nil {
		fmt.Printf("error aborting buffered records: %v\n", err) // this only happens if ctx is canceled
		return
	}
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Printf("error rolling back transaction: %v\n", err)
		return
	}
	fmt.Println("transaction rolled back")
}

func (c *Client) flushToGCS() {
	log.Println("flushing to gcs")
}

func (c *Client) GetMsgCount() uint64 {
	return atomic.LoadUint64(c.msgCount)
}

func (c *Client) Status() (string, error) {
	healthy := c.failed.Load()
	if !healthy {
		return fmt.Sprintf("Kafka down since %v", time.Since(c.failedSince)), ErrKafkaDown
	}
	return "Hunky dory", nil
}
