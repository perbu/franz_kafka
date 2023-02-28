package kafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
)

type Client struct {
	cl     *kgo.Client
	buffer []*kgo.Record
}

func New(brokers []string, topic string) (Client, error) {
	log.Printf("Creating client[broker=%v], will consume topic '%s'", brokers, topic)
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return Client{}, fmt.Errorf("new client: %w", err)
	}
	c := Client{
		cl: cl,
	}
	log.Println("Pinging client")
	err = cl.Ping(context.TODO())
	if err != nil {
		return Client{}, fmt.Errorf("ping: %w", err)
	}
	log.Println("Client OK")
	return c, nil
}

func (c Client) Close() {
	log.Println("Allowing rebalance")
	c.cl.AllowRebalance()
	log.Println("Closing client")
	c.cl.Close()

}

// ReadMessage will read return exactly one message from the topic.
func (c Client) ReadMessage(ctx context.Context) (*kgo.Record, error) {
	fetches := c.cl.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retriable errors are
		// returned from polls so that users can notice and take action.
		return nil, fmt.Errorf("fetch records: %w", errs[0])
	}
	log.Printf("Fetched records, num records: %d", fetches.NumRecords())
	if fetches.NumRecords() == 0 {
		return nil, fmt.Errorf("no records")
	}
	if fetches.NumRecords() > 1 {
		return nil, fmt.Errorf("too many records")
	}
	iter := fetches.RecordIter()
	record := iter.Next()

	return record, nil

}

// ProduceMessage takes a string and produces it to the topic
// it is sync.
func (c Client) ProduceMessage(ctx context.Context, topic, msg string) error {
	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 1)
	c.cl.Produce(ctx, &kgo.Record{
		Topic: topic,
		Value: []byte(msg),
	},
		func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				errCh <- err
			}
		})
	wg.Wait()
	if len(errCh) > 0 {
		log.Println("Error channel has data")
		return <-errCh
	}
	log.Println("Message produced")
	return nil
}
