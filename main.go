package main

import (
	"context"
	"fmt"
	"github.com/perbu/franz_kafka/kafka"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	err := realMain()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("exiting normally")
}

func realMain() error {
	// make a context that we can cancel with ctrl-c:
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	// make a client:

	client, err := kafka.New([]string{"localhost:9092"}, "test")
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := produce(ctx, client)
		if err != nil {
			log.Printf("produce: %s", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := consumer(ctx, client)
		if err != nil {
			log.Printf("consume: %s", err)
		}
	}()
	wg.Wait()
	return nil
}

// this generates a random sentence of 10 words:
func randomSentence() string {
	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"}
	sentence := ""
	for i := 0; i < 10; i++ {
		sentence += words[rand.Intn(len(words))] + " "
	}
	return sentence
}

func produce(ctx context.Context, client kafka.Client) error {
	// set up a timer tick to produce messages every 1s
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := client.ProduceMessage(ctx, "test", randomSentence())
			if err != nil {
				return fmt.Errorf("failed to produce message: %w", err)
			}
		}
	}
}
func consumer(ctx context.Context, client kafka.Client) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := client.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("failed to read message: %w", err)
			}
			log.Printf("read message, topic '%s':  %s", msg.Topic, string(msg.Value))
		}
	}
}
