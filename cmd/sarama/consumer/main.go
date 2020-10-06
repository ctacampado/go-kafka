package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	master, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		log.Println("closing master...")
		if err := master.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// When
	consumer, err := master.ConsumePartition("test-topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		log.Println("closing consumer...")
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Then
ConsumerLoop:
	for {
		select {
		case msg := <-consumer.Messages():
			log.Println(string(msg.Value))
		case <-signals:
			break ConsumerLoop
		}
	}
}
