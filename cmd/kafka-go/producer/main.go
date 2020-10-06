package main

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test-topic",
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 1 * time.Millisecond,
		BatchSize:    1000,
		//Async:        true,
	})

	var msgs []kafka.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, kafka.Message{
			Key:   []byte("key"),
			Value: []byte("iter:" + strconv.Itoa(i)),
		})
	}
	err := writer.WriteMessages(context.Background(), msgs...)
	if err != nil {
		log.Fatal(err)
	}
}
