package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	addr := "localhost:9092"
	topic := "test-topic"
	var partition, min, max int = 0, 4e3, 10e6

	//deadline := time.Now().Add(3 * time.Second)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{addr},
		Topic:     topic,
		Partition: partition,
		MinBytes:  min, // 10KB
		MaxBytes:  max, // 10MB
	})
	//r.SetOffset(kafka.FirstOffset)
	r.SetOffset(kafka.LastOffset)
	//r.SetOffset(3)

	defer func() {
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}
