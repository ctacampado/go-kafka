package main

import (
	"log"
	"strconv"

	"github.com/Shopify/sarama"
)

func main() {
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	var pmsgs []*sarama.ProducerMessage
	for i := 0; i < 10; i++ {
		pmsg := &sarama.ProducerMessage{
			Topic: "test-topic",
			Value: sarama.ByteEncoder([]byte("iter:" + strconv.Itoa(i))),
		}
		pmsgs = append(pmsgs, pmsg)
	}

	err = producer.SendMessages(pmsgs)
	if err != nil {
		log.Fatalln(err)
	}

	//msg := &sarama.ProducerMessage{Topic: "test-topic", Value: sarama.StringEncoder("testing 123")}
	//partition, offset, err := producer.SendMessage(msg)
	//if err != nil {
	//	log.Printf("FAILED to send message: %s\n", err)
	//} else {
	//	log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	//}

}
