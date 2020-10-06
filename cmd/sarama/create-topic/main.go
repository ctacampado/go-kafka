package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}
	defer func() { _ = admin.Close() }()

	topics := []string{"sarama-a", "sarama-b", "sarama-c"}

	log.Println("creating topics...")
	for _, t := range topics {
		err = admin.CreateTopic(t, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			log.Fatal("Error while creating topic: ", err.Error())
		}
	}

	list, err := admin.DescribeTopics(topics)
	if err != nil {
		log.Fatal("Error while listing topics: ", err.Error())
	}

	for _, t := range list {
		log.Println(t.Name)
	}

	for _, t := range topics {
		admin.DeleteTopic(t)
	}
}
