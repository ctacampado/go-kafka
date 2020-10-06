package main

import (
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func createTopic(conn *kafka.Conn, topics ...string) error {

	topicConfigs := []kafka.TopicConfig{}

	topic := kafka.TopicConfig{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	for _, t := range topics {
		topic.Topic = t
		topicConfigs = append(topicConfigs, topic)
	}

	err := conn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}
	return nil
}

func listTopics(conn *kafka.Conn) error {

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return err
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}

	return nil
}

func deleteTopics(conn *kafka.Conn, topics ...string) error {
	for _, t := range topics {
		err := conn.DeleteTopics(t)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	addr := "localhost:9092"
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	topics := []string{"topic-a", "topic-b", "topic-c"}

	log.Println("creating topics...")
	err = createTopic(conn, topics...)
	if err != nil {
		panic(err.Error())
	}

	err = listTopics(conn)
	if err != nil {
		panic(err.Error())
	}

	log.Println("deleting topics...")
	err = deleteTopics(conn, topics...)
	if err != nil {
		panic(err.Error())
	}
}
