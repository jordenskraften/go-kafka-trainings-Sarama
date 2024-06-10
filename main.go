package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

const (
	KafkaServerAddress = "localhost:9093"
	KafkaTopic         = "notifications"
)

func setupProducer() (sarama.SyncProducer, *sarama.Config, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, config, nil
}

type Consumer struct{}

func (consumer Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumer Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (consumer Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func setupConsumer(config *sarama.Config) error {
	config.Consumer.Offsets.Initial = sarama.OffsetNewest // Читаем с начала
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.MaxWaitTime = 500 * time.Millisecond

	consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, "group_id", config)
	if err != nil {
		return fmt.Errorf("error creating consumer group client: %w", err)
	}

	consumer := Consumer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{KafkaTopic}, consumer); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)
	<-sigterm
	return nil
}

func main() {
	fmt.Println("init producer")
	producer, config, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		msg := &sarama.ProducerMessage{
			Topic: KafkaTopic,
			Key:   sarama.StringEncoder(fmt.Sprintf("key %d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("value %d", i)),
		}
		part, offset, err := producer.SendMessage(msg)
		if err == nil {
			fmt.Printf("Sent message to partition %d at offset %d\n", part, offset)
		} else {
			fmt.Println(err)
		}
	}

	fmt.Println("init consumer")
	if err := setupConsumer(config); err != nil {
		log.Fatalf("failed to initialize consumer: %v", err)
	}
}
