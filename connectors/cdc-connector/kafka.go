package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	w *kafka.Writer
}

func NewProducer(brokers []string) *KafkaProducer {
	return &KafkaProducer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			Completion: func(messages []kafka.Message, err error) {
				if err != nil {
					log.Println("kafka batch failed:", err)
				}
			},
		},
	}
}

func primaryKey(meta *TableMeta, e ChangeEvent) []byte {
	row := e.After
	if row == nil {
		row = e.Before
	}

	// fallback jika table TANPA PK
	if len(meta.PK) == 0 {
		return fmt.Appendf(nil, "%s.%s:%s", meta.Schema, meta.Table, e.LSN)
	}

	parts := []string{
		meta.Schema + "." + meta.Table,
	}
	for _, col := range meta.PK {
		parts = append(parts, fmt.Sprint(row[col]))
	}

	return []byte(strings.Join(parts, ":"))
}

var ctx = context.Background()

func (p *KafkaProducer) Produce(event ChangeEvent, meta *TableMeta) error {
	topic := fmt.Sprintf(
		"db.%s.%s.changelog",
		meta.Schema,
		meta.Table,
	)
	key := primaryKey(meta, event)
	err := EnsureTopic(
		"localhost:9092",
		topic,
		3,
		1,
		[]kafka.ConfigEntry{
			kafka.ConfigEntry{
				ConfigName:  "cleanup.policy",
				ConfigValue: "compact",
			},
		},
	)
	if err != nil {
		log.Fatal("failed to ensure topic:", err)
	}
	// DELETE = tombstone
	var value []byte
	if event.Op != "d" {
		var err error
		value, err = json.Marshal(event)
		if err != nil {
			return err
		}
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	err = p.w.WriteMessages(ctx, msg)
	if err != nil {
		log.Println("test Error", err, ctx.Err())
	}
	return err
}

func (p *KafkaProducer) Close() error {
	return p.w.Close()
}

func EnsureTopic(
	broker string,
	topic string,
	partitions int,
	replication int,
	configs []kafka.ConfigEntry,
) error {

	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", controller.Host)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replication,
		ConfigEntries:     configs,
	})

	// topic sudah ada → OK
	if err != nil && errors.Is(err, kafka.TopicAlreadyExists) {
		return nil
	}

	return err
}
