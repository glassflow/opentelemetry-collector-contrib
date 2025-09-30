// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package producer // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/producer"

import (
	"context"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
)

// Message represents a Kafka record.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

// Producer is an interface abstracting the Kafka client send.
type Producer interface {
	ExportData(ctx context.Context, records []Message) error
	Close() error
}

// NopProducer is a no-op implementation used until Kafka is wired.
type NopProducer struct{}

func (NopProducer) ExportData(context.Context, []Message) error { return nil }
func (NopProducer) Close() error                                { return nil }

// SaramaProducer adapts contrib internal/kafka SyncProducer to this interface.
type SaramaProducer struct{ sp sarama.SyncProducer }

// NewSaramaProducer creates a sync producer using contrib Kafka helpers.
func NewSaramaProducer(_ context.Context, client configkafka.ClientConfig, prodCfg configkafka.ProducerConfig, timeout time.Duration) (SaramaProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Timeout = timeout
	cfg.Producer.MaxMessageBytes = prodCfg.MaxMessageBytes
	cfg.Producer.RequiredAcks = sarama.RequiredAcks(prodCfg.RequiredAcks)
	switch prodCfg.Compression {
	case "gzip":
		cfg.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		cfg.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		cfg.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		cfg.Producer.Compression = sarama.CompressionZSTD
	default:
		cfg.Producer.Compression = sarama.CompressionNone
	}
	cfg.Metadata.AllowAutoTopicCreation = prodCfg.AllowAutoTopicCreation
	if client.ClientID != "" {
		cfg.ClientID = client.ClientID
	}
	sp, err := sarama.NewSyncProducer(client.Brokers, cfg)
	if err != nil {
		return SaramaProducer{}, err
	}
	return SaramaProducer{sp: sp}, nil
}

// EnsureTopics creates topics if they do not exist.
func EnsureTopics(ctx context.Context, brokers []string, topics map[string]TopicSpec) error {
	adminCfg := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, adminCfg)
	if err != nil {
		return err
	}
	defer admin.Close()
	for name, spec := range topics {
		if name == "" || !spec.Enabled {
			continue
		}
		_, err := admin.DescribeTopics([]string{name})
		if err == nil {
			continue
		}
		detail := &sarama.TopicDetail{
			NumPartitions:     int32(max(1, int(spec.NumPartitions))),
			ReplicationFactor: int16(max(1, int(spec.ReplicationFactor))),
		}
		if createErr := admin.CreateTopic(name, detail, false); createErr != nil {
			// ignore if topic already exists due to race
			if !isTopicExistsError(createErr) {
				return createErr
			}
		}
	}
	return nil
}

type TopicSpec struct {
	Enabled           bool
	NumPartitions     int32
	ReplicationFactor int16
}

func isTopicExistsError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "already exists")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ExportData converts records and sends them.
func (p SaramaProducer) ExportData(ctx context.Context, records []Message) error {
	if p.sp == nil {
		return nil
	}
	msgs := make([]*sarama.ProducerMessage, 0, len(records))
	for _, r := range records {
		m := &sarama.ProducerMessage{Topic: r.Topic}
		if r.Key != nil {
			m.Key = sarama.ByteEncoder(r.Key)
		}
		if r.Value != nil {
			m.Value = sarama.ByteEncoder(r.Value)
		}
		if len(r.Headers) > 0 {
			for k, v := range r.Headers {
				m.Headers = append(m.Headers, sarama.RecordHeader{Key: []byte(k), Value: v})
			}
		}
		msgs = append(msgs, m)
	}
	return p.sp.SendMessages(msgs)
}

func (p SaramaProducer) Close() error {
	if p.sp == nil {
		return nil
	}
	return p.sp.Close()
}
