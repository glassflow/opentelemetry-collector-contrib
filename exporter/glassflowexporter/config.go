// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package glassflowexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
)

var _ component.Config = (*Config)(nil)

// Config defines configuration for glassflow exporter.
type Config struct {
	TimeoutSettings           exporterhelper.TimeoutConfig    `mapstructure:",squash"`
	QueueBatchConfig          exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	configkafka.ClientConfig  `mapstructure:",squash"`
	Producer                  configkafka.ProducerConfig `mapstructure:"producer"`

	// DryRun, when true, disables real Kafka sends and uses a no-op producer.
	// This is enabled by default for safer local testing and unit tests.
	DryRun bool `mapstructure:"dry_run"`

	// Per-signal configuration
	Traces  SignalConfig  `mapstructure:"traces"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logs    SignalConfig  `mapstructure:"logs"`
}

// SignalConfig holds per-signal basic settings.
type SignalConfig struct {
	Enabled  bool      `mapstructure:"enabled"`
	Encoding string    `mapstructure:"encoding"`
	Topic    TopicSpec `mapstructure:"topic"`
}

// TopicSpec declares topic name and creation parameters.
type TopicSpec struct {
	Name              string `mapstructure:"name"`
	Create            bool   `mapstructure:"create"`
	NumPartitions     int32  `mapstructure:"num_partitions"`
	ReplicationFactor int16  `mapstructure:"replication_factor"`
}

// MetricsConfig contains per-type configs (each is a SignalConfig)
type MetricsConfig struct {
	Gauge                SignalConfig `mapstructure:"gauge"`
	Sum                  SignalConfig `mapstructure:"sum"`
	Histogram            SignalConfig `mapstructure:"histogram"`
	ExponentialHistogram SignalConfig `mapstructure:"exponential_histogram"`
	Summary              SignalConfig `mapstructure:"summary"`
}

func (c *Config) Validate() error { return nil }
