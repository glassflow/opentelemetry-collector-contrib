// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package producer // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/producer"

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
	"github.com/xdg-go/scram"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

const (
	SCRAMSHA512          = "SCRAM-SHA-512"
	SCRAMSHA256          = "SCRAM-SHA-256"
	PLAIN                = "PLAIN"
	AWSMSKIAMOAUTHBEARER = "AWS_MSK_IAM_OAUTHBEARER"
)

// configureSaramaAuthentication configures authentication in sarama.Config.
func configureSaramaAuthentication(
	ctx context.Context,
	config configkafka.AuthenticationConfig,
	saramaConfig *sarama.Config,
) {
	if config.PlainText != nil {
		configurePlaintext(*config.PlainText, saramaConfig)
	}
	if config.SASL != nil {
		configureSASL(ctx, *config.SASL, saramaConfig)
	}
	if config.Kerberos != nil {
		configureKerberos(*config.Kerberos, saramaConfig)
	}
}

func configurePlaintext(config configkafka.PlainTextConfig, saramaConfig *sarama.Config) {
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = config.Username
	saramaConfig.Net.SASL.Password = config.Password
}

func configureSASL(ctx context.Context, config configkafka.SASLConfig, saramaConfig *sarama.Config) {
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = config.Username
	saramaConfig.Net.SASL.Password = config.Password
	saramaConfig.Net.SASL.Version = int16(config.Version)

	// Disable ApiVersionsRequest when using SASL v0 (required by some Kafka versions)
	if config.Version == 0 {
		saramaConfig.ApiVersionsRequest = false
	}

	switch config.Mechanism {
	case SCRAMSHA512:
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha512.New} }
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	case SCRAMSHA256:
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha256.New} }
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case PLAIN:
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case AWSMSKIAMOAUTHBEARER:
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		saramaConfig.Net.SASL.TokenProvider = &awsMSKTokenProvider{ctx: ctx, region: config.AWSMSK.Region}
	}
}

func configureKerberos(config configkafka.KerberosConfig, saramaConfig *sarama.Config) {
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
	saramaConfig.Net.SASL.Enable = true
	if config.UseKeyTab {
		saramaConfig.Net.SASL.GSSAPI.KeyTabPath = config.KeyTabPath
		saramaConfig.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
	} else {
		saramaConfig.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
		saramaConfig.Net.SASL.GSSAPI.Password = config.Password
	}
	saramaConfig.Net.SASL.GSSAPI.KerberosConfigPath = config.ConfigPath
	saramaConfig.Net.SASL.GSSAPI.Username = config.Username
	saramaConfig.Net.SASL.GSSAPI.Realm = config.Realm
	saramaConfig.Net.SASL.GSSAPI.ServiceName = config.ServiceName
	saramaConfig.Net.SASL.GSSAPI.DisablePAFXFAST = config.DisablePAFXFAST
}

type awsMSKTokenProvider struct {
	ctx    context.Context
	region string
}

// Token return the AWS session token for the AWS_MSK_IAM_OAUTHBEARER mechanism
func (c *awsMSKTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthToken(c.ctx, c.region)
	return &sarama.AccessToken{Token: token}, err
}

// XDGSCRAMClient implements sarama.SCRAMClient
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

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
type SaramaProducer struct {
	sp      sarama.SyncProducer
	logger  *zap.Logger
	debug   bool
	metrics Metrics
}

// Metrics interface for recording producer metrics
type Metrics interface {
	RecordExportMetrics(ctx context.Context, records int64, bytes int64, latency float64, outcome string, topic string, partition int32)
}

// NewSaramaProducer creates a sync producer using contrib Kafka helpers.
func NewSaramaProducer(ctx context.Context, client configkafka.ClientConfig, prodCfg configkafka.ProducerConfig, timeout time.Duration, logger *zap.Logger, debug bool, metrics Metrics) (SaramaProducer, error) {
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

	// Configure authentication
	configureSaramaAuthentication(ctx, client.Authentication, cfg)

	sp, err := sarama.NewSyncProducer(client.Brokers, cfg)
	if err != nil {
		return SaramaProducer{}, err
	}
	return SaramaProducer{sp: sp, logger: logger, debug: debug, metrics: metrics}, nil
}

// EnsureTopics creates topics if they do not exist.
func EnsureTopics(ctx context.Context, client configkafka.ClientConfig, topics map[string]TopicSpec) error {
	adminCfg := sarama.NewConfig()

	// Configure authentication for admin client
	configureSaramaAuthentication(ctx, client.Authentication, adminCfg)

	admin, err := sarama.NewClusterAdmin(client.Brokers, adminCfg)
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

// categorizeKafkaError categorizes Kafka errors as permanent, retryable, or throttled
func categorizeKafkaError(err error) error {
	if err == nil {
		return nil
	}

	// Check for Sarama producer errors
	var prodErr sarama.ProducerErrors
	if errors.As(err, &prodErr) && len(prodErr) > 0 {
		// Check if all errors are configuration errors (permanent)
		var areConfigErrs bool
		var confErr sarama.ConfigurationError
		for _, producerErr := range prodErr {
			if areConfigErrs = errors.As(producerErr.Err, &confErr); !areConfigErrs {
				break
			}
		}
		if areConfigErrs {
			return consumererror.NewPermanent(confErr)
		}

		// Check for specific error types
		firstErr := prodErr[0].Err
		switch {
		case isPermanentError(firstErr):
			return consumererror.NewPermanent(firstErr)
		case isThrottleError(firstErr):
			// Extract throttle duration if available
			duration := getThrottleDuration(firstErr)
			if duration > 0 {
				return exporterhelper.NewThrottleRetry(firstErr, duration)
			}
			return firstErr // Retryable
		default:
			return firstErr // Retryable
		}
	}

	// Check for other error types
	switch {
	case isPermanentError(err):
		return consumererror.NewPermanent(err)
	case isThrottleError(err):
		duration := getThrottleDuration(err)
		if duration > 0 {
			return exporterhelper.NewThrottleRetry(err, duration)
		}
		return err // Retryable
	default:
		return err // Retryable
	}
}

// isPermanentError checks if an error is permanent (should not be retried)
func isPermanentError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Configuration errors are permanent
	if strings.Contains(errStr, "configuration") ||
		strings.Contains(errStr, "invalid") ||
		strings.Contains(errStr, "authentication") ||
		strings.Contains(errStr, "authorization") {
		return true
	}

	// Check for specific Sarama error types
	switch err.(type) {
	case sarama.ConfigurationError:
		return true
	default:
		// Check for specific error codes
		if strings.Contains(errStr, "OutOfBrokers") {
			return false // Retryable
		}
		if strings.Contains(errStr, "InsufficientData") ||
			strings.Contains(errStr, "InvalidMessage") ||
			strings.Contains(errStr, "InvalidMessageSize") ||
			strings.Contains(errStr, "InvalidPartition") ||
			strings.Contains(errStr, "InvalidTopic") {
			return true // Permanent
		}
		return false // Assume retryable
	}
}

// isThrottleError checks if an error indicates throttling
func isThrottleError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "throttle") ||
		strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "too many requests") ||
		strings.Contains(errStr, "429")
}

// getThrottleDuration extracts throttle duration from error if available
func getThrottleDuration(err error) time.Duration {
	// For now, return a default throttle duration
	// In a real implementation, you might parse this from the error message
	return 1 * time.Second
}

// ExportData converts records and sends them.
func (p SaramaProducer) ExportData(ctx context.Context, records []Message) error {
	if p.sp == nil {
		return nil
	}

	startTime := time.Now()

	msgs := make([]*sarama.ProducerMessage, 0, len(records))
	for i, r := range records {
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

		if p.debug && i < 3 { // Log first 3 messages for debugging
			p.logger.Debug("Message details",
				zap.Int("index", i),
				zap.String("topic", r.Topic),
				zap.Int("key_len", len(r.Key)),
				zap.Int("value_len", len(r.Value)),
				zap.Int("headers_count", len(r.Headers)))
		}
	}

	if p.debug {
		// Calculate total message size for debugging
		totalSize := 0
		for _, msg := range msgs {
			if msg.Value != nil {
				// Convert sarama.ByteEncoder to []byte to get length
				if byteEncoder, ok := msg.Value.(sarama.ByteEncoder); ok {
					totalSize += len([]byte(byteEncoder))
				} else {
					// For other encoder types, estimate size
					totalSize += 100 // rough estimate
				}
			}
		}
		p.logger.Debug("Sending messages to Kafka",
			zap.Int("count", len(records)),
			zap.Int("kafka_messages", len(msgs)),
			zap.Int("total_size_bytes", totalSize),
			zap.String("topics", p.getUniqueTopics(records)))
	}

	err := p.sp.SendMessages(msgs)
	latency := time.Since(startTime).Seconds()

	// Record metrics
	if p.metrics != nil {
		p.recordMetrics(ctx, records, msgs, err, latency)
	}

	if err != nil {
		// Provide detailed error information
		if p.debug {
			p.logger.Error("Failed to send messages to Kafka",
				zap.Error(err),
				zap.Int("message_count", len(msgs)),
				zap.String("topics", p.getUniqueTopics(records)))
		}

		// Categorize and return appropriate error type
		return categorizeKafkaError(err)
	}

	if p.debug {
		p.logger.Debug("Successfully sent messages to Kafka",
			zap.Int("count", len(msgs)))
	}

	return nil
}

// recordMetrics records metrics for the export operation
func (p SaramaProducer) recordMetrics(ctx context.Context, records []Message, msgs []*sarama.ProducerMessage, err error, latency float64) {
	if p.metrics == nil {
		return
	}

	// Calculate total bytes
	totalBytes := int64(0)
	for _, msg := range msgs {
		totalBytes += int64(msg.ByteSize(2)) // 2 is the overhead for key/value length
	}

	// Group by topic and partition for metrics
	topicPartitionStats := make(map[string]map[int32]int64)
	for _, msg := range msgs {
		topic := msg.Topic
		partition := msg.Partition

		if topicPartitionStats[topic] == nil {
			topicPartitionStats[topic] = make(map[int32]int64)
		}
		topicPartitionStats[topic][partition]++
	}

	// Record metrics for each topic/partition
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}

	for topic, partitions := range topicPartitionStats {
		for partition, count := range partitions {
			// Calculate bytes for this partition (simplified - assumes equal distribution)
			partitionBytes := totalBytes / int64(len(msgs)) * count

			p.metrics.RecordExportMetrics(
				ctx,
				count,
				partitionBytes,
				latency,
				outcome,
				topic,
				partition,
			)
		}
	}
}

// getUniqueTopics returns a comma-separated list of unique topics
func (p SaramaProducer) getUniqueTopics(records []Message) string {
	topicSet := make(map[string]bool)
	for _, r := range records {
		topicSet[r.Topic] = true
	}
	var topics []string
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	return strings.Join(topics, ", ")
}

// formatProducerErrors formats Sarama producer errors into a detailed error message
func (p SaramaProducer) formatProducerErrors(errs sarama.ProducerErrors) error {
	if len(errs) == 0 {
		return nil
	}

	var errorDetails []string
	topicErrors := make(map[string]int)

	for i, err := range errs {
		if i >= 10 { // Limit to first 10 errors to avoid huge error messages
			errorDetails = append(errorDetails, fmt.Sprintf("... and %d more errors", len(errs)-i))
			break
		}

		errorDetails = append(errorDetails, fmt.Sprintf("msg[%d]: topic=%s, partition=%d, offset=%d, error=%s",
			i, err.Msg.Topic, err.Msg.Partition, err.Msg.Offset, err.Err.Error()))

		topicErrors[err.Msg.Topic]++
	}

	// Create summary
	var summary []string
	for topic, count := range topicErrors {
		summary = append(summary, fmt.Sprintf("%s(%d)", topic, count))
	}

	return fmt.Errorf("kafka: Failed to deliver %d messages to topics [%s]. Details: %s",
		len(errs), strings.Join(summary, ", "), strings.Join(errorDetails, "; "))
}

func (p SaramaProducer) Close() error {
	if p.sp == nil {
		return nil
	}
	return p.sp.Close()
}
