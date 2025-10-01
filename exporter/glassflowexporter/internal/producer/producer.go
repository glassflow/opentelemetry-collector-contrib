// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package producer // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/producer"

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
	"github.com/xdg-go/scram"
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
type SaramaProducer struct{ sp sarama.SyncProducer }

// NewSaramaProducer creates a sync producer using contrib Kafka helpers.
func NewSaramaProducer(ctx context.Context, client configkafka.ClientConfig, prodCfg configkafka.ProducerConfig, timeout time.Duration) (SaramaProducer, error) {
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
	return SaramaProducer{sp: sp}, nil
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
