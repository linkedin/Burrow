package shims

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	KafkaClientCertKey     string `env:"KAFKA_CLIENT_CERT_KEY,required"`
	KafkaClientCertKeyPath string `env:"KAFKA_CLIENT_CERT_KEY_PATH,required"`

	KafkaTrustedCert     string `env:"KAFKA_TRUSTED_CERT,required"`
	KafkaTrustedCertPath string `env:"KAFKA_TRUSTED_CERT_PATH,required"`

	KafkaClientCert     string `env:"KAFKA_CLIENT_CERT,required"`
	KafkaClientCertPath string `env:"KAFKA_CLIENT_CERT_PATH,required"`

	ConfigFilePath string `env:"CONFIG_FILE_PATH,required"`
	LogLevel       string `env:"LOG_LEVEL,default=info"`
	Port           int    `env:"PORT,required"`

	KafkaURL     string `env:"KAFKA_URL,required"`
	KafkaVersion string `env:"KAFKA_VERSION,required"`

	BasicAuthUsername string `env:"BASIC_AUTH_USERNAME,required"`
	BasicAuthPassword string `env:"BASIC_AUTH_PASSWORD,required"`
}

// ParseKafkaURLs returns a slice of "host:port" from a
// "scheme://host:port,scheme://host:port" string
func ParseKafkaURLs(kafkaURL string) ([]string, error) {
	URLs := strings.Split(kafkaURL, ",")
	for i, URL := range URLs {
		parsedURL, err := url.Parse(URL)

		if err != nil {
			return nil, err
		}

		URLs[i] = parsedURL.Host
	}

	return URLs, nil
}

// BuildConfig converts a Config struct into a TOML viper config that can
// be written out and consumed by Burrow
func BuildConfig(config *Config) (*viper.Viper, error) {
	kafkaURLs, err := ParseKafkaURLs(config.KafkaURL)
	if err != nil {
		return nil, err
	}

	viperConfig := viper.New()
	viperConfig.SetConfigType("toml")

	// Logging
	viperConfig.Set("logging.level", config.LogLevel)

	// TLS
	viperConfig.Set("tls.heroku-kafka.certfile", config.KafkaClientCertPath)
	viperConfig.Set("tls.heroku-kafka.keyfile", config.KafkaClientCertKeyPath)
	viperConfig.Set("tls.heroku-kafka.cafile", config.KafkaTrustedCertPath)
	viperConfig.Set("tls.heroku-kafka.noverify", true)

	// HTTP
	viperConfig.Set("httpserver.web.address", fmt.Sprintf(":%v", config.Port))
	viperConfig.Set("httpserver.web.timeout", 30)

	// these are new config paths
	viperConfig.Set("httpserver.web.basic-auth-username", config.BasicAuthUsername)
	viperConfig.Set("httpserver.web.basic-auth-password", config.BasicAuthPassword)

	// Client
	viperConfig.Set("client-profile.heroku-kafka.kafka-version", config.KafkaVersion)
	viperConfig.Set("client-profile.heroku-kafka.tls", "heroku-kafka")

	// Consumer
	viperConfig.Set("consumer.heroku-kafka.class-name", "kafka")
	viperConfig.Set("consumer.heroku-kafka.cluster", "heroku-kafka")
	viperConfig.Set("consumer.heroku-kafka.client-profile", "heroku-kafka")
	viperConfig.Set("consumer.heroku-kafka.servers", kafkaURLs)
	viperConfig.Set("consumer.heroku-kafka.start-latest", true)
	viperConfig.Set("consumer.heroku-kafka.backfill-earliest", true)

	// Cluster
	viperConfig.Set("cluster.heroku-kafka.class-name", "kafka")
	viperConfig.Set("cluster.heroku-kafka.client-profile", "heroku-kafka")
	viperConfig.Set("cluster.heroku-kafka.servers", kafkaURLs)
	viperConfig.Set("cluster.heroku-kafka.offset-refresh", 20)

	return viperConfig, err
}
