package shims_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/linkedin/Burrow/shims"
)

// Equal expects the given values to be equal
func Equal(t *testing.T, expected, actual interface{}) {
	t.Helper()

	diff := cmp.Diff(expected, actual)
	if diff != "" {
		t.Errorf("Not Equal: %v", diff)
	}
}

// Error expects the given error to be present.
// Will fatally end the current rest run if not.
func Error(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		return
	}

	t.Fatal("Expected error did not occur!")
}

// Ok expects the given error to be nil.
// Will fatally end the current test run if not.
func Ok(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		return
	}

	t.Fatalf("unexpected error: %v", err)
}

func TestParseKafkaURLsGood(t *testing.T) {
	t.Parallel()

	goodURLs := "kafka+ssl://host-a:9096,kafka+ssl://host-b:9096,kafka+ssl://host-c:9096"
	expected := []string{
		"host-a:9096",
		"host-b:9096",
		"host-c:9096",
	}

	actual, err := shims.ParseKafkaURLs(goodURLs)
	Ok(t, err)
	Equal(t, expected, actual)
}

func TestParseKafkaURLsBad(t *testing.T) {
	t.Parallel()

	goodURLs := "not/a/valid%uri"

	_, err := shims.ParseKafkaURLs(goodURLs)
	Error(t, err)
}

func TestBuildConfig(t *testing.T) {
	t.Parallel()

	baseConfig := &shims.Config{
		KafkaURL:               "kafka+ssl://host-a:9096,kafka+ssl://host-b:9096,kafka+ssl://host-c:9096",
		KafkaTrustedCert:       "KafkaTrustedCert",
		KafkaTrustedCertPath:   "KafkaTrustedCertPath",
		KafkaClientCert:        "KafkaClientCert",
		KafkaClientCertPath:    "KafkaClientCertPath",
		KafkaClientCertKey:     "KafkaClientCertKey",
		KafkaClientCertKeyPath: "KafkaClientCertKeyPath",
		LogLevel:               "LogLevel",
		Port:                   1123,
		KafkaVersion:           "2.4.1",
		ConfigFilePath:         "/etc/shims/burrow.toml",
		BasicAuthUsername:      "username",
		BasicAuthPassword:      "password",
	}

	config, err := shims.BuildConfig(baseConfig)
	Ok(t, err)

	// logging
	Equal(t, "LogLevel", config.GetString("logging.level"))

	// TLS
	Equal(t, "KafkaClientCertPath", config.GetString("tls.heroku-kafka.certfile"))
	Equal(t, "KafkaTrustedCertPath", config.GetString("tls.heroku-kafka.cafile"))
	Equal(t, "KafkaClientCertKeyPath", config.GetString("tls.heroku-kafka.keyfile"))
	Equal(t, true, config.GetBool("tls.heroku-kafka.noverify"))

	// HTTP
	Equal(t, ":1123", config.GetString("httpserver.web.address"))
	Equal(t, 30, config.GetInt("httpserver.web.timeout"))
	Equal(t, "username", config.GetString("httpserver.web.basic-auth-username"))
	Equal(t, "password", config.GetString("httpserver.web.basic-auth-password"))

	// Client
	Equal(t, "2.4.1", config.GetString("client-profile.heroku-kafka.kafka-version"))
	Equal(t, "heroku-kafka", config.GetString("client-profile.heroku-kafka.tls"))

	// Consumer
	Equal(t, "heroku-kafka", config.GetString("consumer.heroku-kafka.cluster"))
	Equal(t, "heroku-kafka", config.GetString("consumer.heroku-kafka.client-profile"))
	Equal(t, "kafka", config.GetString("consumer.heroku-kafka.class-name"))
	Equal(t, []string{"host-a:9096", "host-b:9096", "host-c:9096"}, config.GetStringSlice("consumer.heroku-kafka.servers"))
	Equal(t, true, config.GetBool("consumer.heroku-kafka.start-latest"))
	Equal(t, true, config.GetBool("consumer.heroku-kafka.backfill-earliest"))

	// Cluster
	Equal(t, "heroku-kafka", config.GetString("cluster.heroku-kafka.client-profile"))
	Equal(t, "kafka", config.GetString("cluster.heroku-kafka.class-name"))
	Equal(t, []string{"host-a:9096", "host-b:9096", "host-c:9096"}, config.GetStringSlice("cluster.heroku-kafka.servers"))
	Equal(t, 20, config.GetInt("cluster.heroku-kafka.offset-refresh"))
}
