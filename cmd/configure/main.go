package main

import (
	"io/ioutil"
	"log"

	"github.com/joeshaw/envdecode"

	"github.com/linkedin/Burrow/shims"
)

func ok(context string, err error) {
	if err != nil {
		log.Fatalf("failed %v: %v", context, err)
	}
}

func writeCert(path, content string) error {
	return ioutil.WriteFile(path, []byte(content), 0644)
}

// Reads a helper.Config from the environment, and writes a valid burrow
// configuration file based on that, also writing kafka certificates to
// configured paths
func main() {
	var err error

	log.SetPrefix("[configure] ")
	log.Println("loading configuration from environment")

	config := &shims.Config{}
	envdecode.MustDecode(config)

	burrowConfig, err := shims.BuildConfig(config)
	ok("building burrow config", err)

	log.Println("configuration loaded")

	log.Printf("writing burrow config to: [%v]", config.ConfigFilePath)
	err = burrowConfig.WriteConfigAs(config.ConfigFilePath)
	ok("writing burrow config", err)

	log.Println("writing certificates")

	err = writeCert(config.KafkaClientCertPath, config.KafkaClientCert)
	ok("writing client cert", err)

	err = writeCert(config.KafkaClientCertKeyPath, config.KafkaClientCertKey)
	ok("writing client cert key", err)

	err = writeCert(config.KafkaTrustedCertPath, config.KafkaTrustedCert)
	ok("writing trusted cert", err)

	log.Println("all done")
}
