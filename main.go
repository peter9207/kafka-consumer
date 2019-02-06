package main

import "github.com/spf13/cobra"
import "log"
import "os"
import "os/signal"
import "gopkg.in/Shopify/sarama.v1"
import "crypto/tls"
import "crypto/x509"
import "io/ioutil"

var rootCmd = &cobra.Command{
	Use:   "consume <kafka-broker>",
	Short: "consumer",
	Run: func(cmd *cobra.Command, args []string) {

		kafkaURL := args[0]
		topic := args[1]

		keypair, err := tls.LoadX509KeyPair("service.cert", "service.key")
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile("ca.pem")
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		log.Print("creating config")
		config := sarama.NewConfig()
		config.Net.TLS.Config = &tls.Config{
			Certificates: []tls.Certificate{keypair},
			RootCAs:      caCertPool,
		}
		config.Producer.Return.Successes = true
		config.Net.TLS.Enable = true
		config.Version = sarama.V0_10_2_0

		consumer, err := sarama.NewConsumer([]string{kafkaURL}, config)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := consumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()

		partitionConsumer, err := consumer.ConsumePartition(topic, 0, 0)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()

		// Trap SIGINT to trigger a shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)

		consumed := 0
	ConsumerLoop:
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				log.Printf("Consumed message offset %d\n", msg.Offset)
				consumed++
			case <-signals:
				break ConsumerLoop
			}
		}

		log.Printf("Consumed: %d\n", consumed)

		// Do Stuff Here
	},
}

func main() {

	rootCmd.Execute()
}
