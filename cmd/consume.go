// Copyright © 2018 Zachary Seguin <zachary@zacharyseguin.ca>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alerting/alerts-naads/pkg/processor"
	"github.com/alerting/alerts/pkg/alerts"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var retryTopic string
var group string
var delay int
var alertsAddress string
var fetchTopic string
var system string

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume alerts",
	Run: func(cmd *cobra.Command, args []string) {
		// Connect to alerts service
		grpcConn, err := grpc.Dial(
			alertsAddress,
			grpc.WithMaxMsgSize(1024*1024*1024),
			grpc.WithInsecure(),
		)
		if err != nil {
			log.Fatal(err)
		}

		alertsService := alerts.NewAlertsServiceClient(grpcConn)

		// Generate config.
		conf := processor.Config{
			Brokers:       brokers,
			Topic:         topic,
			RetryTopic:    retryTopic,
			Group:         group,
			Delay:         delay,
			AlertsService: alertsService,
			FetchTopic:    fetchTopic,
			System:        system,
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool)
		go func() {
			defer close(done)
			if err := processor.RunAlert(ctx, conf); err != nil {
				if err != context.Canceled {
					log.Fatal(err)
				}
			}
		}()

		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
		<-wait // Wait for SIGINT or SIGTERM
		log.Println("Signal received, terminating...")
		cancel() // Stop the processor
		<-done
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	// Here you will define your flags and configuration settings.
	consumeCmd.Flags().StringVarP(&group, "group", "g", "", "Group")
	consumeCmd.MarkFlagRequired("group")

	consumeCmd.Flags().StringVarP(&retryTopic, "retry-topic", "r", "", "Retry topic")

	consumeCmd.Flags().IntVarP(&delay, "delay", "d", 0, "Delay, in seconds")

	consumeCmd.Flags().StringVarP(&alertsAddress, "alerts-address", "a", "", "Address of alerts service")
	consumeCmd.MarkFlagRequired("alerts-address")

	consumeCmd.Flags().StringVarP(&fetchTopic, "fetch-topic", "f", "", "Fetch topic")

	consumeCmd.Flags().StringVarP(&system, "system", "s", "naads", "System name")
}
