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
	"github.com/spf13/cobra"
)

var alertsTopic string
var fetchURLs []string

// fetchCmd represents the consume command
var fetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "Fetch alerts",
	Run: func(cmd *cobra.Command, args []string) {
		// Generate config.
		conf := processor.Config{
			Brokers:     brokers,
			Topic:       topic,
			RetryTopic:  retryTopic,
			Group:       group,
			Delay:       delay,
			AlertsTopic: alertsTopic,
			FetchURLs:   fetchURLs,
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool)
		go func() {
			defer close(done)
			if err := processor.RunFetch(ctx, conf); err != nil {
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
	rootCmd.AddCommand(fetchCmd)

	// Here you will define your flags and configuration settings.
	fetchCmd.Flags().StringVarP(&group, "group", "g", "", "Group")
	fetchCmd.MarkFlagRequired("group")

	fetchCmd.Flags().StringVarP(&retryTopic, "retry-topic", "r", "", "Retry topic")

	fetchCmd.Flags().IntVarP(&delay, "delay", "d", 0, "Delay, in seconds")

	fetchCmd.Flags().StringVarP(&alertsTopic, "alerts-topic", "a", "", "Alerts topic")
	fetchCmd.MarkFlagRequired("alerts-topic")

	fetchCmd.Flags().StringArrayVarP(&fetchURLs, "fetch-urls", "f", []string{}, "Fetch URLs")
	fetchCmd.MarkFlagRequired("fetch-urls")
}
