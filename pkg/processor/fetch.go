package processor

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alerting/alerts-naads/pkg/codec"
	"github.com/alerting/alerts/pkg/cap/xml"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/kafka"
)

func clean(str string) string {
	str = strings.Replace(str, "-", "_", -1)
	str = strings.Replace(str, "+", "p", -1)
	str = strings.Replace(str, ":", "_", -1)

	return str
}

func fetch(ctx context.Context, conf *Config, ref *capxml.Reference) (*capxml.Alert, error) {
	// Generate the URL
	u, err := url.Parse(fmt.Sprintf("%s/%sI%s.xml",
		ref.Sent.Format("2006-01-02"),
		clean(ref.Sent.FormatCAP()),
		clean(ref.Identifier)))

	if err != nil {
		return nil, err
	}

	for _, fetchURL := range conf.FetchURLs {
		baseURL, err := url.Parse(fetchURL)
		if err != nil {
			log.Fatal(err)
			// TODO: Handle
		}
		u = baseURL.ResolveReference(u)

		log.Println("Fetching", u.String())
		res, err := http.Get(u.String())
		if err != nil {
			log.Println(err)
			continue
		}
		defer res.Body.Close()

		log.Printf("Got status code %d", res.StatusCode)
		if res.StatusCode != 200 {
			continue
		}

		d := xml.NewDecoder(res.Body)

		var alert capxml.Alert
		if err = d.Decode(&alert); err != nil {
			log.Println(err)
			continue
		}

		return &alert, nil
	}

	return nil, errors.New("Unable to fetch alert")
}

func collectFetch(ctx context.Context, conf *Config) func(ctx goka.Context, msg interface{}) {
	return func(gctx goka.Context, msg interface{}) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(conf.Delay) * time.Second):
		}

		ref := msg.(capxml.Reference)

		log.Printf("Received: %v, %v => %v", gctx.Topic(), gctx.Key(), ref)

		alert, err := fetch(ctx, conf, &ref)
		if err != nil {
			log.Fatal(err)
			// TODO: Handle
		}

		gctx.Emit(goka.Stream(conf.AlertsTopic), alert.ID(), alert)
	}
}

func RunFetch(ctx context.Context, conf Config) error {
	edges := []goka.Edge{
		goka.Input(goka.Stream(conf.Topic), new(codec.Reference), collectFetch(ctx, &conf)),
		goka.Output(goka.Stream(conf.RetryTopic), new(codec.Reference)),
		goka.Output(goka.Stream(conf.AlertsTopic), new(codec.Alert)),
	}

	kconf := kafka.NewConfig()
	// 5 MB
	kconf.Producer.MaxMessageBytes = 1024 * 1024 * 5

	g := goka.DefineGroup(goka.Group(conf.Group), edges...)
	p, err := goka.NewProcessor(conf.Brokers, g,
		goka.WithConsumerBuilder(kafka.ConsumerBuilderWithConfig(kconf)),
		goka.WithProducerBuilder(kafka.ProducerBuilderWithConfig(kconf)))
	if err != nil {
		return err
	}

	return p.Run(ctx)
}
