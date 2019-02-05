package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/alerting/alerts-naads/pkg/codec"
	"github.com/alerting/alerts/pkg/cap"
	capxml "github.com/alerting/alerts/pkg/cap/xml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/kafka"
)

func collectAlert(ctx context.Context, conf *Config) func(ctx goka.Context, msg interface{}) {
	return func(gctx goka.Context, msg interface{}) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(conf.Delay) * time.Second):
		}

		xmlAlert := msg.(capxml.Alert)

		log.Printf("Received: %v, %v => %v,%v,%v", gctx.Topic(), gctx.Key(), xmlAlert.Sender, xmlAlert.Identifier, xmlAlert.Sent)

		// Check if references exist
		if conf.FetchTopic != "" {
			for _, xmlReference := range xmlAlert.References {
				log.Printf("Checking reference: %v", xmlReference)

				sent, _ := ptypes.TimestampProto(xmlReference.Sent.Time)
				ref := &cap.Reference{
					Sender:     xmlReference.Sender,
					Identifier: xmlReference.Identifier,
					Sent:       sent,
				}

				has, err := conf.AlertsService.Has(ctx, ref)
				if err != nil {
					log.Fatal(err)
					// TODO: Handle
				}

				// If we don't have it, and it's not in the fetch table, then let's request it to be fetched.
				if !has.Result && gctx.Lookup(goka.Table(conf.FetchTopic), ref.ID()) == nil {
					log.Printf("Requesting %v", ref)
					gctx.Emit(goka.Stream(conf.FetchTopic), ref.ID(), xmlReference)
				}
			}
		}

		// Convert to CAP
		var alert cap.Alert

		b, err := json.Marshal(xmlAlert)
		if err != nil {
			log.Fatal(err)
			// TODO: Handle
		}

		jd := jsonpb.Unmarshaler{
			AllowUnknownFields: true,
		}
		err = jd.Unmarshal(bytes.NewReader(b), &alert)
		if err != nil {
			log.Fatal(err)
			// TODO: Handle
		}

		// Save the alert, if it's good
		if (alert.Status == cap.Alert_ACTUAL || alert.Status == cap.Alert_EXCERCISE || alert.Status == cap.Alert_TEST) && (alert.MessageType == cap.Alert_ALERT || alert.MessageType == cap.Alert_UPDATE || alert.MessageType == cap.Alert_CANCEL) {
			if _, err := conf.AlertsService.Add(ctx, &alert); err != nil {
				log.Fatal(err)
				// TODO: Handle error
			}
		}

		if conf.RetryTopic != "" {
			gctx.Emit(goka.Stream(conf.RetryTopic), gctx.Key(), alert)
		}
	}
}

func RunAlert(ctx context.Context, conf Config) error {
	edges := []goka.Edge{
		goka.Input(goka.Stream(conf.Topic), new(codec.Alert), collectAlert(ctx, &conf)),
		goka.Output(goka.Stream(conf.RetryTopic), new(codec.Alert)),
		goka.Output(goka.Stream(conf.FetchTopic), new(codec.Reference)),
	}
	if conf.FetchTopic != "" {
		edges = append(edges, goka.Lookup(goka.Table(conf.FetchTopic), new(codec.Reference)))
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
