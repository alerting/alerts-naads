package listen

import (
	"context"
	"encoding/xml"
	"log"
	"net"
	"time"

	"github.com/alerting/alerts-naads/pkg/codec"
	capxml "github.com/alerting/alerts/pkg/cap/xml"
	"github.com/lovoo/goka"
)

type Config struct {
	Brokers []string
	Topic   string
	Address string
	Timeout int
}

func Run(ctx context.Context, conf Config) error {
	emitter, err := goka.NewEmitter(conf.Brokers, goka.Stream(conf.Topic), new(codec.Alert))
	if err != nil {
		log.Fatal(err)
	}
	defer emitter.Finish()

	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
	}
	conn, err := dialer.DialContext(ctx, "tcp", conf.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("Connected to %s", conf.Address)

	decoder := xml.NewDecoder(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(time.Duration(conf.Timeout) * time.Second))
		var alert capxml.Alert

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err = decoder.Decode(&alert); err != nil {
				return err
			}

			log.Printf("Received: %s, %v,%v,%v", alert.ID(), alert.Sender, alert.Identifier, alert.Sent)
			err = emitter.EmitSync(alert.ID(), &alert)
			if err != nil {
				return err
			}
		}
	}
}
