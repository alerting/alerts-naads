package processor

import (
	"github.com/alerting/alerts/pkg/alerts"
)

// TODO: Config type for Alerts, Fetch w/ common core
type Config struct {
	Brokers    []string
	Topic      string
	RetryTopic string
	Group      string
	Delay      int

	AlertsService alerts.AlertsServiceClient
	FetchTopic    string
	AlertsTopic   string
	FetchURLs     []string

	System string
}
