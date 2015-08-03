package main

import (
	"fmt"
	"github.com/marpaia/graphite-golang"
)

var Graphite *graphite.Graphite

func loadGraphite() {
	host := config.Graphite.Host
	port := config.Graphite.Port
	Graphite, err := graphite.NewGraphite(host, port)
	if err != nil {
		Graphite = graphite.NewGraphiteNop(host, port)
	}
	log.Info(fmt.Sprintf("Graphite conn: %#v", Graphite))
}

func init() {
	loadGraphite()
}

func createMetrics(log LogMinute) (metrics []graphite.Metric) {
	// XXX (mmr) : this is ugly... maybe some reflection juice?
	data := map[string]string{
		"connections": log.Connections,
		"sessions":    log.Sessions,
		"selects":     log.Selects,
		"inserts":     log.Inserts,
		"updates":     log.Updates,
		"deletes":     log.Deletes,
		"max":         log.Max,
		"min":         log.Min,
		"duration":    log.Duration,
	}
	prefix := config.Graphite.MetricPrefix
	for k, v := range data {
		name := fmt.Sprintf("%s.%s", prefix, k)
		metrics = append(metrics, graphite.NewMetric(name, v, log.Timestamp))
	}
	return metrics
}

// Send log lines to Graphite
func SendLogs(logs Logs) {
	log.Info("Sending logs to Graphite...")

	var metrics []graphite.Metric
	for _, log := range logs {
		metrics = append(metrics, createMetrics(log)...)
	}
	err := Graphite.SendMetrics(metrics)
	if err != nil {
		log.Panic(err)
	}
}
