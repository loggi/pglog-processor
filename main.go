package main

import (
	"code.google.com/p/gcfg"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/marpaia/graphite-golang"
	"time"
)

const (
	sleepTime  = 2 * time.Second
	configFile = "/etc/pglogger.conf"
)

var log *logrus.Logger
var config Config

type Config struct {
	AwsCredentials struct {
		Key       string
		SecretKey string
	}
	Graphite struct {
		Host string
		Port int
	}
}

func sleep() {
	time.Sleep(sleepTime)
}

func work() {
	SendLogs(AnalyseLogs(FetchLogs()))
}

func loadGraphite() {
	host := config.Graphite.Host
	port := config.Graphite.Port
	Graphite, err := graphite.NewGraphite(host, port)
	if err != nil {
		Graphite = graphite.NewGraphiteNop(host, port)
	}
	log.Info(fmt.Sprintf("Graphite conn: %#v", Graphite))
}

func loadLogging() {
	// XXX (mmr) : find a better way to do this
	log = logrus.New()
	fmt := new(logrus.TextFormatter)
	fmt.FullTimestamp = true
	log.Formatter = fmt
	log.Level = logrus.DebugLevel
}

func loadConfig() {
	gcfg.ReadFileInto(&config, configFile)
}

func init() {
	loadLogging()
	loadConfig()
	loadGraphite()
}

func main() {
	log.Info("Firing up...")
	for {
		work()
		sleep()
	}
}
