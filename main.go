package main

import (
	"code.google.com/p/gcfg"
	"github.com/Sirupsen/logrus"
	"time"
)

const (
	sleepTime  = 2 * time.Second
	configFile = "/etc/pglogger/pglogger.conf"
)

var log *logrus.Logger
var config Config

type Config struct {
	AwsCredentials struct {
		Key       string
		SecretKey string
	}
	Graphite struct {
		Host         string
		Port         int
		MetricPrefix string
	}
}

func sleep() {
	time.Sleep(sleepTime)
}

func work() {
	SendLogs(AnalyzeLogs(FetchLogs()))
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
}

func main() {
	log.Info("Firing up...")
	for {
		work()
		sleep()
	}
}
