package main

import (
	"io/ioutil"
)

// Fetch log lines from RDS using AWS cli
func FetchLogs() string {
	log.Info("Fetching logs...")
	dat, _ := ioutil.ReadFile("minimal.log")
	return string(dat)
}
