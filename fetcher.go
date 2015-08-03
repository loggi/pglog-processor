package main

import (
	"io/ioutil"
)

// Fetch log lines from RDS using AWS cli
func FetchLogLines() string {
	dat, _ := ioutil.ReadFile("minimal.log")
	return string(dat)
}
