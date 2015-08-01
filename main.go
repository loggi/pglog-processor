package main

import (
	"bytes"
	"fmt"
	"log"
	"time"
)

const SLEEP_TIME = 5 * time.Second
const AWS_LOG_PREFIX = "%t:%r:%u@%d:[%p]:"
const PG_BADGER_CMD = "/usr/local/bin/pgbadger"

func sleep() {
	time.Sleep(SLEEP_TIME)
}

func info(msg string) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags)
	logger.Print(msg)
	fmt.Print(&buf)
}

func fetchLogLines(limit, offset int) []string {
	info("Fetching log portion...")
	return []string{"raw1", "raw2"}
}

func processLogLines(lines []string) []string {
	return []string{"proc1", "proc2"}
}

func saveLogLines(data []string) int {
	return 10
}

func work() {
	lines := fetchLogLines(10, 20)
	data := processLogLines(lines)
	saved := saveLogLines(data)
	if saved > 0 {
		info(fmt.Sprintf("Saved %d lines...", saved))
	}
}

func main() {
	info("Firing up...")
	for {
		work()
		sleep()
	}
}
