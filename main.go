package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"time"
)

const SLEEP_TIME = 5 * time.Second
const AWS_LOG_PREFIX = "%t:%r:%u@%d:[%p]:"
const PG_BADGER_CMD = "/usr/local/bin/pgbadger"
const OUTPUT_JSON_FILE_PREFIX = "/data/output"
const INC_CTRL_FILE = "/data/inc_ctrl.dat"

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

func analyzeLogLines(lines []string) []string {
	now := time.Now()
	output_file := fmt.Sprintf("%s-%d%d%d.json",
	    OUTPUT_JSON_FILE_PREFIX, now.Year(), now.Month(), now.Day())

	cmd := exec.Command(PG_BADGER_CMD,
	  "--prefix", AWS_LOG_PREFIX,
		"--last-parsed", INC_CTRL_FILE,
		"--output", output_file,
		"-",
  )
	stdin, _ := cmd.StdinPipe()
	defer stdin.Close()
	for _, line := range lines {
		stdin.Write([]byte(line))
	}
	fmt.Println(cmd.Stdout)
	return []string{"proc1", "proc2"}
}

func saveLogLines(data []string) int {
	return 10
}

func work() {
	lines := fetchLogLines(10, 20)
	data := analyzeLogLines(lines)
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
