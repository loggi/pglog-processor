package main

import (
	"bytes"
	//	"encoding/json"
	"fmt"
	"github.com/marpaia/graphite-golang"
	"log"
	"os/exec"
	"strings"
	"time"
)

const (
	SLEEP_TIME     = 2 * time.Second
	RDS_LOG_PREFIX = "%t:%r:%u@%d:[%p]:"
	PGBADGER_CMD   = "/usr/local/bin/pgbadger"
	INC_CTRL_FILE  = "/data/inc_ctrl.dat"
	GRAPHITE_HOST  = "127.0.0.1"
	GRAPHITE_PORT  = 9999
)

type LogMinute struct {
	Timestamp   time.Time
	Connections int
	Sessions    int
	Selects     int
	Inserts     int
	Updates     int
	Max         float64
	Min         float64
	Duration    float64
}

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
	cmd := exec.Command(PGBADGER_CMD,
		"--prefix", RDS_LOG_PREFIX,
		"--last-parsed", INC_CTRL_FILE,
		"--outfile", "-",
		"--extension", "json",
		"--format", "stderr",
		"-",
	)

	// Handling Stdin
	cmd.Stdin = strings.NewReader(strings.Join(lines, "\n"))

	// Handling Stdout
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out.String())

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

func loadGraphite() {
	Graphite, err := graphite.NewGraphite(GRAPHITE_HOST, GRAPHITE_PORT)
	if err != nil {
		Graphite = graphite.NewGraphiteNop(GRAPHITE_HOST, GRAPHITE_PORT)
	}
	info(fmt.Sprintf("Graphite conn: %#v", Graphite))
}

func init() {
	loadGraphite()
}

func main() {
	info("Firing up...")
	for {
		work()
		sleep()
	}
}
