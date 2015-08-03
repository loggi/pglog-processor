package main

import (
	"bytes"
	"os/exec"
	"strings"
)

const (
	rdsLogPrefix = "%t:%r:%u@%d:[%p]:"
	badgerCmd    = "/usr/local/bin/pgbadger"
	incCtrlFile  = "/data/inc_ctrl.dat"
)

// Run pgbadger for the given lines and return a collectin of LogMinutes
func AnalyzeLogs(lines string) Logs {
	log.Info("Analyzing logs...")
	cmd := exec.Command(badgerCmd,
		"--prefix", rdsLogPrefix,
		"--last-parsed", incCtrlFile,
		"--outfile", "-",
		"--extension", "json",
		"--format", "stderr",
		"-",
	)

	// Handling Stdin
	cmd.Stdin = strings.NewReader(lines)

	// Handling Stdout
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	return ConvertLogs(out.String())
}
