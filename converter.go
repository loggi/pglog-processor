package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Date layout in Go is based on a fixed date
// instead of using ambigous masks (yyyy-dd-mm, yadda-yadda)
// The fixed date is: Mon Jan 2 15:04:05 MST 2006  (MST is GMT-0700)
const dateLayout = "200601020304"

// Handle empty strings
func convMinMax(counter Counter) (min, max string) {
	maxStr := counter.Query.Max
	minStr := counter.Query.Min
	if minStr == "" {
		minStr = "0.0"
	}
	if maxStr == "" {
		maxStr = "0.0"
	}
	return minStr, maxStr
}

// For some weird reason we're getting the duration doubled
// We divide by 2 to handle that
func convDuration(counter Counter) string {
	duration := counter.Query.Duration / 2
	return fmt.Sprintf("%.2f", duration)
}

// Create a new LogLine for the given args
func newLogLine(moment time.Time, counter Counter) LogMinute {
	min, max := convMinMax(counter)
	return LogMinute{
		Timestamp:   moment.Unix(),
		Connections: string(counter.Connection.Count),
		Sessions:    string(counter.Session.Count),
		Selects:     string(counter.Select.Count),
		Inserts:     string(counter.Insert.Count),
		Updates:     string(counter.Update.Count),
		Deletes:     string(counter.Delete.Count),
		Duration:    convDuration(counter),
		Min:         min,
		Max:         max,
	}
}

// Convert pgbadger generated logs to our LogMinute struct
func ConvertLogs(lines string) Logs {
	var logFile LogFile
	json.Unmarshal([]byte(lines), &logFile)

	var logs Logs
	for date, info := range logFile.PerMinuteInfo {
		for hour, info := range info {
			for min, info := range info {
				timeStr := fmt.Sprintf("%s%s%s", date, hour, min)
				moment, err := time.Parse(dateLayout, timeStr)
				if err != nil {
					log.Panic(err)
				}
				logs = append(logs, newLogLine(moment, info))
			}
		}
	}
	sort.Sort(logs)
	return logs
}
