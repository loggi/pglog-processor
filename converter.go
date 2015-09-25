package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"sort"
	"time"
)

// Date layout in Go is based on a fixed date
// instead of using ambiguous masks (yyyy-dd-mm, yadda-yadda)
// The fixed date is: Mon Jan 2 15:04:05 MST 2006  (MST is GMT-0700)
const dateLayout = "200601021504"

// For some weird reason we're getting the duration doubled
// We divide by 2 to handle that
func convDuration(counter Counter) string {
	duration := counter.Query.Duration / 2
	return fmt.Sprintf("%.2f", duration)
}

func handleEmpty(str string) string {
	if str == "" {
		return "0"
	}
	return str
}

func conv(v int) string {
	return handleEmpty(string(v))
}

// Create a new LogLine for the given args
func newLogLine(moment time.Time, counter Counter) LogMinute {
	return LogMinute{
		Timestamp:   moment.Unix(),
		Connections: conv(counter.Connection.Count),
		Sessions:    conv(counter.Session.Count),
		Selects:     conv(counter.Select.Count),
		Inserts:     conv(counter.Insert.Count),
		Updates:     conv(counter.Update.Count),
		Deletes:     conv(counter.Delete.Count),
		Duration:    convDuration(counter),
		Min:         handleEmpty(counter.Query.Min),
		Max:         handleEmpty(counter.Query.Max),
	}
}

// Convert pgbadger generated logs to our LogMinute struct
func ConvertLogs(lines string) Logs {
	log.Info("Converting logs...")
	log.WithField("lines", lines).Debug()
	var logFile LogFile

	json.Unmarshal([]byte(lines), &logFile)
	// TODO - remove hardcoded keys
	fmt.Println("SELECTS:", logFile.PerMinuteInfo["20150801"]["11"]["00"])

	var logs Logs
	for date, info := range logFile.PerMinuteInfo {
		for hour, info := range info {
			for min, info := range info {
				timeStr := fmt.Sprintf("%s%s%s", date, hour, min)
				moment, err := time.Parse(dateLayout, timeStr)
				if err != nil {
					log.Panic(err)
				}
				fmt.Printf("%s:%s:%s SELECT: %v\n", date, hour, min, info.Insert.Count)
				logs = append(logs, newLogLine(moment, info))
			}
		}
	}
	sort.Sort(logs)
	return logs
}
