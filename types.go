package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// Struct representing pgBadger output
// List of first level keys (note that not all is of interest):
//
//	"normalyzed_info", DONE
//	"user_info",
//	"top_locked_info",
//	"host_info",
//	"autovacuum_info",
//	"autoanalyze_info",
//	"tempfile_info",
//	"top_tempfile_info",
//	"session_info",
//	"log_files",
//	"logs_type",
//	"checkpoint_info",
//	"connection_info",
//	"overall_checkpoint",
//	"error_info",
//	"database_info",
//	"overall_stat",
//	"nlines",
//	"lock_info",
//	"per_minute_info",
//	"application_info",
//	"top_slowest" DONE
//
// Currently only top_slowest is converted. TODO add other stats.
type PgBadgerOutputData struct {
	PgBadgerTopSlowest []TopSlowest `json:"top_slowest"`
	//	PgBadgerNormalyzedInfo []NormalyzedInfo `json:"normalyzed_info"`
}

// Milli type is required to make duration unmarshalling flexible.
// We just need to save milliseconds granularity.
type Milli time.Duration

type Timestamp time.Time

// TopSlowest holds the mapped data to be marshaled and sent to ES.
type TopSlowest struct {
	Action    string    `json:"action"`
	Timestamp Timestamp `json:"@timestamp"`
	Duration  Milli     `json:"duration"`
	Query     string    `json:"query"`
	Username  string    `json:"username"`
	Database  string    `json:"database"`
}

// UnmarshalJSON overrides the default unmarshalling, enabling PG log parsing.
func (o *TopSlowest) UnmarshalJSON(data []byte) error {
	var v [9]string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	o.Action = actionKeyOnES
	duration, err := time.ParseDuration(v[0] + "ms")
	if err != nil {
		return err
	}
	timestamp, err := time.Parse(timeStampParseLayout, v[1])
	if err != nil {
		return err
	}
	o.Timestamp = Timestamp(timestamp)
	o.Duration = Milli(duration)
	o.Query = v[2]
	o.Username = v[3]
	o.Database = v[4]
	return nil
}

// String overriding to print milliseconds, not nanoseconds.
func (o Milli) String() string {
	return fmt.Sprintf("%v", time.Duration(o).Nanoseconds()/1e6)
}

// MarshalJSON overriding to print milliseconds, not nanoseconds.
func (o Milli) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", o)), nil
}

// String overriding to force accepted timestamp format
func (t Timestamp) String() string {
	return time.Now().Format(timeStampPrintLayout)
}

// MarshalJSON overriding to force accepted timestamp format
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, t)), nil
}