package types

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"strings"
	"time"
)

// Top Slowest: tsl
const (
	TslTimeStampParseLayout = "2006-01-02 15:04:05"
	TslStampPrintLayout     = "2006-01-02T15:04:05.999999+00:00"
	TslActionKeyOnES        = "PgSlowestQueries"
)

// NormalizedInfo: nfo
const (
	NfoTimeStampParseLayout = "200601021504"
	NfoTimeStampPrintLayout = TslStampPrintLayout
	NfoActionKeyOnES        = "PgNormalizedQueries"
)

// PerMinuteInfo: pmi
const (
	PmiTimeStampParseLayout = "200601021504"
	PmiTimeStampPrintLayout = TslStampPrintLayout
	PmiActionKeyOnES = "PgPerMinuteInfo"
)

// pgBadger output, list of first level keys (note that not all is of interest):
//
//	"normalyzed_info" (sic)  DONE
//	"top_slowest"            DONE
//	"per_minute_info"
//	"user_info"
//	"top_locked_info"
//	"host_info"
//	"autovacuum_info"
//	"autoanalyze_info"
//	"tempfile_info",
//	"top_tempfile_info"
//	"session_info"
//	"log_files"
//	"logs_type"
//	"checkpoint_info"
//	"connection_info"
//	"overall_checkpoint"
//	"error_info"
//	"database_info"
//	"overall_stat"
//	"nlines"
//	"lock_info"
// 	"application_info"
type PgBadgerOutputData struct {
	PgBadgerNormalyzedInfo NormalizedInfo `json:"normalyzed_info"`
	PgBadgerPerMinuteInfo  PerMinuteInfo  `json:"per_minute_info"`
	PgBadgerTopSlowest     []TopSlowest   `json:"top_slowest"`
}

// Milli type is required to make duration unmarshalling flexible.
// We just need to save milliseconds granularity.
type Milli time.Duration

// String overriding to print milliseconds, not nanoseconds.
func (o Milli) String() string {
	return fmt.Sprintf("%v", time.Duration(o).Nanoseconds()/1e6)
}

// MarshalJSON overriding to print milliseconds, not nanoseconds.
func (o Milli) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", o)), nil
}

// UnmarshalJSON overriding to force duration format
func (o *Milli) UnmarshalJSON(data []byte) error {
	// coherently using the same layout used to print.
	adjusted := fmt.Sprintf("%sms", strings.Trim(string(data), `"`))
	if dur, err := time.ParseDuration(adjusted); err != nil {
		return err
	} else {
		*o = Milli(dur)
	}
	return nil
}

// Timestamp type is required to make time unmarshalling flexible.
// We need to save using a specific layout.
type Timestamp time.Time

// String overriding to force accepted timestamp format
func (t Timestamp) String() string {
	return time.Time(t).Format(TslStampPrintLayout)
}

// MarshalJSON overriding to force accepted timestamp format
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, t)), nil
}

// UnmarshalJSON overriding to force timestamp format
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	// coherently using the same layout used to print.
	if ts, err := time.Parse(TslStampPrintLayout, strings.Trim(string(data), `"`)); err != nil {
		return err
	} else {
		*t = Timestamp(ts)
	}
	return nil
}

// TopSlowest represents the top slowest queries, including the values bound.
// In addition, this struct represents the ElasticSearch format data exactly.
type TopSlowest struct {
	Action    string    `json:"action"`
	Timestamp Timestamp `json:"@timestamp"`
	Duration  Milli     `json:"duration"`
	Query     string    `json:"query"`
	Username  string    `json:"username"`
	Database  string    `json:"database"`
}

// UnmarshalJSON overrides the default unmarshalling, enabling pgBadger output
// parsing.
// The pgBadger `top_slowest` section is structured as a array of values.
func (o *TopSlowest) UnmarshalJSON(data []byte) error {
	var v [9]string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	o.Action = TslActionKeyOnES
	duration, err := time.ParseDuration(v[0] + "ms")
	if err != nil {
		return err
	}
	timestamp, err := time.Parse(TslTimeStampParseLayout, v[1])
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

// NormalizedInfo contains the most run generic queries in the time period.
// It includes average duration anc count per minute.
// The data output from pgBadger is tree structured, but we want to send
// to ES and/or Graphite a flat data format.
//
// The following represents the data output by pgBadger:
//`
// map["select 1":
//   map["chronos":
//     map["20151006":
//       map[
//         "19":
//           map[
//             "min": map["00":3, "01":3]
//             "min_duration": map[01:215.289 00:233.06]
//             "count":6
//             "duration":7369.941
//           ]
//       ]
//     ]
//   ]
// ]
//`
type NormalizedInfo struct {
	Entries []NormalizedInfoEntry
}

// NormalizedInfo "Output" format
type NormalizedInfoEntry struct {
	Action    string    `json:"action"`
	Timestamp Timestamp `json:"@timestamp"`
	Duration  Milli     `json:"duration"`
	Query     string    `json:"query"`
	Count     int       `json:"count"`
}

// NormalizedInfo "Input" format
type Chronos struct {
	//          date       hour
	Chronos map[string]map[string]NormalizedInfoMinute
}

type NormalizedInfoMinute struct {
	Count        int
	Duration     float64 // TODO change to Duration
	Min          map[string]int
	Min_Duration map[string]float64
}

func (o *NormalizedInfo) UnmarshalJSON(data []byte) error {
	//      map[query]Chronos
	var res map[string]Chronos
	if err := json.Unmarshal(data, &res); err != nil {
		return err
	}

	for query, c := range res {
		for date, h := range c.Chronos {
			for hour, m := range h {
				for minute, count := range m.Min {
					en := NormalizedInfoEntry{
						Action: NfoActionKeyOnES,
						Query:  query,
						Count:  count,
					}
					if ts, err := time.Parse(NfoTimeStampParseLayout, date+hour+minute); err != nil {
						log.WithError(err).Error("Could not process")
						continue
					} else {
						en.Timestamp = Timestamp(ts)
					}

					if dur, err := time.ParseDuration(fmt.Sprintf("%fms", m.Min_Duration[minute])); err != nil {
						log.WithError(err).Error("Could not process")
						continue
					} else {
						en.Duration = Milli(dur)
					}
					o.Entries = append(o.Entries, en)
				}
			}
		}
	}
	//		fmt.Println(en)
	log.WithField("NormalizedInfo UnmarshalJSON", res).Debug()
	return nil
}

// PerMinuteInfo contains metrics per minute.
// The following represents the data output by pgBadger:
//`
// map["20151006":                                  -> date
//   map["18":                                      -> hour
//     map["01":                                    -> min
//       map[
//         "SELECT":                                -> query type
//           map[
//             "count": 6                           -> count
//             "duration": 233.06                   -> acum per minute
//             "second": map["01": 3, "03":3]       -> not interested...
//           ]
//       ]
//     ]
//   ]
// ]
//`
// The 'query type' can be one of 'SELECT', 'OTHERS', 'query' or 'session',
// where 'query' aggregates the duration sum of 'SELECT" and "OTHERS" and
// 'session' is not relevant.
type PerMinuteInfo struct {
	Entries []PerMinuteInfoEntry
}

type PerMinuteInfoEntry struct {
	Action    string    `json:"action"`
	Desc      string    `json:"desc"`
	Timestamp Timestamp `json:"@timestamp"`
	Duration  Milli     `json:"duration"`
	Count     int       `json:"count"`
}

type PerMinuteInfoMinute struct {
	Count    int
	Duration float64 // TODO change to Duration
	Second   interface{}
}

func (o *PerMinuteInfo) UnmarshalJSON(data []byte) error {
	//      map[date]  map[hour]  map[minute]map[desc]  PerMinuteInfoMinute
	var res map[string]map[string]map[string]map[string]PerMinuteInfoMinute
	if err := json.Unmarshal(data, &res); err != nil {
		return err
	}

	for date, h := range res {
		for hour, m := range h {
			for minute, d := range m {
				for desc, info := range d {
					en := PerMinuteInfoEntry{
						Action:    PmiActionKeyOnES,
						Desc:      desc,
						Count:     info.Count,
					}
					if ts, err := time.Parse(PmiTimeStampParseLayout, date+hour+minute); err != nil {
						log.WithError(err).Error("Could not process")
						continue
					} else {
						en.Timestamp = Timestamp(ts)
					}

					if dur, err := time.ParseDuration(fmt.Sprintf("%fms", info.Duration)); err != nil {
						log.WithError(err).Error("Could not process")
						continue
					} else {
						en.Duration = Milli(dur)
					}
					o.Entries = append(o.Entries, en)
				}
			}
		}
	}
	log.WithField("PerMinuteInfo UnmarshalJSON", res).Debug()
	return nil
}
