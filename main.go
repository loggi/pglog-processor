package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"code.google.com/p/gcfg"
	log "github.com/Sirupsen/logrus"
)

const (
	consumedSuffix  = ".consumed"
	configFile      = "/data/pglogger.conf"
	badgerCmd       = "/usr/local/bin/pgbadger"
	timeStampLayout = "2006-01-02 15:04:05"
	actionKeyOnES   = "pgSlowestQueries"
)

// Config contains configuration data read from conf file.
type Config struct {
	Main struct {
		DebugLevel        string
		SleepTimeDuration time.Duration
		SleepTime         string
		RunDockerCmd      bool
		OutputFilePath    string
	}
	PgBadger struct {
		InputDir string
		Prefix   string
	}
	Graphite struct {
		Host         string
		Port         int
		MetricPrefix string
	}
}

// FileDesc is a parameter object.
type FileDesc struct {
	filename string
	dirpath  string
}

func (f *FileDesc) filepath() string {
	return f.dirpath + "/" + f.filename
}

var config Config

func init() {
	err := gcfg.ReadFileInto(&config, configFile)
	check(err, "Couldn't set configuration", log.Fields{"configFile": configFile})
	config.Main.SleepTimeDuration, err = time.ParseDuration(config.Main.SleepTime)
	check(err, "Couldn't set duration", log.Fields{"configFile": configFile})
	log.WithField("config", config).Info()
	level, err := log.ParseLevel(config.Main.DebugLevel)
	check(err, "Couldn't set debug level", log.Fields{"level": config.Main.DebugLevel})
	log.SetLevel(level)
}

func main() {
	log.Info("Firing up...")

	// loop indefinitely
	//     list files and select one to analyze
	//     analyze (after analyze, remove? rename?)
	//     generate and save output
	for {
		fd, err := find()
		if err != nil {
			log.WithError(err).Info(
				"If there are too many of this error, increase sleeptime.")
			time.Sleep(config.Main.SleepTimeDuration)
			continue
		}

		analyzed := analyze(fd)
		converted := convert(analyzed)
		appendlog(converted)
		consumed(fd)

		time.Sleep(config.Main.SleepTimeDuration)
	}
}

func check(err error, panicMsg string, panicFields log.Fields) {
	if err == nil {
		return
	}
	log.WithError(err).Info()
	log.WithFields(panicFields).Panic(panicMsg)
	panic(panicMsg)
}

// Find a file to be analyzed. Returns the first file not marked as consumed.
func find() (FileDesc, error) {
	dir := config.PgBadger.InputDir
	files, err := ioutil.ReadDir(dir) // files sorted by name
	check(err, "Couldn't read dir", log.Fields{"dir": dir})
	for i := 0; i < len(files); i++ {
		f := files[i]
		if !strings.HasSuffix(f.Name(), consumedSuffix) {
			return FileDesc{filename: f.Name(), dirpath: dir}, nil
		}
	}
	return FileDesc{}, errors.New("No files to read from")
}

// Run pgBadger and returns the generated data.
func analyze(f FileDesc) []byte {
	log.WithField("filepath", f.filepath()).Info("Analyzing")

	var cmd *exec.Cmd

	if config.Main.RunDockerCmd {
		cmd = exec.Command("docker",
			"run",
			"--entrypoint", "pgbadger",
			"-v", "/data:/data",
			"--rm", "loggi/pglogger",
			"--prefix", config.PgBadger.Prefix,
			"--outfile", "-",
			"--extension", "json",
			f.filepath(),
		)
	} else {
		cmd = exec.Command(badgerCmd,
			"--prefix", config.PgBadger.Prefix,
			"--outfile", "-",
			"--extension", "json",
			f.filepath(),
		)
	}

	var e bytes.Buffer
	cmd.Stderr = &e
	out, err := cmd.Output()
	check(err, "Couldn't run analyzer", log.Fields{
		"cmd":      badgerCmd,
		"filepath": f.filepath(),
		"msg":      string(e.Bytes())})
	return out
}

// Convert given data, in json format, to another json ready to be sent to ES
func convert(data []byte) []byte {
	log.WithField("data len", len(data)).Info("Converting")

	var j PgBadgerOutputData
	log.WithField("data", string(data)).Debug("Data ready to be converted")
	err := json.Unmarshal(data, &j)
	check(err, "Couldn't unmarshal data", log.Fields{})

	log.WithField("unmarshaled", j).Debug()

	var converted []byte
	for _, tps := range j.PgBadgerTopSlowest{
		res, err := json.Marshal(tps)
		check(err, "Couldn't marshal object", log.Fields{"object": j})
		log.WithField("marshaled", string(res)).Debug()
		check(err, "Couldn't marshal object", log.Fields{"object": j})
		log.WithField("marshaled", string(res)).Debug()
		res = append(res, []byte("\n")...)
		converted = append(converted, res...)
	}
	return converted
}

// markAsConsumed marks the given file as consumed, avoiding re-reading it.
func consumed(f FileDesc) {
	old := f.filepath()
	new := f.filepath() + consumedSuffix
	err := os.Rename(old, new)
	check(err, "Couldn't rename file, to set as consumed", log.Fields{
		"old": old,
		"new": new,
	})
}

// Struct representing pgBadger output
// List of first level keys (note that not all is of interest):
//
//	"normalyzed_info",
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
//	"top_slowest"
type PgBadgerOutputData struct {
	PgBadgerTopSlowest []TopSlowest `json:"top_slowest"`
}

type Milli time.Duration

// TopSlowest holds the mapped data to be marshaled and sent to ES.
type TopSlowest struct {
	Action      string    `json:"action"`
	Timestamp   time.Time `json:"@timestamp"`
	Duration    Milli     `json:"duration"`
	Query       string    `json:"query"`
	Server      string    `json:"server"`
	Application string    `json:"application"`
}

// UnmarshalJSON overrides the default unmarshalling, enabling PG log parsing.
func (o *TopSlowest) UnmarshalJSON(data []byte) error {
	var v [9]string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	o.Action = "pgSlowestQueries"
	duration, err := time.ParseDuration(v[0] + "ms")
	if err != nil {
		return err
	}
	timestamp, err := time.Parse(timeStampLayout, v[1])
	if err != nil {
		return err
	}
	o.Timestamp = timestamp
	o.Duration = Milli(duration)
	o.Query = v[2]
	o.Server = v[3]
	o.Application = v[4]
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

func appendlog(converted []byte) {
	outFile := config.Main.OutputFilePath
	f, errOpen := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)
	defer f.Close()
	check(errOpen, "Couldn't open output file", log.Fields{"outFile": outFile})
	_, errWrite := f.Write(converted)
	f.Sync()
	check(errWrite, "Couldn't write to output", log.Fields{"outFile": outFile})
}
