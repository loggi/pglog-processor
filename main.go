package main

import (
	"code.google.com/p/gcfg"
	"errors"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"os/exec"
	"time"
	"bytes"
	"os"
	"strings"
	"encoding/json"
)

const (
	consumedSuffix = ".consumed"
	configFile = "/data/pglogger.conf"
	//badgerCmd = "/usr/local/bin/pgbadger"
	badgerCmd = "docker run --entrypoint pgbadger -v /data:/data --rm loggi/pglogger"
)

type Config struct {
	Main struct {
		DebugLevel string
		SleepTimeDuration time.Duration
		SleepTime string
	}
	PgBadger struct {
		InputDir       string
		OutputDir      string
		Prefix         string
		LastParsedFile string
	}
	Graphite struct {
		Host         string
		Port         int
		MetricPrefix string
	}
}

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
		data := analyze(fd)
		convert(data)

		// TODO seiti - write after transforming
		outFile := config.PgBadger.OutputDir + "/" + fd.filename + ".json"
		err = ioutil.WriteFile(
			outFile,
			data,
			0666)
		check(err, "Couldn't write to output", log.Fields{"outFile": outFile})

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

// Run pgbadger for the given lines and return an array of LogMinutes
// XXX TODO return correct data
func analyze(f FileDesc) []byte {
	log.WithField("filepath", f.filepath()).Info("Analyzing")

	cmd := exec.Command("docker",
		"run",
		"--entrypoint", "pgbadger",
		"-v", "/data:/data",
		"--rm", "loggi/pglogger",
		"--prefix", config.PgBadger.Prefix,
//		"--last-parsed", config.PgBadger.LastParsedFile,
		"--outfile", "-",
		"--extension", "json",
		//		"-O", logFilesDir,
		//		"-o", f.filename + ".json",
		f.filepath(),
	)
	// TODO XXX seiti - revert to simple command (using docker container in local env)
//	cmd := exec.Command(badgerCmd,
//		"--prefix", config.PgBadger.Prefix,
//		"--last-parsed", config.PgBadger.LastParsedFile,
//		"--outfile", "-",
//		"--extension", "json",
//		//		"-O", logFilesDir,
//		//		"-o", f.filename + ".json",
//		f.filepath(),
//	)

	var e bytes.Buffer
	cmd.Stderr = &e
	out, err := cmd.Output()
	check(err, "Couldn't run analyzer", log.Fields{
		"cmd": badgerCmd,
		"filepath": f.filepath(),
		"msg": string(e.Bytes())})
	return out
}

func convert(data []byte) {
	log.WithField("data len", len(data)).Info("Converting")

	var j PgBagerJson
	log.WithField("data", string(data)).Debug("Data ready to be converted")
	err := json.Unmarshal(data, &j)
	check(err, "Couldn't unmarshal data", log.Fields{})

	log.WithField("unmarshaled", j).Debug()

	// TODO XXX seiti - convert to another struct, marshall and return



//	var logs Logs
//	for date, info := range logFile.PerMinuteInfo {
//		for hour, info := range info {
//			for min, info := range info {
//				timeStr := fmt.Sprintf("%s%s%s", date, hour, min)
//				moment, err := time.Parse(dateLayout, timeStr)
//				if err != nil {
//					log.Panic(err)
//				}
//				fmt.Printf("%s:%s:%s SELECT: %v\n", date, hour, min, info.Insert.Count)
//				logs = append(logs, newLogLine(moment, info))
//			}
//		}
//	}
//	sort.Sort(logs)
}

func consumed(f FileDesc) {
	old := f.filepath()
	new := f.filepath() + consumedSuffix
	err := os.Rename(old, new)
	check(err, "Couldn't rename file, to set as consumed", log.Fields{
		"old": old,
		"new": new,
	})
}


// JSON struct representing pgBadger output
// List of first level keys (note that not all is of interest):
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
type PgBagerJson struct {
	TopSlowest []TDS `json:"top_slowest"`
}

type TDS struct {
	Dur time.Duration
	Tim time.Time
	Que string
	Ser string
	App string
	W string
	X string
	Y string
	Z string
}

func (o *TDS) UnmarshalJSON(data []byte) error {
	var v [9]string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	// TODO seiti - add error checking!!!
	o.Dur, _ = time.ParseDuration(v[0] + "ms")
	o.Tim, _ = time.Parse("2006-01-02 15:04:05", v[1]) // TODO extract layout
	o.Que = v[2]
	o.Ser = v[3]
	o.App = v[4]
	o.W = v[5]
	o.X = v[6]
	o.Y = v[7]
	o.Z = v[8]
	return nil
}