package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"code.google.com/p/gcfg"
	"flag"
	log "github.com/Sirupsen/logrus"
)

const (
	consumedSuffix       = ".consumed"
	defaultConfigFile    = "pglog-processor.conf"
	badgerCmd            = "/usr/local/bin/pgbadger"
	timeStampParseLayout = "2006-01-02 15:04:05"
	timeStampPrintLayout = "2006-01-02T15:04:05.999999+00:00"
	actionKeyOnES        = "PgSlowestQueries"
)

// Config contains configuration data read from conf file.
// Main.RunDockerCmd is useful for local testing.
type Config struct {
	Main struct {
		SleepTimeDuration time.Duration
		SleepTime         string
		OutputFilePath    string
		LogLevel          string
		Test              bool
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

	var confPath string
	flag.StringVar(&confPath, "conf", defaultConfigFile, "Config file path")
	test := flag.Bool("test", false, "Testing?")
	flag.Parse()

	log.WithField("Using configuration file", confPath).Info()

	err := gcfg.ReadFileInto(&config, confPath)
	check(err, "Couldn't set configuration", log.Fields{"configFile": confPath})
	config.Main.SleepTimeDuration, err = time.ParseDuration(config.Main.SleepTime)
	check(err, "Couldn't set duration", log.Fields{"configFile": confPath})
	log.WithField("config", config).Info()
	level, err := log.ParseLevel(config.Main.LogLevel)
	check(err, "Couldn't set debug level", log.Fields{"level": config.Main.LogLevel})

	log.SetLevel(level)
	config.Main.Test = *test
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
		appendLog(converted)
		consumed(fd)

		time.Sleep(config.Main.SleepTimeDuration)
	}
}

// Simple error checking. Wraps log utilities.
func check(err error, panicMsg string, panicFields log.Fields) {
	if err == nil {
		return
	}
	log.WithError(err).Error()
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

	if config.Main.Test {
		cmd = exec.Command("docker",
			"run",
			"--entrypoint", "pgbadger",
			"-v", "/data:/data",
			"--rm", "loggi/pglog-processor",
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
	for _, tps := range j.PgBadgerTopSlowest {
		res, err := json.Marshal(tps)
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

// Appends the given byte array to target file, saving it.
func appendLog(converted []byte) {
	outFile := config.Main.OutputFilePath
	f, errOpen := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)
	defer f.Close()
	check(errOpen, "Couldn't open output file", log.Fields{"outFile": outFile})
	_, errWrite := f.Write(converted)
	f.Sync()
	check(errWrite, "Couldn't write to output", log.Fields{"outFile": outFile})
}
