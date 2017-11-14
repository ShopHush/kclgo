package kclgo

import (
	"log"
	"os"
	"strconv"

	"github.com/rickar/props"
)

var _ ConfigInterface = (*KCLConfig)(nil)

type KCLConfig struct {
	StreamName            string
	InputFileName         string
	OutputFileName        string
	ErrorFileName         string
	SleepSeconds          int
	CheckPointRetries     int
	CheckPointFreqSeconds int
	OutLogger             LoggerInterface
	OutLoggerFileName     string
	ErrLogger             LoggerInterface
	ErrLoggerFileName     string
}

// Implements the config interface to parse from a java properties file
func (cfg *KCLConfig) Parse(propertiesFile string) error {
	f, err := os.Open(propertiesFile)
	if err != nil {
		return err
	}
	p, err := props.Read(f)
	if err != nil {
		return err
	}

	cfg.StreamName = p.GetDefault("streamName", "")
	cfg.InputFileName = p.GetDefault("InputFileName", "")
	cfg.OutputFileName = p.GetDefault("OutputFileName", "")
	cfg.ErrorFileName = p.GetDefault("ErrorFileName", "")
	sleep := p.GetDefault("sleepSeconds", "5")
	sleepVal, err := strconv.Atoi(sleep)
	if err != nil {
		return err
	}
	cfg.SleepSeconds = sleepVal

	checkRetry := p.GetDefault("checkPointRetries", "5")
	checkVal, err := strconv.Atoi(checkRetry)
	if err != nil {
		return err
	}
	cfg.CheckPointRetries = checkVal

	checkRetrySeconds := p.GetDefault("checkPointFreqSeconds", "60")
	checkFreq, err := strconv.Atoi(checkRetrySeconds)
	if err != nil {
		return err
	}
	cfg.CheckPointFreqSeconds = checkFreq

	// default loggers, if you want to use your own logger, add them to your own config object

	cfg.OutLoggerFileName = p.GetDefault("outLoggerFileName", "")
	if cfg.OutLoggerFileName == "" {
		// this isn't great... the KCL java library is listening on stdout, better to leave that open for comms
		cfg.OutLogger = log.New(os.Stdout, "KCLgo/", log.LstdFlags)
	} else {
		f, err := os.OpenFile(cfg.OutLoggerFileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		cfg.OutLogger = log.New(f, "KCLgo/", log.LstdFlags)
	}

	cfg.ErrLoggerFileName = p.GetDefault("errLoggerFileName", "")
	if cfg.ErrLoggerFileName == "" {
		// this isn't great... the KCL java library is listening on stderr, better to leave that open for comms
		cfg.ErrLogger = log.New(os.Stderr, "KCLgo/", log.LstdFlags)
	} else {
		f, err := os.OpenFile(cfg.ErrLoggerFileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		cfg.ErrLogger = log.New(f, "KCLgo/", log.LstdFlags)
	}

	return nil
}

func NewConfigFromPropsFile(propertiesFile string) (*KCLConfig, error) {
	cfg := new(KCLConfig)
	err := cfg.Parse(propertiesFile)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
