package kclgo

type KCLConfig struct {
	InputFileName           string
	OutputFileName          string
	ErrorFileName           string
	SleepSeconds          int
	CheckPointRetries      int
	CheckPointFreqSeconds int
}
