package kclgo

// This is the main interface to implement to process KCL records with your code
type RecordProcessor interface {
	Initialize(*InitializeInput) error
	ProcessRecords(*ProcessRecordsInput) error
	CheckPoint(sequenceNumber string, subSequenceNumber int) error
	Shutdown(*ShutdownInput) error
	ShutdownRequested(*ShutdownRequestedInput) error
}

// If you need more complex record processing than this (Taking a single record, processing and returning an error)
// Then create your own RecordProcessor interface and implement your own ProcessRecords and the rest of the interface
// If your record processing is simple, just provide a function that implements this interface to the
// NewDefaultRecordProcessor function and then creating your own record processor is simple
type RecordProcessingFunc interface {
	ProcessRecord(Record) error
}

type CheckPointer interface {
	//CheckPoints at a particular sequence number you provide or if no sequence number is given, the CheckPoint will be
	// at the end of the most recently delivered list of records
	CheckPoint(sequenceNumber string, subSequenceNumber int) error
}

type ActionInterface interface {
	Perform(RecordProcessor) error
	GetAction() string
}

type ConfigInterface interface {
	Parse(string) error
}

// pass in whatever logger you want to use.
type LoggerInterface interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
