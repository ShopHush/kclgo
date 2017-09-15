package kclgo

type RecordProcessor interface {
	Initialize(*InitializeInput) error
	ProcessRecords(*ProcessRecordsInput) error
	CheckPoint(sequenceNumber string, subSequenceNumber int) error
	Shutdown(*ShutdownInput) error
	ShutdownRequested(*ShutdownRequestedInput) error
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
