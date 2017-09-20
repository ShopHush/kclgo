package kclgo

var _ RecordProcessingFunc = (*ExampleProcessingFunc)(nil)

type ExampleProcessingFunc struct {
	config *KCLConfig
}

func (e *ExampleProcessingFunc) ProcessRecord(record Record) error {
	data, err := record.BinaryData()
	if err != nil {
		return err
	}
	//*******************************************************
	// Put your custom record processing code here in your record processor

	e.config.OutLogger.Printf("%s\n", string(data))

	//*******************************************************

	return nil
}
