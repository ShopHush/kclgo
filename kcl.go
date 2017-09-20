package kclgo

import (
	"fmt"
)

type KCL struct {
	handler      *IoHandler
	checkpointer CheckPointer
	processor    RecordProcessor
	config       *KCLConfig
}

func (k *KCL) performAction(action ActionInterface) (err error) {
	switch i := action.(type) {
	case *InitializeInput:
		err = k.processor.Initialize(i)
	case *ProcessRecordsInput:
		err = k.processor.ProcessRecords(i)
	case *ShutdownInput:
		err = k.processor.Shutdown(i)
	case *ShutdownRequestedInput:
		err = k.processor.ShutdownRequested(i)
	case *checkPointResponse:
		err = i.Perform(k.processor)
	default:
		return MalformedAction(fmt.Errorf("UnknownAction"))
	}

	if err != nil {
		return err
	}
	return nil
}

func (k *KCL) reportDone(action ActionInterface) {
	k.handler.WriteActionResponse(getActionResponse(action.GetAction()))

}
func (k *KCL) handleLine(line *string) {
	action, err := k.handler.LoadAction(line)
	if err != nil {
		k.config.ErrLogger.Printf("Error (%s) loading line: (%s) with action (%s)\n", *line, err.Error(), action.GetAction())
		return
	}
	err = k.performAction(action)
	if err != nil {
		k.config.ErrLogger.Printf("Error (%s) loading line: (%s) with action (%s)\n", *line, err.Error(), action.GetAction())
		return
	}
	switch action.(type) {
	case *checkPointResponse:
		// don't respond to the response
		return
	default:
		k.reportDone(action)
	}

}
func (k *KCL) Run() {
	for {
		line, err := k.handler.ReadLine()
		if err != nil {
			k.config.ErrLogger.Println(err)
		} else {
			k.handleLine(&line)
		}
	}
}

func NewDefaultKCL(config *KCLConfig, processingFunc RecordProcessingFunc) (*KCL, error) {
	k := new(KCL)
	k.config = config
	k.handler = NewIOHandler(config)
	if err := k.handler.Init(); err != nil {
		return nil, err
	}
	k.checkpointer = NewCheckPointer(k.handler)
	k.processor = NewDefaultRecordProcessor(config, k.handler, k.checkpointer, processingFunc)

	return k, nil
}

func NewKCL(config *KCLConfig, processor RecordProcessor) (*KCL, error) {
	k := new(KCL)
	k.config = config
	k.handler = NewIOHandler(config)
	if err := k.handler.Init(); err != nil {
		return nil, err
	}
	k.checkpointer = NewCheckPointer(k.handler)
	k.processor = processor

	return k, nil
}
