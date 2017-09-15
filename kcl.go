package kclgo

import (
	"fmt"
	"log"
)

type KCL struct {
	handler *ioHandler
	cp *KCLCheckPointer
	processor *KCLRecordProcessor
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
	k.reportDone(action)
	return nil
}

func (k *KCL) reportDone(action ActionInterface) {
	k.handler.WriteActionResponse(getActionResponse(action.GetAction()))

}
func (k *KCL) handleLine(line *string) {
	action, err := k.handler.LoadAction(line)
	if err != nil {
		log.Printf("Error loading line: %s\n", err.Error())
	}
	err = k.performAction(action)
	if err != nil {
		log.Printf("Error loading line: %s\n", err.Error())
	}
	k.reportDone(action)
}
func (k *KCL) Run() {

	for {
		line, err := k.handler.ReadLine()
		if err != nil {
			log.Println(err)
		}
		k.handleLine(&line)
	}
}
