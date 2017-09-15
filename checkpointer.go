package kclgo

import (
	"encoding/json"
	"errors"
)

var _ CheckPointer = (*KCLCheckPointer)(nil)

type KCLCheckPointer struct {
	handler *ioHandler
}

// CheckPoints at a particular sequence number you provide or if no sequence number is given, the checkpoint will
// be at the end of the most recently delivered list of records
func (c *KCLCheckPointer) CheckPoint(sequenceNumber string, subSequenceNumber int) error {
	var seq *string
	if sequenceNumber != "" {
		seq = &sequenceNumber
	}

	message := CheckPointRequest{
		Action:            "checkpoint",
		SequenceNumber:    seq,
		SubSequenceNumber: subSequenceNumber,
	}
	err := c.handler.WriteCheckPointRequest(message)
	if err != nil {
		return err
	}
	return c.getResponse()
}

func (c *KCLCheckPointer) getResponse() error {
	dat, err := c.handler.ReadLine()
	if err != nil {
		return err
	}

	var response checkPointResponse
	if err = json.Unmarshal([]byte(dat), &response); err != nil {
		return err
	}

	if response.Error != nil {
		return CheckPointError(errors.New(*response.Error))
	}

	if response.Action != "checkpoint" {
		// We are in an invalid state. We will raise a checkpoint exception
		// to the RecordProcessor indicating that the KCL (or KCLgo) is in
		// an invalid state. See KCL documentation for description of this
		// exception. Note that the documented guidance is that this exception
		// is NOT retryable so the client code should exit.
		return CheckPointError(errors.New("InvalidStateException"))
	}

	return nil
}

func NewCheckPointer(handler *ioHandler) *KCLCheckPointer {
	ck := new(KCLCheckPointer)
	ck.handler = handler
	return ck
}
