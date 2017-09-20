package kclgo

import "errors"

var _ ActionInterface = (*InitializeInput)(nil)
var _ ActionInterface = (*ProcessRecordsInput)(nil)
var _ ActionInterface = (*ShutdownInput)(nil)
var _ ActionInterface = (*ShutdownRequestedInput)(nil)
var _ ActionInterface = (*checkPointResponse)(nil)

type InitializeInput struct {
	Action            string  `json:"action"`
	ShardID           string  `json:"shardId"`
	SequenceNumber    *string `json:"sequenceNumber"`
	SubSequenceNumber int     `json:"subSequenceNumber"`
}

//{"action":"initialize","shardId":"shardId-000000000000","sequenceNumber":"TRIM_HORIZON","subSequenceNumber":0}
func (i *InitializeInput) Perform(processor RecordProcessor) error {
	return processor.Initialize(i)
}
func (i *InitializeInput) GetAction() string {
	return i.Action
}

// Process Records Input
type ProcessRecordsInput struct {
	Action             string   `json:"action"`
	MillisBehindLatest int      `json:"millisBehindLatest"`
	Records            []Record `json:"records"`
}

func (p *ProcessRecordsInput) Perform(processor RecordProcessor) error {
	return processor.ProcessRecords(p)
}
func (p *ProcessRecordsInput) GetAction() string {
	return p.Action
}

const (
	ZOMBIE    = "ZOMBIE"
	TERMINATE = "TERMINATE"
)

// Shutdown Input comes from the KCL to tell us to shutdown.
// There are two reasons for this, ZOMBIE and TERMINATE.
type ShutdownInput struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func (s *ShutdownInput) Perform(processor RecordProcessor) error {
	return processor.Shutdown(s)
}
func (s *ShutdownInput) GetAction() string {
	return s.Action
}

// Shutdown Requested Input is a graceful shutdown request from the KCL
type ShutdownRequestedInput struct {
	Action string `json:"action"`
}

func (s *ShutdownRequestedInput) Perform(processor RecordProcessor) error {
	return processor.ShutdownRequested(s)
}
func (s *ShutdownRequestedInput) GetAction() string {
	return s.Action
}

// CheckPoint messages are different, they get sent upon call to checkpoint()
type CheckPointRequest struct {
	Action            string  `json:"action"`            // checkpoint
	SequenceNumber    *string `json:"sequenceNumber"`    // can be none
	SubSequenceNumber int     `json:"subSequenceNumber"` // always a number, default to 0
}

// CheckPoint response from the server upon being instructed to checkpoint
type checkPointResponse struct {
	Action            string  `json:"action"`
	Error             *string `json:"error"`
	SequenceNumber    *string `json:"sequenceNumber"`
	SubSequenceNumber *int    `json:"subSequenceNumber"`
}

func (c *checkPointResponse) Perform(processor RecordProcessor) error {
	if c.Error != nil {
		return CheckPointError(errors.New(*c.Error))
	}
	return nil
}
func (c *checkPointResponse) GetAction() string {
	return c.Action
}

type ActionResponse struct {
	Action      string `json:"action"`
	ResponseFor string `json:"responseFor"`
}

func getActionResponse(action string) ActionResponse {
	return ActionResponse{
		Action:      "status",
		ResponseFor: action,
	}
}
