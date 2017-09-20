package kclgo

import (
	"fmt"
	"math/big"
	"time"
)

var _ RecordProcessor = (*DefaultRecordProcessor)(nil)

type DefaultRecordProcessor struct {
	handler            *IoHandler
	checkpointer       CheckPointer
	config             *KCLConfig
	largestSeq         *big.Int
	largestSubSeq      int
	lastCheckpointTime time.Time
	processingFunc     RecordProcessingFunc
}

func (k *DefaultRecordProcessor) Initialize(input *InitializeInput) error {
	k.config.OutLogger.Printf("Processing shard %v\n", input.ShardID)
	k.largestSeq = &big.Int{}
	k.lastCheckpointTime = time.Now()

	return nil
}

func (k *DefaultRecordProcessor) shouldUpdateSequence(seq *big.Int, subSeq int) bool {
	zero := &big.Int{}
	if seq.Cmp(k.largestSeq) > 0 || (k.largestSeq.Cmp(zero) == 0 && k.largestSubSeq == 0) {
		return true
	}
	if subSeq > k.largestSubSeq {
		return true
	}
	return false
}

func (k *DefaultRecordProcessor) ProcessRecords(input *ProcessRecordsInput) error {
	k.config.OutLogger.Printf("Processing (%v) Records (%v) milliseconds behind latest", len(input.Records), input.MillisBehindLatest)

	var retErr error
	for _, r := range input.Records {
		seq := new(big.Int)
		if _, worked := seq.SetString(r.SequenceNumber, 10); !worked {
			return fmt.Errorf("could not parse Sequence Number (%s) into big.Int", r.SequenceNumber)
		}

		if err := k.processingFunc.ProcessRecord(r); err != nil {
			retErr = err
			break
		}
		if k.shouldUpdateSequence(seq, r.SubSequenceNumber) {
			k.largestSeq = seq
			k.largestSubSeq = r.SubSequenceNumber
		}
	}

	if retErr == nil && !time.Now().Before(
		k.lastCheckpointTime.Add(
			time.Duration(int64(k.config.CheckPointFreqSeconds))*time.Second)) {
		k.checkpointer.CheckPoint(k.largestSeq.String(), k.largestSubSeq)
	}

	return retErr
}

func (k *DefaultRecordProcessor) CheckPoint(sequenceNumber string, subSequenceNumber int) error {
	for i := 0; i < k.config.CheckPointRetries; i++ {
		if err := k.checkpointer.CheckPoint(sequenceNumber, subSequenceNumber); err != nil {
			switch err.Error() {
			case "ShutdownException":
				k.config.OutLogger.Println("Encountered Shutdown Exception, skipping checkpoint")
				return err
			case "ThrottlingException":
				if k.config.CheckPointRetries < i {
					k.config.OutLogger.Printf("Was throttled while checkpointing, will attempt again in %v seconds\n", k.config.CheckPointFreqSeconds)
				} else {
					k.config.ErrLogger.Printf("Failed to checkpoint after %v tries, giving up\n", i)
					return err
				}
			case "InvalidStateException":
				k.config.ErrLogger.Printf("Received Invalid State exception, client code should exit now\n")
			default:
				k.config.ErrLogger.Printf("Received error: (%s) when trying to checkpoint\n", err.Error())
			}
			time.Sleep(time.Duration(int64(k.config.CheckPointFreqSeconds)) * time.Second)
		} else {
			return nil
		}
	}
	return nil
}

func (k *DefaultRecordProcessor) Shutdown(input *ShutdownInput) error {
	switch input.Reason {
	case ZOMBIE:
		// don't checkpoint, just cleanup and leave
		k.config.OutLogger.Println("Shutting down due to failover. Will not checkpoint.")
		return k.handler.Cleanup()
	case TERMINATE:
		k.config.OutLogger.Println("Was told to terminate, will attempt to checkpoint.")
		k.checkpointer.CheckPoint("", 0)
		return k.handler.Cleanup()
	default:
		k.config.ErrLogger.Println("Unknown shutdown reason, will terminate without checkpointing")
		return k.handler.Cleanup()
	}
}
func (k *DefaultRecordProcessor) ShutdownRequested(input *ShutdownRequestedInput) error {
	k.config.OutLogger.Println("Was told to gracefully shutdown, will attempt to checkpoint.")
	k.checkpointer.CheckPoint("", 0)
	return k.handler.Cleanup()
}

func NewDefaultRecordProcessor(config *KCLConfig, handler *IoHandler, checkpointer CheckPointer, processingFunc RecordProcessingFunc) *DefaultRecordProcessor {
	processor := new(DefaultRecordProcessor)
	processor.config = config
	processor.handler = handler
	processor.checkpointer = checkpointer
	processor.processingFunc = processingFunc
	return processor
}
