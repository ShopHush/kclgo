package kclgo

import (
	"fmt"
	"log"
	"time"
	"strconv"
)

type KCLRecordProcessor struct {
	handler            *ioHandler
	cp                 *KCLCheckPointer
	config             KCLConfig
	largestSeq         int
	largestSubSeq      int
	lastCheckpointTime time.Time
}


func (k *KCLRecordProcessor) Initialize(input *InitializeInput) error {
	log.Printf("Processing shard %v\n", input.ShardID)
	k.lastCheckpointTime = time.Now()

	return nil
}

func (k *KCLRecordProcessor) shouldUpdateSequence(seq, subSeq int) bool {
	if seq > k.largestSeq || (k.largestSeq == 0 && k.largestSubSeq == 0) {
		return true
	}
	if subSeq > k.largestSubSeq {
		return true
	}
	return false
}

func (k *KCLRecordProcessor) ProcessRecords(input *ProcessRecordsInput) error {
	log.Printf("Processing (%v) Records\t(%v) milliseconds behind latest", len(input.Records), input.MillisBehindLatest)

	var retErr error
	for _, r := range input.Records {
		seq, err := strconv.Atoi(r.SequenceNumber)
		if err != nil {
			retErr = err
			break
		}
		if err := k.ProcessRecord(r); err != nil {
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
			time.Duration(int64(k.config.CheckPointFreqSeconds)) * time.Second)) {
		k.cp.CheckPoint(strconv.Itoa(k.largestSeq), k.largestSubSeq)
	}

	return retErr
}

func (k *KCLRecordProcessor) ProcessRecord(record Record) error {
	data, err := record.BinaryData()
	if err != nil {
		return err
	}
	fmt.Printf("%s", string(data))
	return nil
}

func (k *KCLRecordProcessor) CheckPoint(sequenceNumber string, subSequenceNumber int) error {
	for i := 0; i < k.config.CheckPointRetries; i++ {
		if err := k.cp.CheckPoint(sequenceNumber, subSequenceNumber); err != nil {
			switch err.Error() {
			case "ShutdownException":
				log.Println("Encountered Shutdown Exception, skipping checkpoint")
				return err
			case "ThrottlingException":
				if k.config.CheckPointRetries < i {
					log.Printf("Was throttled while checkpointing, will attempt again in %v seconds\n", k.config.CheckPointFreqSeconds)
				} else {
					log.Printf("Failed to checkpoint after %v tries, giving up\n", i)
					return err
				}
			case "InvalidStateException":
				log.Fatalf("Received Invalid State exception, bailing out\n")
			default:
				log.Printf("Received error: (%s) when trying to checkpoint\n", err.Error())
			}
			time.Sleep(time.Duration(int64(k.config.CheckPointFreqSeconds)) * time.Second)
		} else {
			return nil
		}
	}
	return nil
}

func (k *KCLRecordProcessor) Shutdown(input *ShutdownInput) error {
	switch input.Reason {
	case ZOMBIE:
		// don't checkpoint, just cleanup and leave
		log.Println("Shutting down due to failover. Will not checkpoint.")
		return k.handler.Cleanup()
	case TERMINATE:
		log.Println("Was told to terminate, will attempt to checkpoint.")
		k.cp.CheckPoint("", 0)
		return k.handler.Cleanup()
	default:
		log.Println("Unknown shutdown reason, will terminate without checkpointing")
		return k.handler.Cleanup()
	}
}
func (k *KCLRecordProcessor) ShutdownRequested(input *ShutdownRequestedInput) error {
	log.Println("Was told to gracefully shutdown, will attempt to checkpoint.")
	k.cp.CheckPoint("", 0)
	return k.handler.Cleanup()
}

func NewKCLRecordProcessor(config KCLConfig) (*KCLRecordProcessor, error) {
	processor := new(KCLRecordProcessor)
	processor.config = config
	h, err := newIOHandler(config)
	if err != nil {
		return processor, err
	}
	processor.handler = h
	processor.cp = NewCheckPointer(processor.handler)
	return processor, nil
}
