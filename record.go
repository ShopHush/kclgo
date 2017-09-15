package kclgo

import (
	"encoding/base64"
	"time"
)

type Record struct {
	Data                        string `json:"data"`
	PartitionKey                string `json:"partitionKey"`
	SequenceNumber              string `json:"SequenceNumber"`
	ApproximateArrivalTimestamp int    `json:"approximateArrivalTimestamp"`
	SubSequenceNumber           int    `json:"SubSequenceNumber"`
	Action                      string `json:"Action"`
}

// Return the raw data from the Kinesis Record
func (r *Record) BinaryData() ([]byte, error) {
	return base64.StdEncoding.DecodeString(r.Data)
}

// return Time parsed from Kinesis timestamp
func (r *Record) ApproximateArrivalTime() time.Time {
	return time.Unix(int64(r.ApproximateArrivalTimestamp), 0)
}


