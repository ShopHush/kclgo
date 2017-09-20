package kclgo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type IoHandler struct {
	config     *KCLConfig
	inputFile  *os.File
	outputFile *os.File
	errorFile  *os.File
	reader     *bufio.Reader
	mux        sync.Mutex
}

func (i *IoHandler) Init() (err error) {
	if i.config.InputFileName == "" {
		i.inputFile = os.Stdin
	} else {
		if i.inputFile, err = os.Open(i.config.InputFileName); err != nil {
			return
		}
	}
	i.reader = bufio.NewReader(i.inputFile)

	if i.config.OutputFileName == "" {
		i.outputFile = os.Stdout
	} else {
		if i.outputFile, err = os.Open(i.config.OutputFileName); err != nil {
			return
		}
	}
	if i.config.ErrorFileName == "" {
		i.errorFile = os.Stderr
	} else {
		if i.errorFile, err = os.Open(i.config.ErrorFileName); err != nil {
			return
		}
	}
	return
}

func (i *IoHandler) Cleanup() (err error) {
	errors := make([]error, 3)
	errors[0] = i.inputFile.Close()
	errors[1] = i.outputFile.Close()
	errors[2] = i.outputFile.Close()

	for _, e := range errors {
		if e != nil {
			err = e
		}
		break
	}
	return
}

// Writes a line to the output file. The line is preceeded and followed by a new line because other libraries
// could be writing to the output file as well (e.g. some libs might write debugging info to STDOUT) so we would
// like to prevent our lines from being interlaced with other messages so the MultiLangDaemon can understand them.
func (i *IoHandler) WriteLine(line string) (err error) {
	i.mux.Lock()
	defer i.mux.Unlock()
	_, err = i.outputFile.WriteString(fmt.Sprintf("\n%s\n", line))
	if err != nil {
		return
	}

	err = i.outputFile.Sync()
	return
}

// Write a line to the Error file.
func (i *IoHandler) WriteError(line string) (err error) {
	i.mux.Lock()
	defer i.mux.Unlock()
	_, err = i.errorFile.WriteString(fmt.Sprintf("%s\n", line))
	if err != nil {
		return
	}

	err = i.errorFile.Sync()
	return
}

// Reads a line from the input file.
// A single line read from the input_file (e.g. '{"Action" : "initialize", "shardId" : "shardId-000001"}')
// KCL on the java side sends a single (could be huge) message and waits for a response
func (i *IoHandler) ReadLine() (string, error) {
	s, err := i.reader.ReadString('\n')
	// soak up the EOF errors, those don't need to be returned
	if err == io.EOF {
		return s, nil
	}
	return s, err
}

//Decodes a message from the MultiLangDaemon.
// line: A message line that was delivered received from the MultiLangDaemon (e.g.
// '{"Action" : "initialize", "shardId" : "shardId-000001"}')
// returns an action that can be called for the line
func (i *IoHandler) LoadAction(line *string) (ActionInterface, error) {
	return decodeMessage(line)
}

// KCL expects an ack for every message it sends
// {'action' : status', 'responseFor' : '<action_name>'}
func (i *IoHandler) WriteActionResponse(response ActionResponse) error {
	resp, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return i.WriteLine(string(resp))
}

func (i *IoHandler) WriteCheckPointRequest(req CheckPointRequest) error {
	resp, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return i.WriteLine(string(resp))
}

func NewIOHandler(config *KCLConfig) *IoHandler {
	h := new(IoHandler)
	h.config = config
	return h
}
