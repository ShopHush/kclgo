package kclgo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type ioHandler struct {
	config KCLConfig
	inputFile      *os.File
	outputFile     *os.File
	errorFile      *os.File
	reader         *bufio.Reader
	mux            sync.Mutex
}

func (i *ioHandler) Init(config KCLConfig) (err error) {
	i.config = config
	if config.InputFileName == "" {
		i.inputFile = os.Stdin
	} else {
		if i.inputFile, err = os.Open(config.InputFileName); err != nil {
			return
		}
	}
	i.reader = bufio.NewReader(i.inputFile)

	if config.OutputFileName == "" {
		i.outputFile = os.Stdout
	} else {
		if i.outputFile, err = os.Open(config.OutputFileName); err != nil {
			return
		}
	}
	if config.ErrorFileName == "" {
		i.errorFile = os.Stderr
	} else {
		if i.errorFile, err = os.Open(config.ErrorFileName); err != nil {
			return
		}
	}
	return
}

func (i *ioHandler) Cleanup() (err error) {
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

//Writes a line to the output file. The line is preceeded and followed by a new line because other libraries
//could be writing to the output file as well (e.g. some libs might write debugging info to STDOUT) so we would
//like to prevent our lines from being interlaced with other messages so the MultiLangDaemon can understand them.
//:type l: str
//:param l: A line to write (e.g. '{"Action" : "status", "responseFor" : "<someAction>"}')
//'''
func (i *ioHandler) WriteLine(line string) (err error) {
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
func (i *ioHandler) WriteError(line string) (err error) {
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
func (i *ioHandler) ReadLine() (string, error) {
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
func (i *ioHandler) LoadAction(line *string) (ActionInterface, error) {
	return decodeMessage(line)
}

// {'action' : status', 'responseFor' : 'initialize'}
func (i *ioHandler) WriteActionResponse(response ActionResponse) error {
	resp, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return i.WriteLine(string(resp))
}

func (i *ioHandler) WriteCheckPointRequest(req CheckPointRequest) error {
	resp, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return i.WriteLine(string(resp))
}

func newIOHandler(config KCLConfig) (*ioHandler, error) {
	h := new(ioHandler)
	err := h.Init(config)
	return h, err
}
