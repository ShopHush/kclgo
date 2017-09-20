package kclgo

import (
	"encoding/json"
	"fmt"
)

// I don't want to json parse the whole message twice (could be a very long line and very slow)
// so I'm counting on the java code to put the action as the first token (as is the case in the code)
// so I'm just slurping in up to the first comma and returning the action
func slurpToFirstComma(message *string) (string, error) {
	var i int
	for index, runeValue := range *message {
		if runeValue == ',' {
			i = index
			break
		}
	}
	t := make([]byte, i, i)
	// copy stops early upon reaching the capacity of src or dest.
	copy(t, *message)
	t = append(t, '}')

	act := struct {
		Action string `json:"action"`
	}{}
	if err := json.Unmarshal(t, &act); err != nil {
		return "", MalformedAction(err)
	}
	return act.Action, nil
}

func decodeMessage(message *string) (ActionInterface, error) {
	action, err := slurpToFirstComma(message)
	if err != nil {
		return nil, err
	}

	actions := make(map[string]ActionInterface)
	actions["initialize"] = &InitializeInput{}
	actions["processRecords"] = &ProcessRecordsInput{}
	actions["shutdown"] = &ShutdownInput{}
	actions["checkpoint"] = &checkPointResponse{}
	actions["shutdownRequested"] = &ShutdownRequestedInput{}

	msgInput, there := actions[action]
	if !there {
		return nil, MalformedAction(fmt.Errorf("Action (%s) not mapped in kclgo ", action))
	}

	if err := json.Unmarshal([]byte(*message), msgInput); err != nil {
		return msgInput, err
	}
	return msgInput, nil
}
