package main

import (
	"fmt"
	"testing"
)

func TestParseEventPayload(t *testing.T) {
	goodPayloads := []string{
		"666|F|60|50",
		"1|U|12|9",
		"542532|B",
		"634|S|32",
	}
	expectedEvents := []ProtocolEvent{
		{payload: "666|F|60|50", eventType: Follow, sequenceNum: 666, fromUserId: 60, toUserId: 50},
		{payload: "1|U|12|9", eventType: Unfollow, sequenceNum: 1, fromUserId: 12, toUserId: 9},
		{payload: "542532|B", eventType: Broadcast, sequenceNum: 542532},
		{payload: "634|S|32", eventType: StatusUpdate, sequenceNum: 634, fromUserId: 32},
	}
	badPayloads := []string{
		"hey man",
		"1|U|3|4|5",
		"1|B|3|4",
		"1|S|3|4",
		"1|L|3|4",
		"1|2|3|4|5",
		"a|b|c|d|e",
		"||||||||||||||",
	}
	for i := 0; i < len(goodPayloads); i++ {
		event, err := parseEventPayload(goodPayloads[i])
		if err != nil {
			t.Errorf("Error induced by valid string %s; ProtocolEvent is %+v",
				goodPayloads[i], event)
		} else {
			fmt.Printf("Input string %s produced ProtocolEvent%+v\n",
				goodPayloads[i], event)
		}
		if expectedEvents[i] != event {
			t.Errorf("For string %s, ProtocolEvent\n%+v\n is not expected ProtocolEvent\n%+v",
				goodPayloads[i], event, expectedEvents[i])
		}
	}
	for i := 0; i < len(badPayloads); i++ {
		event, err := parseEventPayload(badPayloads[i])
		if err == nil {
			t.Errorf("No error induced by invalid string %s; ProtocolEvent is %+v",
				badPayloads[i], event)
		} else {
			fmt.Printf("Input string %s produced error %s\n",
				badPayloads[i], err)
		}
	}
}
