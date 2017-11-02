package main

import (
	"container/heap"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"testing"
)

func TestParseEventPayload(t *testing.T) {
	goodPayloads := []string{
		"666|F|60|50",
		"1|U|12|9",
		"542532|B",
		"634|S|32",
		"64|P|32|59",
		"67|H|32|5",
	}
	expectedEvents := []ProtocolEvent{
		{payload: "666|F|60|50", eventType: Follow, sequenceNum: 666, fromUserId: 60, toUserId: 50},
		{payload: "1|U|12|9", eventType: Unfollow, sequenceNum: 1, fromUserId: 12, toUserId: 9},
		{payload: "542532|B", eventType: Broadcast, sequenceNum: 542532},
		{payload: "634|S|32", eventType: StatusUpdate, sequenceNum: 634, fromUserId: 32},
		{payload: "64|P|32|59", eventType: PrivateMsg, sequenceNum: 64, fromUserId: 32, toUserId: 59},
		{payload: "67|H|32|5", eventType: Search, sequenceNum: 67, fromUserId: 32, toUserId: 5},
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

func TestEventHeap(t *testing.T) {
	goodPayloads := []string{
		"666|F|60|50",
		"1|U|12|9",
		"542532|B",
		"634|S|32",
		"64|P|32|59",
		"2|S|32",
		"3|S|32",
	}
	h := make(EventHeap, 0)

	//Place the events into the EventHeap
	heap.Init(&h)
	for i := 0; i < len(goodPayloads); i++ {
		event, err := parseEventPayload(goodPayloads[i])
		if err != nil {
			t.Errorf("Error induced by valid string %s; ProtocolEvent is %+v",
				goodPayloads[i], event)
		} else {
			fmt.Printf("Input string %s produced ProtocolEvent%+v\n",
				goodPayloads[i], event)
			heap.Push(&h, &event)
		}
	}

	//Verify events popped from heap are in ascending order
	sorted := make([]*ProtocolEvent, 0)
	for h.Len() > 0 {
		min := heap.Pop(&h).(*ProtocolEvent)
		sorted = append(sorted, min)
	}
	for i := 0; i < len(sorted)-1; i++ {
		if sorted[i].sequenceNum > sorted[i+1].sequenceNum {
			t.Errorf("Expected EventHeap to pop events in order of ascending sequenceNum but instead contained:\n")
			spew.Dump(sorted)
		}
	}
}
