package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type EventType int

const (
	Follow EventType = iota
	Unfollow
	Broadcast
	PrivateMsg
	StatusUpdate
	Search
)

type ProtocolEvent struct {
	payload     string
	sequenceNum int
	eventType   EventType
	fromUserId  int
	toUserId    int
	toNotify    []int
}

type iEvent interface {
	mutate(s *Service)
	notify(s *Service)
}

func (event *ProtocolEvent) String() string {
	return event.payload
}

func (event *ProtocolEvent) ToWire() string {
	return event.payload + "\r\n"
}

//An EventHeap is a minheap of pointers to protocol events
type EventHeap []*ProtocolEvent

//Methods implementing container/heap interface
func (h EventHeap) Len() int           { return len(h) }
func (h EventHeap) Less(i, j int) bool { return h[i].sequenceNum < h[j].sequenceNum }
func (h EventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *EventHeap) Push(x interface{}) {
	*h = append(*h, x.(*ProtocolEvent))
}

func (h *EventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

//Handle an event as specified-- not threadsafe
func (event *ProtocolEvent) mutate(s *Service) {
	switch event.eventType {
	case PrivateMsg:
		event.toNotify = []int{event.toUserId}
	case Follow:
		FollowUser(s, event.fromUserId, event.toUserId)
		event.toNotify = []int{event.toUserId}
	case Unfollow:
		UnfollowUser(s, event.fromUserId, event.toUserId)
		event.toNotify = []int{}
	case StatusUpdate:
		event.toNotify = GetFollowers(s, event.fromUserId)
	case Broadcast:
		event.toNotify = GetAllClients(s)
	default:
		log.Fatalf("unable to handle" + event.payload)
	}
}

//Send a message to all client channels specified in users
func (event *ProtocolEvent) notify(s *Service) {
	for _, userId := range event.toNotify {
		toChan := getUserChannel(s, userId)
		if toChan != nil {
			toChan <- *event
		}
	}
}

//Parse an event payload into a struct, or return an error if
//it is not an event of the specified format:
//'payload|sequence_num|type|from_user_id|to_user_id'
func parseEventPayload(payload string) (ProtocolEvent, error) {
	const (
		divider             = "|"
		aboveMaxPayloadSize = 5
		minPayloadSize      = 2
	)
	var (
		expectedPayloadSize int
		event               ProtocolEvent
	)

	event.payload = payload

	//split payload into max 5 components in case message body is hostilely long
	splitPayload := strings.SplitN(payload, divider, aboveMaxPayloadSize)
	if len(splitPayload) < minPayloadSize {
		return event, fmt.Errorf("Not enough fields in payload %s", payload)
	}
	if sequenceNum, err := strconv.Atoi(splitPayload[0]); err != nil {
		return event, fmt.Errorf("Sequence number is non-int in payload  %s", payload)
	} else {
		event.sequenceNum = sequenceNum
	}

	//parse type of event (second payload field)
	switch splitPayload[1] {
	case "F":
		event.eventType = Follow
		expectedPayloadSize = 4
	case "U":
		event.eventType = Unfollow
		expectedPayloadSize = 4
	case "P":
		event.eventType = PrivateMsg
		expectedPayloadSize = 4
	case "S":
		event.eventType = StatusUpdate
		expectedPayloadSize = 3
	case "B":
		event.eventType = Broadcast
		expectedPayloadSize = 2
	case "H":
		event.eventType = Search
		expectedPayloadSize = 4
	default:
		return event, fmt.Errorf("User event type invalid in payload %s", payload)
	}
	if len(splitPayload) != expectedPayloadSize {
		return event, fmt.Errorf("Invalid number of fields in payload %s", payload)
	}
	switch event.eventType {
	case Follow, Unfollow, PrivateMsg, Search:
		if toUserId, err := strconv.Atoi(splitPayload[3]); err != nil {
			return event, fmt.Errorf("To User ID is non-int in payload  %s", payload)
		} else {
			event.toUserId = toUserId
		}
		fallthrough
	case StatusUpdate:
		if fromUserId, err := strconv.Atoi(splitPayload[2]); err != nil {
			return event, fmt.Errorf("From User ID is non-int in payload  %s", payload)
		} else {
			event.fromUserId = fromUserId
		}
	default:
	}
	return event, nil
}
