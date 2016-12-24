package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type ProtocolEvent struct {
	payload     string
	sequenceNum int
	eventType   EventType
	fromUserId  int
	toUserId    int
}

type EventType int

const (
	Follow EventType = iota
	Unfollow
	Broadcast
	PrivateMsg
	StatusUpdate
)

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
	default:
		return event, fmt.Errorf("User event type invalid in payload %s", payload)
	}
	if len(splitPayload) != expectedPayloadSize {
		return event, fmt.Errorf("Invalid number of fields in payload %s", payload)
	}
	switch event.eventType {
	case Follow, Unfollow, PrivateMsg:
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

func main() {
	log.SetOutput(os.Stdout)

	eventSrc, err := net.Listen("tcp", ":9090")
	userClients, err := net.Listen("tcp", ":9099")
	if err != nil {
		log.Fatal(err)
	}
	for {
		eventsConn, err := eventSrc.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleEvents(eventsConn)

		clientConn, err := userClients.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleClient(clientConn)
	}
}

func handleEvents(c net.Conn) {
	buf := make([]byte, 4096)

	for {
		n, err := c.Read(buf)
		if err != nil || n == 0 {
			c.Close()
			break
		}
		log.Printf("%s", buf[0:n])
		//log.Printf("Connection from %v closed.", c.RemoteAddr())
	}
}

func handleClient(c net.Conn) {
	for {
		_, err := c.Write([]byte("TODO hey man"))
		if err != nil {
			c.Close()
			break
		}
		//log.Printf("Connection from %v closed.", c.RemoteAddr())
	}
}
