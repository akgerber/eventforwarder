package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

const (
	eventSrcPort   = ":9090"
	userClientPort = ":9099"
	bufferSize     = 0
)

var (
	done  chan bool = make(chan bool)
	debug           = false
)

//Main server function:
//-Accept a single event source connection
//-Accept many user client connections afterward
func main() {
	//Handle a single event source connection
	eventSrc, err := net.Listen("tcp", eventSrcPort)
	if err != nil {
		log.Fatal(err)
	}
	eventsConn, err := eventSrc.Accept()
	if err != nil {
		log.Println(err)
	}
	go handleEventStream(eventsConn)

	//Handle client connections
	userClients, err := net.Listen("tcp", userClientPort)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case <-done:
			break
		default:
			clientConn, err := userClients.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			go handleClient(clientConn)
		}
	}
}

//Map of client IDs to ProtocolEvent channels
var userChans = struct {
	sync.RWMutex
	m map[int](chan ProtocolEvent)
}{m: map[int](chan ProtocolEvent){}}

//Get a channel from the channel map (threadsafe)
func getUserChannel(user int) (userChan chan ProtocolEvent) {
	userChans.RLock()
	userChan = userChans.m[user]
	userChans.RUnlock()
	return
}

//If the user-channel hasn't been created yet, create it (threadsafe)
func makeUserChannel(user int) (userChan chan ProtocolEvent) {
	userChan = getUserChannel(user)

	if userChan == nil {
		userChans.Lock()
		log.Printf("Creating channel for client %d", user)
		if userChans.m[user] == nil { //make sure another thread didn't initialize
			userChans.m[user] = make(chan ProtocolEvent, bufferSize)
		}
		userChan = userChans.m[user]
		userChans.Unlock()
	}
	return
}

//Handle a stream of events from a connection-- must be single-threaded to order events
func handleEventStream(c net.Conn) {
	var (
		err             error
		eventPayload    string
		lastSequenceNum int       = 0
		orderedEvents   EventHeap = make(EventHeap, 0)
	)
	eventReader := bufio.NewReader(c)

	heap.Init(&orderedEvents)

	for {
		if eventPayload, err = eventReader.ReadString('\n'); err != nil {
			if err == io.EOF {
				log.Printf("Event stream complete. Exiting.")
				for _, userId := range getAllClients() {
					close(getUserChannel(userId))
				}
				done <- true
				break
			} else {
				log.Fatal(err)
			}
		}
		if event, err := parseEventPayload(strings.TrimSpace(eventPayload)); err != nil {
			log.Printf("Error parsing event: %s", err)
		} else {
			heap.Push(&(orderedEvents), &event)
			log.Printf("Received event: %d; Previously processed event: %d", event.sequenceNum, lastSequenceNum)

			for len(orderedEvents) > 0 &&
				orderedEvents[0].sequenceNum == lastSequenceNum+1 {
				event = *(heap.Pop(&orderedEvents).(*ProtocolEvent))
				log.Printf("Processing event: %d", event.sequenceNum)
				handleEvent(event)
				lastSequenceNum++
			}
		}
	}
	log.Printf("Event handler exiting")
}

//Handle an event as specified-- not threadsafe
func handleEvent(event ProtocolEvent) {
	log.Println(event.payload)

	switch event.eventType {
	case PrivateMsg:
		notifyUser(event.toUserId, event)
	case Follow:
		FollowUser(event.fromUserId, event.toUserId)
		notifyUser(event.toUserId, event)
	case Unfollow:
		UnfollowUser(event.fromUserId, event.toUserId)
	case StatusUpdate:
		notifyMany(GetFollowers(event.fromUserId), event)
	case Broadcast:
		notifyMany(getAllClients(), event)
	default:
		log.Println("unable to handle" + event.payload)
	}
}

//Return a slice of all connected client IDs
func getAllClients() []int {
	userChans.RLock()
	clients := make([]int, 0, len(userChans.m))
	for user, _ := range userChans.m {
		clients = append(clients, user)
	}
	userChans.RUnlock()
	log.Printf("Currently connected clients: %#v", clients)
	return clients
}

//Send a message to all client channels specified in users
func notifyMany(users []int, event ProtocolEvent) {
	for _, user := range users {
		notifyUser(user, event)
	}
}

//Send a message to a client channel, or silently ignore it if client doesn't exist
func notifyUser(userId int, event ProtocolEvent) {
	toChan := getUserChannel(userId)
	if toChan != nil {
		log.Printf("Notifying user %d of event %#v", userId, event.payload)
		toChan <- event
	} else {
		log.Printf("No client for user %d; ignoring  %#v", userId, event.payload)
	}
}

//Accept a client connection and receive its UserID
func getClientUser(c net.Conn) (int, error) {
	var (
		clientId int
		line     string
		err      error
	)
	events := bufio.NewReader(c)
	if line, err = events.ReadString('\n'); err != nil {
		return 0, err
	}
	if clientId, err = strconv.Atoi(strings.TrimSpace(line)); err != nil {
		return 0, fmt.Errorf("Channel ID is non-int:  %s", line)
	} else {
		return clientId, nil
	}
}

//Accept connections from a client, create their channels, and pass on notifications
func handleClient(c net.Conn) {
	var (
		clientId int
		err      error
	)

	if clientId, err = getClientUser(c); err != nil {
		log.Fatal(err)
	}
	events := makeUserChannel(clientId)
	eventWriter := bufio.NewWriter(c)
	log.Printf("Client %d waiting for notifications", clientId)

	for event := range events {
		//wait for events on channel and write them to connection
		log.Printf("Sending %s to client %d", event.payload, clientId)
		_, err := eventWriter.WriteString(event.ToWire())
		if err != nil {
			log.Fatal(err)
		}
		eventWriter.Flush()
	}
	log.Printf("Client %d exiting", clientId)
}
