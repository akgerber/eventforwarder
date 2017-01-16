package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	host           = "localhost"
	eventSrcPort   = ":9090"
	userClientPort = ":9099"
	bufferSize     = 0
)

var (
	debug = false
)

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

//Notify other goroutines and exit
func finish(eventsDone chan bool) {
	for _, userId := range getAllClients() {
		close(getUserChannel(userId))
	}
	eventsDone <- true
	runtime.Goexit()
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

//Return a slice of all connected client IDs
func getAllClients() []int {
	userChans.RLock()
	clients := make([]int, 0, len(userChans.m))
	for user, _ := range userChans.m {
		clients = append(clients, user)
	}
	userChans.RUnlock()
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
		toChan <- event
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

//Handle a stream of events from a connection-- must be single-threaded to order events
func handleEventStream(c net.Conn, eventsDone chan bool) {
	defer c.Close()
	var (
		err             error
		eventPayload    string
		event           ProtocolEvent
		lastSequenceNum int       = 0
		eventBuffer     EventHeap = make(EventHeap, 0)
	)
	eventReader := bufio.NewReader(c)

	heap.Init(&eventBuffer)

	for {
		if eventPayload, err = eventReader.ReadString('\n'); err != nil {
			if err == io.EOF {
				finish(eventsDone)
			} else {
				log.Fatal(err)
			}
		}
		if event, err = parseEventPayload(strings.TrimSpace(eventPayload)); err != nil {
			log.Fatalf("Error parsing event: %s", err)
		}

		//process events so long as the next ones in sequenceNum order are available
		heap.Push(&(eventBuffer), &event)
		for len(eventBuffer) > 0 &&
			eventBuffer[0].sequenceNum == lastSequenceNum+1 {
			event = *(heap.Pop(&eventBuffer).(*ProtocolEvent))
			handleEvent(event)
			lastSequenceNum++
		}
	}
}

//Accept connections from a client, create their channels, and pass on notifications
func handleClient(c net.Conn) {
	defer c.Close()
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

	//wait for events until channel closes and write them to connection
	for event := range events {
		log.Printf("Sending %s to client %d", event.payload, clientId)
		_, err := eventWriter.WriteString(event.ToWire())
		if err != nil {
			log.Fatal(err)
		}
		eventWriter.Flush()
	}
	runtime.Goexit()
}

//Main server function:
//-Accept a single event source connection
//-Accept many user client connections afterward
func main() {
	var (
		eventSrc, userClients  *net.TCPListener
		eventsConn, clientConn *net.TCPConn
		laddr                  *net.TCPAddr
		eventsDone             chan bool = make(chan bool)
		err                    error
	)
	defer eventSrc.Close()
	defer userClients.Close()
	defer os.Exit(0)

	//Handle a single event source connection
	if laddr, err = net.ResolveTCPAddr("tcp", host+eventSrcPort); err != nil {
		log.Fatal(err)
	}
	if eventSrc, err = net.ListenTCP("tcp", laddr); err != nil {
		log.Fatal(err)
	}
	if eventsConn, err = eventSrc.AcceptTCP(); err != nil {
		log.Fatal(err)
	}
	go handleEventStream(eventsConn, eventsDone)

	//Handle client connections
	if laddr, err = net.ResolveTCPAddr("tcp", host+userClientPort); err != nil {
		log.Fatal(err)
	}
	if userClients, err = net.ListenTCP("tcp", laddr); err != nil {
		log.Fatal(err)
	}
	userClients.SetDeadline(time.Now().Add(time.Second))
	for {
		select {
		case <-eventsDone:
			runtime.Goexit()
		default:
			if clientConn, err = userClients.AcceptTCP(); err != nil {
				continue
			}
			go handleClient(clientConn)
		}
	}
}
