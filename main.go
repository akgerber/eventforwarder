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

//Get a channel from the channel map (threadsafe)
func getUserChannel(s Service, user int) (userChan chan ProtocolEvent) {
	s.userChanLock.RLock()
	userChan = s.userChans[user]
	s.userChanLock.RUnlock()
	return
}

//Notify other goroutines and exit
func finishEvents(s Service) {
	log.Printf("Exiting-- EOF received from event source")
	for _, userId := range getAllClients(s) {
		close(getUserChannel(s, userId))
	}
	s.eventsDone <- true
	runtime.Goexit()
}

//If the user-channel hasn't been created yet, create it (threadsafe)
func makeUserChannel(s Service, user int) (userChan chan ProtocolEvent) {
	const bufferSize = 0
	userChan = getUserChannel(s, user)

	if userChan == nil {
		s.userChanLock.Lock()
		if s.userChans[user] == nil { //make sure another thread didn't initialize
			s.userChans[user] = make(chan ProtocolEvent, bufferSize)
		}
		userChan = s.userChans[user]
		s.userChanLock.Unlock()
	}
	return
}

//Return a slice of all connected client IDs
func getAllClients(s Service) []int {
	s.userChanLock.RLock()
	clients := make([]int, 0, len(s.userChans))
	for user, _ := range s.userChans {
		clients = append(clients, user)
	}
	s.userChanLock.RUnlock()
	return clients
}

//Send a message to all client channels specified in users
func notifyMany(s Service, users []int, event ProtocolEvent) {
	for _, user := range users {
		notifyUser(s, user, event)
	}
}

//Send a message to a client channel, or silently ignore it if client doesn't exist
func notifyUser(s Service, userId int, event ProtocolEvent) {
	toChan := getUserChannel(s, userId)
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
func handleEvent(s Service, event ProtocolEvent) {
	switch event.eventType {
	case PrivateMsg:
		notifyUser(s, event.toUserId, event)
	case Follow:
		FollowUser(s, event.fromUserId, event.toUserId)
		notifyUser(s, event.toUserId, event)
	case Unfollow:
		UnfollowUser(s, event.fromUserId, event.toUserId)
	case StatusUpdate:
		notifyMany(s, GetFollowers(s, event.fromUserId), event)
	case Broadcast:
		notifyMany(s, getAllClients(s), event)
	default:
		log.Fatalf("unable to handle" + event.payload)
	}
}

//Handle a stream of events from a connection-- must be single-threaded to order events
func handleEventStream(s Service, c net.Conn) {
	var (
		err             error
		eventPayload    string
		lastSequenceNum int       = 0
		eventBuffer     EventHeap = make(EventHeap, 0)
	)
	defer s.waitGroup.Done()
	defer c.Close()

	eventReader := bufio.NewReader(c)
	heap.Init(&eventBuffer)
	log.Printf("Event stream connected")

	for {
		if eventPayload, err = eventReader.ReadString('\n'); err != nil {
			if err == io.EOF {
				finishEvents(s)
			} else {
				log.Fatal(err)
			}
		}
		if event, err := parseEventPayload(strings.TrimSpace(eventPayload)); err != nil {
			log.Fatalf("Error parsing event: %s", err)
		} else {
			//process events so long as the next ones in sequenceNum order are available
			heap.Push(&(eventBuffer), &event)
			for len(eventBuffer) > 0 &&
				eventBuffer[0].sequenceNum == lastSequenceNum+1 {
				event = *(heap.Pop(&eventBuffer).(*ProtocolEvent))
				handleEvent(s, event)
				lastSequenceNum++
			}
		}
	}
}

//Accept connections from a client, create their channels, and pass on notifications
func handleClient(s Service, c net.Conn) {
	var (
		clientId int
		err      error
	)
	defer s.waitGroup.Done()
	defer c.Close()

	if clientId, err = getClientUser(c); err != nil {
		log.Fatal(err)
	}
	events := makeUserChannel(s, clientId)
	eventWriter := bufio.NewWriter(c)
	log.Printf("Client %d waiting for notifications", clientId)

	//wait for events until channel closes and write them to connection
	for event := range events {
		_, err := eventWriter.WriteString(event.ToWire())
		if err != nil {
			log.Fatal(err)
		}
		eventWriter.Flush()
	}
	runtime.Goexit()
}

//Service shared state
type Service struct {
	followers                    map[int](map[int]bool) //must be single-threaded
	eventsDone                   chan bool
	eventSrc, userClients        *net.TCPListener
	eventsConn, clientConn       *net.TCPConn
	eventSrcPort, userClientPort string
	waitGroup                    *sync.WaitGroup
	userChans                    map[int](chan ProtocolEvent)
	userChanLock                 sync.RWMutex
}

//Initialize service state
func MakeService(eventSrcPort string, userClientPort string) *Service {
	s := &Service{
		followers:      make(map[int](map[int]bool)),
		eventsDone:     make(chan bool),
		eventSrcPort:   eventSrcPort,
		userClientPort: userClientPort,
		waitGroup:      &sync.WaitGroup{},
		userChans:      make(map[int](chan ProtocolEvent)),
		userChanLock:   sync.RWMutex{},
	}
	return s
}

//Main server function:
//-Accept a single event source connection and handle
//-Accept many user client connections afterward and handle
func (s *Service) Serve() {
	const host = "localhost"
	var (
		laddr *net.TCPAddr
		err   error
	)
	defer s.waitGroup.Wait()
	defer s.eventSrc.Close()
	defer s.userClients.Close()

	//Handle a single event source connection
	if laddr, err = net.ResolveTCPAddr("tcp", host+s.eventSrcPort); err != nil {
		log.Fatal(err)
	}
	if s.eventSrc, err = net.ListenTCP("tcp", laddr); err != nil {
		log.Fatal(err)
	}
	if s.eventsConn, err = s.eventSrc.AcceptTCP(); err != nil {
		log.Fatal(err)
	}
	go handleEventStream(*s, s.eventsConn)
	s.waitGroup.Add(1)

	//Handle client connections
	if laddr, err = net.ResolveTCPAddr("tcp", host+s.userClientPort); err != nil {
		log.Fatal(err)
	}
	if s.userClients, err = net.ListenTCP("tcp", laddr); err != nil {
		log.Fatal(err)
	}
	//Have AcceptTCP time out every second so program may exit when events EOF is hit
	s.userClients.SetDeadline(time.Now().Add(time.Second))
	for {
		select {
		case <-s.eventsDone:
			runtime.Goexit()
		default:
			if s.clientConn, err = s.userClients.AcceptTCP(); err != nil {
				continue
			}
			go handleClient(*s, s.clientConn)
			s.waitGroup.Add(1)
		}
	}
}

func main() {
	const (
		eventSrcPort   = ":9090"
		userClientPort = ":9099"
	)
	defer os.Exit(0)

	s := MakeService(eventSrcPort, userClientPort)
	s.Serve()

}
