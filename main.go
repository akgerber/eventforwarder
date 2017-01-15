package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

func main() {
	log.SetOutput(os.Stdout)

	eventSrc, err := net.Listen("tcp", ":9090")
	userClients, err := net.Listen("tcp", ":9099")
	if err != nil {
		log.Fatal(err)
	}

	eventsConn, err := eventSrc.Accept()
	if err != nil {
		log.Println(err)
	}
	go handleEventStream(eventsConn)
	for {
		clientConn, err := userClients.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleClient(clientConn)
	}
}

var userChans = struct {
	sync.RWMutex
	m map[int](chan ProtocolEvent)
}{m: map[int](chan ProtocolEvent){}}

var followers = make(map[int](map[int]bool))

var orderedEvents = struct {
	sync.Mutex
	h EventHeap
}{h: make(EventHeap, 0)}

func getUserChannel(user int) (userChan chan ProtocolEvent) {
	userChans.RLock()
	userChan = userChans.m[user]
	userChans.RUnlock()
	return
}

func makeUserChannel(user int) (userChan chan ProtocolEvent) {
	const bufferSize = 0
	userChan = getUserChannel(user)

	//if the user-channel hasn't been created yet, initialize it
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

func handleEventStream(c net.Conn) {
	var (
		eventPayload    string
		lastSequenceNum int
		err             error
	)
	eventReader := bufio.NewReader(c)
	lastSequenceNum = 0

	orderedEvents.Lock()
	heap.Init(&orderedEvents.h)
	orderedEvents.Unlock()

	for {
		if eventPayload, err = eventReader.ReadString('\n'); err != nil {
			c.Close()
			break
		}
		if event, err := parseEventPayload(strings.TrimSpace(eventPayload)); err != nil {
			log.Printf("Error parsing event: %s", err)
		} else {
			orderedEvents.Lock()
			heap.Push(&(orderedEvents.h), &event)
			log.Printf("Received event: %d; Previously processed event: %d", event.sequenceNum, lastSequenceNum)

			for len(orderedEvents.h) > 0 &&
				orderedEvents.h[0].sequenceNum == lastSequenceNum+1 {
				event = *(heap.Pop(&orderedEvents.h).(*ProtocolEvent))
				log.Printf("Processing event: %d", event.sequenceNum)
				handleEvent(event)
				lastSequenceNum++
			}
			orderedEvents.Unlock()
		}
	}
}

func handleEvent(event ProtocolEvent) {
	log.Println(event.payload)

	switch event.eventType {
	case PrivateMsg:
		notify(event.toUserId, event)
	case Follow:
		follow(event.fromUserId, event.toUserId)
		notify(event.toUserId, event)
	case Unfollow:
		unfollow(event.fromUserId, event.toUserId)
	case StatusUpdate:
		notifyMany(getFollowers(event.fromUserId), event)
	case Broadcast:
		notifyMany(getAllClients(), event)
	default:
		log.Println("unable to handle" + event.payload)
	}
}

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

func follow(fromUserId int, toUserId int) {
	if followers[toUserId] == nil {
		followers[toUserId] = make(map[int]bool)
	}
	followers[toUserId][fromUserId] = true
	log.Printf("userId %d has followers %v", toUserId, followers[toUserId])
}

func unfollow(fromUserId int, toUserId int) {
	if followers[toUserId] != nil {
		delete(followers[toUserId], fromUserId)
	}
	log.Printf("userId %d has followers %v", toUserId, followers[toUserId])
}

func getFollowers(userId int) []int {
	usersFollowers := make([]int, 0, len(followers[userId]))
	for follower, _ := range followers[userId] {
		usersFollowers = append(usersFollowers, follower)
	}
	log.Printf("userId %d has followers %v", userId, usersFollowers)
	return usersFollowers
}

func notifyMany(users []int, event ProtocolEvent) {
	for _, user := range users {
		notify(user, event)
	}
}

func notify(userId int, event ProtocolEvent) {
	toChan := getUserChannel(userId)
	if toChan != nil {
		log.Printf("Notifying user %d of event %#v", userId, event.payload)
		toChan <- event
	} else {
		log.Printf("No client for user %d; ignoring  %#v", userId, event.payload)
	}
}

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

func handleClient(c net.Conn) {
	var (
		clientId int
		err      error
	)

	if clientId, err = getClientUser(c); err != nil {
		log.Println(err)
		c.Close()
		return
	}
	events := makeUserChannel(clientId)
	log.Printf("Client %d waiting for notifications", clientId)
	eventWriter := bufio.NewWriter(c)

	for {
		//wait for events on channel and write them
		event := <-events
		log.Printf("Sending %s to client %d", event.payload, clientId)
		_, err := eventWriter.WriteString(event.payload + "\r\n")
		if err != nil {
			log.Println(err)
			c.Close()
			break
		}
		eventWriter.Flush()
	}
}
