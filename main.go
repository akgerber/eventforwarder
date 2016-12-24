package main

import ( "log"; "net" )

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
