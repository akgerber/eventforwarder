package main

import ( "log"; "net" )

func main() {
    eventSrc, err := net.Listen("tcp", ":9090")
    if err != nil {
        log.Fatal(err)
    }
    for {
        conn, err := eventSrc.Accept()
        if err != nil {
            log.Println(err)
            continue
        }
        go handleConnection(conn)
    }
}

func handleConnection(c net.Conn) {
    buf := make([]byte, 4096)

    for {
        n, err:= c.Read(buf)
        if err != nil || n == 0 {
            c.Close()
            break
        }
        n, err = c.Write(buf[0:n])
        if err != nil {
            c.Close()
            break
        }
        log.Printf("Connection from %v closed.", c.RemoteAddr())
    }
}
