package main

import (
	"bufio"
	"net"
	"os"

	"github.com/sirupsen/logrus"
)

const BUFFER_SIZE = 1024

var log = logrus.New()

func main() {
	log.Out = os.Stdout
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Warnln("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Errorf("Could not accept connection.\n")
		} else {
			log.Infof("Accepted Connection from %s\n", conn.RemoteAddr().String())
		}
		go handleConnection(conn)
	}
}

func handleConnection(c net.Conn) {
	buffer := make([]byte, BUFFER_SIZE)
	connectionString := c.RemoteAddr().String()
	conn := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
	n, err := conn.Read(buffer)
	if err != nil {
		log.Errorf("Could not read from %s\n", connectionString)
	} else {
		log.Infof("Read %d data from %s\n", n, connectionString)
	}

	request, _ := UnmarshallRequest(buffer)
	response := Response{
		MessageSize: 4,
		Header: &ResponseHeaderV0{
			CorrelationID: (request.Header).(*RequestHeaderV2).CorrelationID,
		},
	}
	n, err = conn.Write(MarshallResponse(response))
	if err != nil {
		log.Errorf("Could not write to %s\n", connectionString)
	} else {
		log.Infof("Wrote %d data to %s\n", n, connectionString)
	}
	conn.Flush()
	c.Close()
}
