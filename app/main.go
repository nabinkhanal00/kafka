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
			log.Errorf("Could not accept connection: %v", err)
			continue
		}
		log.Infof("Accepted Connection from %s", conn.RemoteAddr().String())
		go handleConnection(conn)
	}
}

func handleConnection(c net.Conn) {
	defer c.Close()
	buffer := make([]byte, BUFFER_SIZE)
	conn := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
	n, err := conn.Read(buffer)
	if err != nil {
		log.Errorf("Could not read from %s: %v", c.RemoteAddr().String(), err)
		return
	}
	log.Infof("Read %d bytes from %s", n, c.RemoteAddr().String())

	request, err := UnmarshallRequest(buffer[:n])
	if err != nil {
		log.Errorf("Failed to parse request: %v", err)
		return
	}
	rh, ok := request.Header.(*RequestHeaderV2)
	if !ok {
		log.Errorf("Invalid request header type")
		return
	}
	response := Response{
		Header: &ResponseHeaderV0{
			CorrelationID: rh.CorrelationID,
		},
		Body: &APIVersionsResponseV3{
			ErrorCode: UNSUPPORTED_VERSION,
		},
	}
	respBytes := MarshallResponse(response)
	n, err = conn.Write(respBytes)
	if err != nil {
		log.Errorf("Could not write to %s: %v", c.RemoteAddr().String(), err)
		return
	}
	log.Infof("Wrote %d bytes to %s", n, c.RemoteAddr().String())
	conn.Flush()
}
