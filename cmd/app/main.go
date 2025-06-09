package main

import (
	"bufio"
	"net"
	"os"

	kafka "github.com/nabinkhanal00/kafka/app"
	"github.com/nabinkhanal00/kafka/app/requests"
	"github.com/nabinkhanal00/kafka/app/responses"
	"github.com/sirupsen/logrus"
)

const BUFFER_SIZE = 1024

var log = logrus.New()

func main() {
	var _propFilePath string
	if len(os.Args) == 2 {

		_propFilePath = os.Args[1]
	}
	_ = _propFilePath

	log.Out = os.Stdout
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Warnln("Failed to bind to port 9092")
		os.Exit(1)
	}
	log.Infof("Listening on %s\n", l.Addr().String())
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
	for {
		n, err := conn.Read(buffer)
		log.Infoln("Read second set of data")
		if err != nil {
			log.Errorf("Could not read from %s: %v", c.RemoteAddr().String(), err)
			return
		}
		log.Infof("Read %d bytes from %s", n, c.RemoteAddr().String())
		log.Infof("Got Data: %X\n", buffer[:n])
		request, err := kafka.UnmarshallRequest(buffer[:n])
		if err != nil {
			log.Errorf("Failed to parse request: %v", err)
			return
		}
		rh, ok := request.Header.(*kafka.RequestHeaderV2)
		if !ok {
			log.Errorf("Invalid request header type")
			return
		}

		var response kafka.Response
		switch rh.RequestAPIKey {
		case kafka.ApiVersions:
			errorCode := kafka.NONE
			if rh.RequestAPIVersion < 0 || rh.RequestAPIVersion > 4 {
				errorCode = kafka.UNSUPPORTED_VERSION
			}

			response = kafka.Response{
				Header: &kafka.ResponseHeaderV0{
					CorrelationID: rh.CorrelationID,
				},
				Body: &responses.APIVersionsV4{
					ErrorCode: errorCode,
					APIKeys: []responses.APIKey{
						{
							Key:        18,
							MaxVersion: 4,
							MinVersion: 0,
						},
						{
							Key:        75,
							MaxVersion: 0,
							MinVersion: 0,
						},
					},
				},
			}
		case kafka.DescribeTopicPartitions:
			rb, ok := request.Body.(*requests.DescribeTopicPartitionsV0)
			if !ok {
				log.Errorf("Invalid request body type")
				return
			}
			requestTopics := rb.Topics
			responseTopics := []responses.Topic{}
			for _, topic := range requestTopics {
				rt := responses.Topic{}
				rt.ErrorCode = kafka.UNKNOWN_TOPIC_OR_PARTITION
				rt.TopicName = topic.Name
				responseTopics = append(responseTopics, rt)
			}
			response = kafka.Response{
				Header: &kafka.ResponseHeaderV1{
					CorrelationID: rh.CorrelationID,
				},
				Body: &responses.DescribeTopicPartitionsV0{
					Topics: responseTopics,
					NextCursor: responses.Cursor{
						TopicName:      "hello",
						PartitionIndex: 0,
					},
				},
			}

		default:

		}
		respBytes := kafka.MarshallResponse(response)
		n, err = conn.Write(respBytes)
		if err != nil {
			log.Errorf("Could not write to %s: %v", c.RemoteAddr().String(), err)
			return
		}
		log.Infof("Wrote %d bytes to %s: %X", n, c.RemoteAddr().String(), respBytes)
		conn.Flush()
	}
}
