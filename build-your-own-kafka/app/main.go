package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/api"
	apiversion "github.com/codecrafters-io/kafka-starter-go/app/api/api_version"
	describetopic "github.com/codecrafters-io/kafka-starter-go/app/api/describe_topic"
	"github.com/codecrafters-io/kafka-starter-go/app/api/fetch"
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
	"github.com/codecrafters-io/kafka-starter-go/app/model"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Start listening on 9092")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func Read(conn net.Conn) (*api.Message, error) {
	modelMsg := &model.Message{}

	messageSizeBytes := make([]byte, 4)
	_, err := conn.Read(messageSizeBytes)
	if err != nil {
		return nil, err
	}

	messageSize := int32(binary.BigEndian.Uint32(messageSizeBytes))
	bodyBytes := make([]byte, messageSize)
	_, err = conn.Read(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &api.RawRequest{}
	req = req.From(messageSizeBytes, bodyBytes)

	parsedMsg, _ := modelMsg.FromRawRequest(req)

	msg := &api.Message{}
	msg.MessageSize = parsedMsg.MessageSize
	msg.Header = parsedMsg.Header
	msg.Error = parsedMsg.Error
	msg.RequestBody = parsedMsg.RequestBody

	return msg, nil
}

func Send(conn net.Conn, response []byte) error {
	_, err := conn.Write(response)
	return err
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Println("Error closing connection: ", err.Error())
		}
	}(conn)

	for {
		msg, err := Read(conn)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			log.Println("Error reading data: ", err.Error())
			os.Exit(1)
		}

		respBytes := make([]byte, 8192)
		enc := &encoder.BinaryEncoder{}
		enc.Init(respBytes)

		switch msg.Header.ApiKey {
		case constant.ApiVersions:
			resp := apiversion.PrepareAPIVersionsResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error encoding response: ", err.Error())
				os.Exit(1)
			}
		case constant.DescribeTopicPartitions:
			resp := describetopic.PrepareDescribeTopicPartitionsResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error encoding response: ", err.Error())
				os.Exit(1)
			}
		case constant.Fetch:
			resp := fetch.PrepareFetchResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error encoding response: ", err.Error())
			}
		}

		respBytes = enc.ToKafkaResponse()
		err = Send(conn, respBytes)
		if err != nil {
			log.Println("Error writing data: ", err.Error())
			os.Exit(1)
		}
	}
}
