package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("TCP server")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleMessage(conn)
	}
}

func handleMessage(conn net.Conn) {

	defer conn.Close()

	var data = make([]byte, 3072)
	conn.Read(data)

	bulkStrings := bytes.Split(data, []byte("\r\n"))

	var command = string(bulkStrings[2])
	noOfStrings, err := strconv.Atoi(string(bulkStrings[0][1]))

	if err != nil {
		os.Exit(1)
	}

	switch strings.ToLower(command) {
	case "ping":
		{
			if noOfStrings > 2 {
				conn.Write(wrongNumberOfArgumentsError(command))
				return
			}

			if noOfStrings == 2 {
				conn.Write(handlePingMessage(string(bulkStrings[4])))
			}

			conn.Write(handlePingMessage(""))
		}

	case "echo":
		{

			if noOfStrings != 2 {
				conn.Write(wrongNumberOfArgumentsError(command))
				return
			}

			conn.Write(handleEchoCommand(string(bulkStrings[4])))
		}

	case "set":
		{

		}

	case "get":
		{

		}

	default:
		{
			conn.Write(handleDefaultCase(command))
		}
	}
}

func handlePingMessage(message string) []byte {

	if len(message) > 0 {
		return []byte(fmt.Sprintf("+%s\r\n", message))
	}

	// EX: resp simple string "+OK\r\n"
	return []byte("+PONG\r\n")
}

func handleEchoCommand(message string) []byte {

	// Ex: resp bulk string "$5\r\nhello\r\n"

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message))
}

func handleDefaultCase(command string) []byte {
	// EX: Error format "-Error message\r\n"

	respErrorMessage := fmt.Sprintf("-ERR unknown command '%v'\r\n", command)

	return []byte(respErrorMessage)
}

// Error message: (error) ERR wrong number of arguments for 'echo' command
func wrongNumberOfArgumentsError(command string) []byte {

	return []byte(fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", command))
}
