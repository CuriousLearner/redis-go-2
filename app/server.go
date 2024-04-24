package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func parseCommand(encodedCommand []byte) (command string, err error) {
	// TODO: Add validation for correct RESP format

	commandString := string(encodedCommand)
	if commandString[0] != '*' {
		return "", errors.New("Invalid command")
	}
	args := strings.Split(commandString, "\r\n")
	for i, arg := range args[2:] {
		if i%2 == 0 {
			command += arg + " "
		}
	}
	return command, nil
}

func handleConnection(conn net.Conn, buffer []byte) {
	for {
		n, err := conn.Read(buffer)
		if errors.Is(err, io.EOF) {
			fmt.Println("[INFO] EOF detected: ", err.Error())
			break
		}
		if err != nil {
			fmt.Println("Error reading: ", err.Error())
		}
		respEncodedCommand := buffer[:n]
		command, err := parseCommand(respEncodedCommand)
		if err != nil {
			fmt.Println("Error parsing command: ", err.Error())
			break
		}

		response := processCommand(command)
		generateResponse(conn, response)
	}
}

func generateResponse(conn net.Conn, response string) {
	_, err := conn.Write([]byte(response))
	if err != nil {
		fmt.Println("Error writing: ", err.Error())
	}
}

func processCommand(commandString string) (response string) {
	parsedCommand := strings.Split(commandString, " ")
	command := strings.ToUpper(parsedCommand[0])
	args := strings.Trim(strings.Join(parsedCommand[1:], " "), " ")
	switch command {
	case "PING":
		response = "+PONG\r\n"
	case "ECHO":
		response = fmt.Sprintf("$%d\r\n%s\r\n", len(args), args)
	case "SET":
		keyValue := strings.Split(args, " ")
		expiresMilli := 0
		var err error
		if len(keyValue) == 4 {
			expiresMilli, err = strconv.Atoi(keyValue[3])
			if strings.ToUpper(keyValue[2]) != "PX" || err != nil {
				response = "-ERR unknown command\r\n"
				break
			}
		}
		handleSetCommand(keyValue[0], keyValue[1], int64(expiresMilli))
		response = "+OK\r\n"
	case "GET":
		valueExp, exists := handleGetCommand(args)
		if exists {
			response = fmt.Sprintf("$%d\r\n%s\r\n", len(valueExp.value), valueExp.value)
		} else {
			// `-1\r\n` (a "null bulk string")
			response = "$-1\r\n"
		}
	case "CONFIG":
		if strings.ToUpper(parsedCommand[1]) != "GET" {
			response = "-ERR unknown command\r\n"
			break
		}
		key := string(parsedCommand[2])
		response = formatRESPArray([]string{key, getConfig(key)})
	default:
		response = "-ERR unknown command\r\n"
	}
	return response
}

func getConfig(key string) string {
	return config[key]
}

type KVExpiry struct {
	value  string
	expiry int64
}

var db = make(map[string]KVExpiry)

func handleSetCommand(key, value string, expiry int64) {
	expires := int64(0)
	if expiry != 0 {
		expires = time.Now().UnixMilli() + expiry
	}
	valueExp := KVExpiry{value: value, expiry: expires}
	db[key] = valueExp
}

func handleGetCommand(key string) (KVExpiry, bool) {
	valueExp, exists := db[key]
	if exists && valueExp.expiry != 0 && time.Now().UnixMilli() > valueExp.expiry {
		delete(db, key)
		exists = false
	}
	return valueExp, exists
}

func formatRESPBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func formatRESPArray(elements []string) string {
	resp := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		resp += formatRESPBulkString(element)
	}
	return resp
}

var dir string
var dbfilename string

func initConfig() {
	flag.StringVar(&dir, "dir", "/default/path", "directory where RDB files are stored")
	flag.StringVar(&dbfilename, "dbfilename", "dump.rdb", "name of the RDB file")
	flag.Parse()
	config["dir"] = dir
	config["dbfilename"] = dbfilename
}

var config = make(map[string]string)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	initConfig()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	buffer := make([]byte, 1024)

	for {
		conn, err := l.Accept()
		defer conn.Close()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn, buffer)

	}

}
