// A small Redis protocol server backed by gocache.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/Kirep-06/gocache"
)

var protocolErr = errors.New("protocol error")

type Request struct {
	args [][]byte
	buf  *bytes.Buffer
}

func (req *Request) Reset() {
	req.args = req.args[:0]
	req.buf.Reset()
}

type Server struct {
	cache *gocache.Cache
}

func NewServer(cacheSize int) *Server {
	return &Server{cache: gocache.NewCache(cacheSize)}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protocolErr
	}
	return line[:len(line)-2], nil
}

func btoi(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	value, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, protocolErr
	}
	return value, nil
}

func lower(data []byte) {
	for i := range data {
		if data[i] >= 'A' && data[i] <= 'Z' {
			data[i] += 'a' - 'A'
		}
	}
}

// ReadClient decodes one array of bulk strings from the connection.
func (server *Server) ReadClient(r *bufio.Reader, req *Request) error {
	line, err := readLine(r)
	if err != nil {
		return err
	}
	if len(line) == 0 || line[0] != '*' {
		return protocolErr
	}
	argc, err := btoi(line[1:])
	if err != nil || argc < 1 || argc > 4 {
		return protocolErr
	}
	var starts, ends [4]int
	req.buf.Write(line)
	req.buf.WriteString("\r\n")
	cursor := len(line) + 2
	for i := 0; i < argc; i++ {
		line, err = readLine(r)
		if err != nil {
			return err
		}
		if len(line) == 0 || line[0] != '$' {
			return protocolErr
		}
		length, parseErr := btoi(line[1:])
		if parseErr != nil || length < 0 || length > 512*1024*1024 {
			return protocolErr
		}
		req.buf.Write(line)
		req.buf.WriteString("\r\n")
		cursor += len(line) + 2
		starts[i], ends[i] = cursor, cursor+length
		if _, err := io.CopyN(req.buf, r, int64(length)+2); err != nil {
			return err
		}
		cursor += length + 2
	}
	data := req.buf.Bytes()
	for i := 0; i < argc; i++ {
		req.args = append(req.args, data[starts[i]:ends[i]])
	}
	lower(req.args[0])
	return nil
}

func (server *Server) execute(args [][]byte) []byte {
	unsupported := []byte("-ERR unsupported command\r\n")
	switch {
	case len(args) == 1 && bytes.Equal(args[0], []byte("ping")):
		return []byte("+PONG\r\n")
	case len(args) == 1 && bytes.Equal(args[0], []byte("dbsize")):
		return []byte(":" + strconv.FormatInt(server.cache.EntryCount(), 10) + "\r\n")
	case len(args) == 2 && bytes.Equal(args[0], []byte("get")):
		value, err := server.cache.Get(args[1])
		if err != nil {
			return []byte("$-1\r\n")
		}
		var reply bytes.Buffer
		reply.WriteString("$" + strconv.Itoa(len(value)) + "\r\n")
		reply.Write(value)
		reply.WriteString("\r\n")
		return reply.Bytes()
	case len(args) == 2 && bytes.Equal(args[0], []byte("del")):
		if server.cache.Del(args[1]) {
			return []byte(":1\r\n")
		}
		return []byte(":0\r\n")
	case len(args) == 3 && bytes.Equal(args[0], []byte("set")):
		server.cache.Set(args[1], args[2], 0)
		return []byte("+OK\r\n")
	case len(args) == 4 && bytes.Equal(args[0], []byte("setex")):
		expire, err := btoi(args[2])
		if err != nil {
			return unsupported
		}
		server.cache.Set(args[1], args[3], expire)
		return []byte("+OK\r\n")
	default:
		return unsupported
	}
}

type Session struct {
	server    *Server
	conn      net.Conn
	reader    *bufio.Reader
	replyChan chan []byte
}

func (server *Server) serveConn(conn net.Conn) {
	session := &Session{
		server:    server,
		conn:      conn,
		reader:    bufio.NewReader(conn),
		replyChan: make(chan []byte, 100),
	}
	go session.readLoop()
	go session.writeLoop()
}

func (session *Session) readLoop() {
	defer close(session.replyChan)
	req := &Request{buf: new(bytes.Buffer)}
	for {
		req.Reset()
		if err := session.server.ReadClient(session.reader, req); err != nil {
			return
		}
		session.replyChan <- session.server.execute(req.args)
	}
}

func (session *Session) writeLoop() {
	defer session.conn.Close()
	for reply := range session.replyChan {
		if _, err := session.conn.Write(reply); err != nil {
			return
		}
	}
}

func (server *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	log.Println("Listening on port", addr)
	for {
		if tcpListener, ok := listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(time.Second))
		}
		conn, err := listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				continue
			}
			return err
		}
		server.serveConn(conn)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
	debug.SetGCPercent(10)
	go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()
	log.Println(NewServer(256 * 1024 * 1024).Start(":7788"))
}
