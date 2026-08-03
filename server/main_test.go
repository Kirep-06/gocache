package main

import (
	"bufio"
	"bytes"
	"testing"
)

func TestReadClientPipelinedRequests(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
	reader := bufio.NewReader(bytes.NewBufferString(input))
	server := NewServer(512 * 1024)
	req := &Request{buf: new(bytes.Buffer)}
	if err := server.ReadClient(reader, req); err != nil || len(req.args) != 3 || string(req.args[0]) != "set" || string(req.args[2]) != "value" {
		t.Fatalf("first request: args=%q err=%v", req.args, err)
	}
	req.Reset()
	if err := server.ReadClient(reader, req); err != nil || len(req.args) != 2 || string(req.args[0]) != "get" {
		t.Fatalf("second request: args=%q err=%v", req.args, err)
	}
}

func TestReadClientRejectsMalformedArray(t *testing.T) {
	server := NewServer(512 * 1024)
	req := &Request{buf: new(bytes.Buffer)}
	if err := server.ReadClient(bufio.NewReader(bytes.NewBufferString("*0\r\n")), req); err != protocolErr {
		t.Fatalf("zero-argument array: err=%v", err)
	}
}

func TestServerCommands(t *testing.T) {
	server := NewServer(512 * 1024)
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"ping"}, "+PONG\r\n"},
		{[]string{"set", "key", "value"}, "+OK\r\n"},
		{[]string{"dbsize"}, ":1\r\n"},
		{[]string{"get", "key"}, "$5\r\nvalue\r\n"},
		{[]string{"del", "key"}, ":1\r\n"},
		{[]string{"get", "key"}, "$-1\r\n"},
		{[]string{"setex", "key", "bad", "value"}, "-ERR unsupported command\r\n"},
		{[]string{"unknown"}, "-ERR unsupported command\r\n"},
	}
	for _, tc := range cases {
		args := make([][]byte, len(tc.args))
		for i, arg := range tc.args {
			args[i] = []byte(arg)
		}
		if got := string(server.execute(args)); got != tc.want {
			t.Fatalf("%v: got %q want %q", tc.args, got, tc.want)
		}
	}
}
