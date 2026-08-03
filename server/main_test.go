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
