package operation

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

func StartSimulateErrorServer(_ *Config) {
	httpCode := ":10801"
	errSocket := ":10082"
	elog.Println("INFO", "start error simulate")
	go simulateConnectionError(errSocket)
	simulateHttpCode(httpCode)
}

func handleConnection(conn net.Conn) {
	elog.Println("INFO", "close connection", conn.RemoteAddr())
	conn.Close()
}

func simulateConnectionError(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		elog.Println("INFO", "listen failed", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			elog.Println("INFO", "accept error", err)
		}
		go handleConnection(conn)
	}
}

type debug struct {
}

func (d *debug) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	seps := strings.Split(strings.TrimPrefix(path, "/"), "/")
	code, err := strconv.ParseUint(seps[0], 10, 64)
	elog.Println("INFO", "request is", path)
	if err != nil {
		elog.Println("INFO", "parse code failed", seps[0], err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Length", "32")
	w.WriteHeader(int(code))
}

func simulateHttpCode(addr string) {
	http.ListenAndServe(addr, &debug{})
}
