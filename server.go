package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
)

type HttpServer struct {
	socket  string
	port    int
	_type   string
	systems map[string]*System
}

func NewHttpServerUnixSocket(socket string, systems map[string]*System) *HttpServer {
	return &HttpServer{
		_type:   "unix",
		socket:  socket,
		systems: systems,
	}
}

func (s *HttpServer) Serve() error {
	var listener net.Listener
	if s._type == "unix" {
		os.Remove(s.socket)
		l, err := net.Listen("unix", s.socket)
		if err != nil {
			return err
		}

		listener = l
	} else if s._type == "tcp" {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
		if err != nil {
			return err
		}

		listener = l
	} else {
		return fmt.Errorf("Unsupported protocol: %s", s._type)
	}
	defer listener.Close()

	return http.Serve(listener, s)
}

func (s *HttpServer) Close() {
	os.Remove(s.socket)
}

func (s *HttpServer) handleError(err error, w http.ResponseWriter) {
	w.Header().Add("content-type", "application/json")
	switch GetErrorType(err) {
	case 1:
		w.WriteHeader(500)
		sendJson(w, ErrorMessage{Message: err.Error()})
	case 2, 3, 4:
		w.WriteHeader(400)
		sendJson(w, ErrorMessage{Message: err.Error()})
	default:
		w.WriteHeader(500)
		sendJson(w, ErrorMessage{Message: err.Error()})
	}
}

func sendJson(w http.ResponseWriter, msg interface{}) {
	bytes, err := json.Marshal(msg)
	if err != nil {
		panic(err.Error())
	}

	if _, err := w.Write(bytes); err != nil {
		panic(err.Error())
	}
}

func (s *HttpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	system, ok := s.systems[path]
	if !ok {
		s.handleError(NewOperationError(UnknownFile, fmt.Sprintf("Unknown file: %s", path)), w)
		return
	}

	switch req.Method {
	case "GET":
		err := system.Get(w)
		if err != nil {
			s.handleError(err, w)
			return
		}
	case "EDIT":
		err := system.Edit()
		if err != nil {
			s.handleError(err, w)
			return
		}
		w.WriteHeader(200)
	case "COMMIT":
		err := system.Commit(req.URL.Query().Get("force") == "true")
		if err != nil {
			s.handleError(err, w)
			return
		}
		w.WriteHeader(200)
	case "UPDATE":
		err := system.Update(req.URL.Query().Get("force") == "true")
		if err != nil {
			s.handleError(err, w)
			return
		}
		w.WriteHeader(200)
	}
}
