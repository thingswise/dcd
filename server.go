package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
)

type HttpServer struct {
	socket  string
	port    int
	_type   string
	systems map[string]*System
	// progress handlers
	progressHandlers map[string]*ProgressHandler
	phMutex          *sync.Mutex
}

func NewHttpServerUnixSocket(socket string, systems map[string]*System) *HttpServer {
	return &HttpServer{
		_type:            "unix",
		socket:           socket,
		systems:          systems,
		progressHandlers: make(map[string]*ProgressHandler),
		phMutex:          &sync.Mutex{},
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
		SendJson(w, ErrorMessage{Message: err.Error()})
	case 2, 3, 4, 6:
		w.WriteHeader(400)
		SendJson(w, ErrorMessage{Message: err.Error()})
	default:
		w.WriteHeader(500)
		SendJson(w, ErrorMessage{Message: err.Error()})
	}
}

func SendJson(w http.ResponseWriter, msg interface{}) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if _, err := w.Write(bytes); err != nil {
		return err
	}

	return nil
}

func (s *HttpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	system, ok := s.systems[path]
	if !ok {
		s.handleError(NewOperationError(UnknownFile, fmt.Sprintf("Unknown file: %s", path)), w)
		return
	}

	var progressHandler *ProgressHandler = nil
	progress := req.URL.Query().Get("progress")
	if req.Method != "PROGRESS" && progress != "" {
		progressHandler = &ProgressHandler{
			Id:       progress,
			Total:    -1,
			Progress: -1,
		}
		if err := s.registerProgressHandler(progressHandler); err != nil {
			s.handleError(err, w)
			return
		}
		defer s.unregisterProgressHandler(progressHandler)
	}

	switch req.Method {
	case "GET":
		err := system.Get(w, progressHandler)
		if err != nil {
			s.handleError(err, w)
			return
		}
	case "EDIT":
		err := system.Edit(req.URL.Query().Get("force") == "true", progressHandler)
		if err != nil {
			s.handleError(err, w)
			return
		}
		if progressHandler != nil {
			progressHandler.SendJson(w)
		} else {
			w.WriteHeader(200)
		}
	case "COMMIT":
		err := system.Commit(req.URL.Query().Get("force") == "true", progressHandler)
		if err != nil {
			s.handleError(err, w)
			return
		}
		if progressHandler != nil {
			progressHandler.SendJson(w)
		} else {
			w.WriteHeader(200)
		}
	case "UPDATE":
		err := system.Update(req.URL.Query().Get("force") == "true", progressHandler)
		if err != nil {
			s.handleError(err, w)
			return
		}
		if progressHandler != nil {
			progressHandler.SendJson(w)
		} else {
			w.WriteHeader(200)
		}
	case "PROGRESS":
		if progress == "" {
			err := NewOperationError(InvalidRequest, "Missing request parameter `progress`")
			s.handleError(err, w)
			return
		}
		ph := s.lookupProgressHandler(progress)
		if ph == nil {
			err := NewOperationError(InvalidRequest, "Could not find progress handler: `"+progress+"`")
			s.handleError(err, w)
			return
		}
		ph.SendJson(w)
	}
}

func (s *HttpServer) registerProgressHandler(h *ProgressHandler) error {
	s.phMutex.Lock()
	defer s.phMutex.Unlock()

	if _, ok := s.progressHandlers[h.Id]; ok {
		return NewOperationError(InvalidRequest, "Progress handler with id `"+h.Id+"` has already been registered")
	}

	s.progressHandlers[h.Id] = h
	return nil
}

func (s *HttpServer) unregisterProgressHandler(h *ProgressHandler) {
	s.phMutex.Lock()
	defer s.phMutex.Unlock()

	delete(s.progressHandlers, h.Id)
}

func (s *HttpServer) lookupProgressHandler(id string) *ProgressHandler {
	s.phMutex.Lock()
	defer s.phMutex.Unlock()

	if h, ok := s.progressHandlers[id]; ok {
		return h
	} else {
		return nil
	}
}
