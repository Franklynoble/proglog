package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

/*
NewHTTPServer(addr string) takes in an address for the server to run on and returns
an *http.Server
*/

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()

	r := mux.NewRouter()

	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}
type ConsumeResponse struct {
	Record Record `json:"record"`
}

/*
We now have a server referencing a log for the server to defer to in its handlers.
A produce request contains the record that the caller of our API wants
appended to the log, and a produce response tells the caller what offset the
log stored the records under. A consume request specifies which records the
*/

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest

	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: off}
	// new Necoder response to the ResponseWriter
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

/*
The produce handler implements the three steps we discussed before:
unmarshaling the request into a struct, using that struct to produce to the
log and getting the offset that the log stored the record under, and marshaling
and writing the result to the response

The consume handler is like the produce handler but calls Read(offset uint64) to
get the record stored in the log
*/
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {

	var req ConsumeRequest

	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)

	if err == ErrOfsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
