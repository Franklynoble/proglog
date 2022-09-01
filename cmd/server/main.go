package main

import (
	"log"

	"github.com/Franklynoble/prolog/internal/server"
)

/*
Our main() function just needs to create and start the server, passing in the
address to listen on (localhost:8080) and telling the server to listen for and handle
requests by calling ListenAndServe(). Wrapping our server with the *net/http.Server
in NewHTTPServer()
*/

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
