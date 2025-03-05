package handler

import (
	"encoding/json"
	"github.com/nbarbey/go-event-store/eventstore"
	"net/http"
)

type Server[E any] struct {
	address    string
	eventStore *eventstore.EventStore[E]
	*http.ServeMux
}

func (e Server[E]) Start() error {
	return http.ListenAndServe(e.address, e.ServeMux)
}

func (e Server[E]) Stop() {}

func NewServerFromEventStore[E any](hostname string, es *eventstore.EventStore[E]) *Server[E] {
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", func(writer http.ResponseWriter, request *http.Request) {
		var event E
		err := json.NewDecoder(request.Body).Decode(&event)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		_, err = es.Publish(request.Context(), event)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("/all", func(writer http.ResponseWriter, request *http.Request) {
		events, err := es.All(request.Context())
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		err = json.NewEncoder(writer).Encode(events)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	return &Server[E]{address: hostname, eventStore: es, ServeMux: mux}
}
