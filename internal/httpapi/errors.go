package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// errorBody is the single shape every failure takes.
//
// The machine-readable code is the contract; the message is for a human
// reading logs. A client that has to distinguish "sold out" from "you already
// bought one" should switch on Code, never on the prose, because prose is
// exactly the thing that gets reworded.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeError renders a failure. It is a free function rather than a method so
// that middleware, which has no Server, can use it too.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	// Failure bodies are small, fixed structs that cannot fail to encode,
	// so there is nothing useful to do with an error here.
	_ = json.NewEncoder(w).Encode(errorBody{Error: errorDetail{Code: code, Message: message}})
}

// writeInternalError hides the cause from the client and records it for us.
//
// An internal error is either a bug or an outage. Either way the detail is
// useless to the caller and occasionally tells them more about the system than
// they should know.
func (s *Server) writeInternalError(w http.ResponseWriter, msg string, err error) {
	s.logger.Error(msg, slog.Any("error", err))
	writeError(w, http.StatusInternalServerError, "internal_error", "something went wrong on our side")
}
