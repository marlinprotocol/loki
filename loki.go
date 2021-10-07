package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"encoding/hex"
)

type Loki struct {
	connString string
	db         *sql.DB
	m          *sync.Mutex
}

func (l *Loki) connect() {
	// mutex assumed to be locked

	timerWait := time.Second
	for {
		log.Info("Connecting to DB...")
		db, err := sql.Open("postgres", l.connString)
		if err != nil {
			log.Error("DB error: ", err)
			// exponential backoff
			time.Sleep(timerWait * time.Second)
			timerWait *= 2
			if timerWait > 300*time.Second {
				timerWait = 300 * time.Second
			}

			continue
		}

		// have valid db here
		// unlock and return
		log.Info("DB connected")
		l.db = db
		l.m.Unlock()
		return
	}
}

func (l *Loki) handleSpan(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(404)
		return
	}

	spanId, err := strconv.Atoi(strings.TrimPrefix(r.URL.String(), "/bor/span/"))
	if err != nil {
		w.WriteHeader(400)
		return
	}

	query := "SELECT span FROM spans WHERE id = $1;"
	l.m.Lock()
	rows, err := l.db.Query(query, spanId)
	if err != nil {
		// trigger reconnection without unlocking
		go l.connect()

		w.WriteHeader(500)
		return
	}
	l.m.Unlock()
	defer rows.Close()

	next := rows.Next()
	if !next {
		// no results
		respBytes := []byte("{\"error\":\"{\"codespace\":\"sdk\",\"code\":1,\"message\":\"could not get span; span not found for id\"}\"}")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(respBytes)))
		w.WriteHeader(500)
		w.Write(respBytes)

		return
	}
	var result string
	err = rows.Scan(&result)
	if err != nil {
		w.WriteHeader(500)
		return
	}

	respBytes, err := hex.DecodeString(result)
	if err != nil {
		w.WriteHeader(500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(respBytes)))
	w.Write(respBytes)
}

func (l *Loki) handleClerk(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(404)
		return
	}

	respBytes := []byte("{\"height\":\"0\",\"result\":[]}")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(respBytes)))
	w.Write(respBytes)
}

func (l *Loki) ListenAndServe(addr string) error {
	http.HandleFunc("/bor/span/", l.handleSpan)
	http.HandleFunc("/clerk/", l.handleClerk)
	// lock and connect
	l.m.Lock()
	go l.connect()

	// TODO: db close?

	return http.ListenAndServe(addr, nil)
}
