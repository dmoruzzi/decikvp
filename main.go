package main

import (
	"database/sql"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/glebarez/sqlite"
)

const maxDBSize = 64 * 1024 * 1024 // 64MB

var (
	lastCleanup time.Time
	cleanupMu   sync.Mutex
	apiKey      = os.Getenv("API_KEY")
)

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("sqlite", "./kvp.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS kv_store (
			key TEXT PRIMARY KEY,
			value TEXT,
			expires_at DATETIME
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	go cleanupExpired()

	http.HandleFunc("/", handleRequest)
	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	if apiKey != "" && r.Header.Get("X-API-Key") != apiKey {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := r.URL.Path

	if path == "/" || path == "/index.html" {
		if r.Method == http.MethodGet {
			http.ServeFile(w, r, "index.html")
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	key := path[1:]
	if key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		handlePost(w, r, key)
	case http.MethodGet:
		handleGet(w, r, key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	expiresAt := time.Now().Add(1 * time.Hour)

	_, err = db.Exec(`
		INSERT OR REPLACE INTO kv_store (key, value, expires_at)
		VALUES (?, ?, ?)
	`, key, body, expiresAt)
	if err != nil {
		http.Error(w, "Failed to store value", http.StatusInternalServerError)
		return
	}

	go checkAndCleanupBySize()

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("stored"))
}

func handleGet(w http.ResponseWriter, r *http.Request, key string) {
	var value string
	var expiresAt time.Time

	err := db.QueryRow(`
		SELECT value, expires_at FROM kv_store
		WHERE key = ?
	`, key).Scan(&value, &expiresAt)

	if err == sql.ErrNoRows {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Failed to retrieve value", http.StatusInternalServerError)
		return
	}

	if time.Now().After(expiresAt) {
		db.Exec("DELETE FROM kv_store WHERE key = ?", key)
		http.Error(w, "Key expired", http.StatusNotFound)
		return
	}

	w.Write([]byte(value))
}

func cleanupExpired() {
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		db.Exec("DELETE FROM kv_store WHERE expires_at < ?", time.Now())
		log.Println("Cleaned up expired entries")
	}
}

func checkAndCleanupBySize() {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()

	if time.Since(lastCleanup) < 1*time.Minute {
		return
	}

	fi, err := os.Stat("./kvp.db")
	if err != nil || fi.Size() < maxDBSize {
		lastCleanup = time.Now()
		return
	}

	log.Printf("Database size %d exceeds max %d, cleaning up...", fi.Size(), maxDBSize)
	for {
		fi, _ := os.Stat("./kvp.db")
		if fi.Size() < maxDBSize {
			break
		}
		result, err := db.Exec(`
			DELETE FROM kv_store WHERE key IN (
				SELECT key FROM kv_store ORDER BY expires_at ASC
			)
		`)
		if err != nil || result == nil {
			break
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			break
		}
	}
	lastCleanup = time.Now()
	log.Println("Cleanup by size complete")
}
