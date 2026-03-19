package main

import (
	"database/sql"
	"fmt"
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

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS usage_stats (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			method TEXT,
			key TEXT,
			status TEXT,
			response_time_ms INTEGER
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_stats_timestamp ON usage_stats(timestamp)`)
	if err != nil {
		log.Fatal(err)
	}

	go cleanupExpired()

	http.HandleFunc("/", handleRequest)
	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if path == "/" || path == "/index.html" {
		if r.Method == http.MethodGet {
			http.ServeFile(w, r, "index.html")
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if path == "/stats" {
		handleStats(w, r)
		return
	}

	if apiKey != "" && r.Header.Get("X-API-Key") != apiKey {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
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
	start := time.Now()
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

	logStats("POST", key, time.Since(start).Milliseconds())
}

func handleGet(w http.ResponseWriter, r *http.Request, key string) {
	start := time.Now()
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

	logStats("GET", key, time.Since(start).Milliseconds())
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
				SELECT key FROM kv_store ORDER BY expires_at ASC LIMIT 1
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

func logStats(method, key string, durationMs int64) {
	go func() {
		db.Exec(`INSERT INTO usage_stats (method, key, response_time_ms) VALUES (?, ?, ?)`,
			method, key, durationMs)
	}()
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type statRow struct {
		TotalRequests   int64
		TotalPosts      int64
		TotalGets       int64
		UniqueKeys      int64
		AvgResponseTime float64
		TotalKeys       int64
	}

	var stats statRow

	err := db.QueryRow(`
		SELECT 
			COALESCE((SELECT COUNT(*) FROM usage_stats), 0) as total_requests,
			COALESCE((SELECT COUNT(*) FROM usage_stats WHERE method = 'POST'), 0) as total_posts,
			COALESCE((SELECT COUNT(*) FROM usage_stats WHERE method = 'GET'), 0) as total_gets,
			COALESCE((SELECT COUNT(DISTINCT key) FROM usage_stats), 0) as unique_keys,
			COALESCE((SELECT AVG(response_time_ms) FROM usage_stats), 0) as avg_response_time,
			COALESCE((SELECT COUNT(*) FROM kv_store), 0) as total_keys
	`).Scan(&stats.TotalRequests, &stats.TotalPosts, &stats.TotalGets, &stats.UniqueKeys, &stats.AvgResponseTime, &stats.TotalKeys)

	if err != nil {
		log.Printf("Stats query error: %v", err)
		http.Error(w, "Failed to retrieve stats", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"total_requests":` + fmt.Sprint(stats.TotalRequests) +
		`,"total_posts":` + fmt.Sprint(stats.TotalPosts) +
		`,"total_gets":` + fmt.Sprint(stats.TotalGets) +
		`,"unique_keys":` + fmt.Sprint(stats.UniqueKeys) +
		`,"total_keys":` + fmt.Sprint(stats.TotalKeys) +
		`,"avg_response_time_ms":` + fmt.Sprint(stats.AvgResponseTime) + `}`))
}
