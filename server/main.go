package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Message types for signaling
type SignalMessage struct {
	Type    string          `json:"type"`    // "offer", "answer", "ice-candidate"
	From    string          `json:"from"`    // sender ID
	To      string          `json:"to"`      // recipient ID
	Payload json.RawMessage `json:"payload"` // SDP or ICE candidate
}

// Client represents a connected peer
type Client struct {
	ID     string
	Conn   *websocket.Conn
	Send   chan []byte
	server *Server
}

// Server manages all connected clients
type Server struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	broadcast  chan *SignalMessage
	mu         sync.RWMutex
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

func NewServer() *Server {
	return &Server{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan *SignalMessage),
	}
}

// Run starts the server's main loop
func (s *Server) Run() {
	for {
		select {
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client.ID] = client
			s.mu.Unlock()
			log.Printf("Client registered: %s (total: %d)", client.ID, len(s.clients))

		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client.ID]; ok {
				delete(s.clients, client.ID)
				close(client.Send)
			}
			s.mu.Unlock()
			log.Printf("Client unregistered: %s (total: %d)", client.ID, len(s.clients))

		case message := <-s.broadcast:
			// Send message to specific recipient
			s.mu.RLock()
			if client, ok := s.clients[message.To]; ok {
				data, _ := json.Marshal(message)
				select {
				case client.Send <- data:
				default:
					close(client.Send)
					delete(s.clients, client.ID)
				}
			}
			s.mu.RUnlock()
		}
	}
}

// Handle WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	// Get client ID from query parameter
	clientID := r.URL.Query().Get("id")
	if clientID == "" {
		conn.Close()
		return
	}

	client := &Client{
		ID:     clientID,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		server: s,
	}

	s.register <- client

	// Start goroutines for reading and writing
	go client.writePump()
	go client.readPump()
}

// Read messages from the WebSocket
func (c *Client) readPump() {
	defer func() {
		c.server.unregister <- c
		c.Conn.Close()
	}()

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		// Parse the signaling message
		var msg SignalMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("JSON unmarshal error: %v", err)
			continue
		}

		log.Printf("Message from %s to %s: %s", msg.From, msg.To, msg.Type)

		// Forward the message to the recipient
		c.server.broadcast <- &msg
	}
}

// Write messages to the WebSocket
func (c *Client) writePump() {
	defer c.Conn.Close()

	for message := range c.Send {
		if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
			log.Printf("Write error: %v", err)
			return
		}
	}
}

// List all connected clients
func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clients := make([]string, 0, len(s.clients))
	for id := range s.clients {
		clients = append(clients, id)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"clients": clients,
		"count":   len(clients),
	})
}

func main() {
	server := NewServer()
	go server.Run()

	// WebSocket endpoint for signaling
	http.HandleFunc("/ws", server.handleWebSocket)

	// HTTP endpoint to list connected clients
	http.HandleFunc("/clients", server.handleClients)

	// Simple health check
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "P2P Signaling Server Running\n")
	})

	port := ":8080"
	log.Printf("Starting P2P signaling server on %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("ListenAndServe error:", err)
	}
}