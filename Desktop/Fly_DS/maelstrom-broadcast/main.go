package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n           *maelstrom.Node
	messages    map[int]bool
	messagesMu  sync.RWMutex
	topology    []string
	topologyMu  sync.RWMutex
	broadcasts  map[string]map[int]bool // node -> message -> sent
	broadcastMu sync.RWMutex
}

func newServer() *server {
	return &server{
		n:          maelstrom.NewNode(),
		messages:   make(map[int]bool),
		broadcasts: make(map[string]map[int]bool),
	}
}

func (s *server) broadcast(msg int, excludeNode string) {
	s.messagesMu.Lock()
	if s.messages[msg] {
		s.messagesMu.Unlock()
		return
	}
	s.messages[msg] = true
	s.messagesMu.Unlock()

	s.topologyMu.RLock()
	nodes := s.topology
	s.topologyMu.RUnlock()

	for _, node := range nodes {
		if node == excludeNode {
			continue
		}

		s.broadcastMu.Lock()
		if _, exists := s.broadcasts[node]; !exists {
			s.broadcasts[node] = make(map[int]bool)
		}
		if s.broadcasts[node][msg] {
			s.broadcastMu.Unlock()
			continue
		}
		s.broadcasts[node][msg] = true
		s.broadcastMu.Unlock()

		var handlerMu sync.Mutex
		var handler bool
		go func(node string) {
			for !handler {
				s.n.RPC(node, map[string]any{
					"type":    "broadcast",
					"message": msg,
				}, func(msg maelstrom.Message) error {
					handlerMu.Lock() // New!
					defer handlerMu.Unlock()
					handler = true
					return nil
				})

				time.Sleep(500 * time.Millisecond)
			}
		}(node)
	}
}

func main() {
	s := newServer()

	s.n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if messageValue, ok := body["message"]; ok {
			if messageFloat, ok := messageValue.(float64); ok {
				s.broadcast(int(messageFloat), msg.Src)
			}
		}

		return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	s.n.Handle("read", func(msg maelstrom.Message) error {
		s.messagesMu.RLock()
		messages := make([]int, 0, len(s.messages))
		for msg := range s.messages {
			messages = append(messages, msg)
		}
		s.messagesMu.RUnlock()

		return s.n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	s.n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topologyMap := body["topology"].(map[string]any)
		nodeList := topologyMap[s.n.ID()].([]interface{})

		s.topologyMu.Lock()
		s.topology = make([]string, len(nodeList))
		for i, node := range nodeList {
			s.topology[i] = node.(string)
		}
		s.topologyMu.Unlock()

		return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	s.n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
