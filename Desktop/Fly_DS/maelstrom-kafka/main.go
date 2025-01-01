package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaServer struct {
	kv *maelstrom.KV
	mu sync.RWMutex
}

func NewKafkaServer(kv *maelstrom.KV) *KafkaServer {
	return &KafkaServer{
		kv: kv,
	}
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)
	server := NewKafkaServer(kv)

	// Handle send RPC
	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := body["msg"]

		offset, err := server.AppendMessage(key, message)
		if err != nil {
			return err
		}

		return node.Reply(msg, map[string]interface{}{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	// Handle poll RPC
	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]interface{})
		result, err := server.PollMessages(offsets)
		if err != nil {
			return err
		}

		return node.Reply(msg, map[string]interface{}{
			"type": "poll_ok",
			"msgs": result,
		})
	})

	// Handle commit_offsets RPC
	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]interface{})
		err := server.CommitOffsets(offsets)
		if err != nil {
			return err
		}

		return node.Reply(msg, map[string]interface{}{
			"type": "commit_offsets_ok",
		})
	})

	// Handle list_committed_offsets RPC
	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]interface{})
		result, err := server.ListCommittedOffsets(keys)
		if err != nil {
			return err
		}

		return node.Reply(msg, map[string]interface{}{
			"type":    "list_committed_offsets_ok",
			"offsets": result,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *KafkaServer) AppendMessage(key string, message interface{}) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offsetKey := fmt.Sprintf("%s_offset", key)
	logKey := fmt.Sprintf("%s_logs", key)

	// Fetch or initialize offset
	var offset int
	rawOffset, err := s.kv.Read(context.Background(), offsetKey)
	if err != nil {
		rawOffset = 0
	}
	if rawOffset != nil {
		offset = int(rawOffset.(int))
	}

	// Store the message at the current offset
	for ; ; offset++ {
		if err := s.kv.CompareAndSwap(context.Background(),
			offsetKey, offset-1, offset, true); err != nil {
			continue
		}
		break
	}

	logEntryKey := fmt.Sprintf("%s_%d", logKey, offset)
	err = s.kv.Write(context.Background(), logEntryKey, message)
	if err != nil {
		return -1, err
	}

	return offset, nil
}

func (s *KafkaServer) PollMessages(offsets map[string]interface{}) (map[string][][]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][][]interface{})
	for key, startOffset := range offsets {
		start := int(startOffset.(float64))
		logKey := fmt.Sprintf("%s_logs", key)

		messages := [][]interface{}{}
		for i := start; ; i++ {
			logEntryKey := fmt.Sprintf("%s_%d", logKey, i)
			rawMessage, err := s.kv.Read(context.Background(), logEntryKey)
			if err != nil {
				break
			}
			messages = append(messages, []interface{}{i, rawMessage})
		}

		result[key] = messages
	}

	return result, nil
}

func (s *KafkaServer) CommitOffsets(offsets map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, offset := range offsets {
		commitKey := fmt.Sprintf("%s_commit", key)
		commitOffset := int(offset.(float64))

		err := s.kv.Write(context.Background(), commitKey, commitOffset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *KafkaServer) ListCommittedOffsets(keys []interface{}) (map[string]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]int)
	for _, key := range keys {
		keyStr := key.(string)
		commitKey := fmt.Sprintf("%s_commit", keyStr)

		rawCommitOffset, err := s.kv.Read(context.Background(), commitKey)
		if err != nil {
			result[keyStr] = 0
			continue
		}

		result[keyStr] = int(rawCommitOffset.(int))
	}

	return result, nil
}
