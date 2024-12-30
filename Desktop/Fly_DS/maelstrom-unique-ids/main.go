package main

import (
    "encoding/json"
    "log"

    "github.com/google/uuid"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()

    n.Handle("generate", func(msg maelstrom.Message) error {
        var body struct {
            Type string `json:"type"`
        }

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        return n.Reply(msg, struct {
            Type string `json:"type"`
            ID   string `json:"id"`
        }{
            Type: "generate_ok",
            ID:   uuid.NewString(),
        })
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
