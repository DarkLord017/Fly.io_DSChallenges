package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type txn struct {
	MsgId int     `json:"msg_id"`
	Txn   [][]any `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()
	txnList := make(map[int]int)
	mu := sync.RWMutex{}

	n.Handle("txn", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body txn
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := make([][]any, 0, len(body.Txn))
		mu.Lock()
		for _, txn := range body.Txn {
			op := make([]any, len(txn))
			copy(op, txn)
			if txn[0] == "r" {
				a := int(txn[1].(float64))
				op[2] = txnList[a]
			} else if txn[0] == "w" {
				a := int(txn[1].(float64))
				b := int(txn[2].(float64))
				txnList[a] = b
			}
			res = append(res, op)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  res,
		})

	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
