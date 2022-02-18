package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/huichaiotsou/go-migrate/types"
	"github.com/huichaiotsou/go-migrate/utils"
	_ "github.com/lib/pq"
)

func main() {
	db := utils.GetDB()

	var txRows []types.TransactionRow
	err := db.Select(&txRows, "SELECT * FROM transaction LIMIT 40")
	if err != nil {
		log.Fatal("Error while selecting transaction rows: ", err)
	}

	for _, txRow := range txRows {
		// Handle transaction

		// Handle message
		var msgs []map[string]interface{}
		err = json.Unmarshal([]byte(txRow.Messages), &msgs)
		if err != nil {
			log.Fatalf("error while unmarshaling messages: ", err.Error())
		}
		for _, msg := range msgs {
			involvedAddresses := utils.MessageParser(msg)
			delete(msg, "@type")
			fmt.Println(involvedAddresses)
		}
	}
}
