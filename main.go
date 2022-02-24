package main

import (
	"fmt"
	"log"
	"os"

	"github.com/huichiaotsou/migrate-go/database"
	"github.com/huichiaotsou/migrate-go/types"
	_ "github.com/lib/pq"
)

func main() {
	cfg := &types.Config{}
	err := types.GetEnvConfig(cfg)
	if err != nil {
		log.Fatal("Error while setting config", err)
	}

	db := database.GetDB()
	defer db.Sqlx.Close()

	if len(os.Args) < 2 {
		fmt.Println("Usage: 'go run main.go perpare-tables' or 'go run main.go migrate'")
		return
	}

	switch os.Args[1] {
	case "prepare-tables":
		fmt.Println("--- Preparing tables ---")

		// ALTER tables and indexes to add "_old" tags
		err = db.AlterTables()
		if err != nil {
			log.Fatal("Error while altering tables: ", err)
		}

		// CREATE new tables with new indexes
		err = db.CreateTables(cfg)
		if err != nil {
			log.Fatal("Error while creating tables: ", err)
		}

		fmt.Println("--- Preparing tables completed ---")

	case "migrate":
		limit := cfg.Limit
		offset := int64(0)

		for {
			fmt.Printf("--- Migrating data from row %v to row %v --- \n", offset, offset+limit)

			// SELECT rows from transaction_old table
			txRows, err := db.SelectRows(limit, offset)
			if err != nil {
				log.Fatal("error while selecting transaction rows: ", err)
			}
			if len(txRows) == 0 {
				break
			}

			// INSERT INTO transaction and message tables
			err = db.InsertTransactions(txRows, cfg)
			if err != nil {
				log.Fatal("error while inserting data: ", err)
			}

			offset += limit
		}

		// DROP old messages_by_address function
		err = db.DropMessageByAddressFunc()
		if err != nil {
			log.Fatal("error while dropping messages_by_address function: ", err)
		}

		// CREATE new messages_by_address function
		err = db.CreateMessageByAddressFunc()
		if err != nil {
			log.Fatal("error while creating messages_by_address function: ", err)
		}

		fmt.Println("--- Migration completed ---")
	}

}
