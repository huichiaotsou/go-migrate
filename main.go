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
		fmt.Println("Missing argument: perpare-tables or migrate")
		return
	}

	switch os.Args[1] {
	case "prepare-tables":
		fmt.Println("--- Preparing tables ---")
		err = db.AlterTables()
		if err != nil {
			log.Fatal("Error while altering tables: ", err)
		}
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
			txRows, err := db.SelectRows(limit, offset)
			if len(txRows) == 0 {
				break
			}

			if err != nil {
				log.Fatal("error while selecting transaction rows: ", err)
			}
			err = db.InsertTransactions(txRows, cfg)
			if err != nil {
				log.Fatal("error while inserting data: ", err)
			}

			offset += limit
		}

		err = db.DropMessageByAddressFunc()
		if err != nil {
			log.Fatal("error while dropping message by address function: ", err)
		}
		err = db.CreateMessageByAddressFunc()
		if err != nil {
			log.Fatal("error while creating message by address function: ", err)
		}
		fmt.Println("--- Migration completed ---")
	}

}
