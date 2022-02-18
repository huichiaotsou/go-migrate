package main

import (
	"fmt"
	"log"

	"github.com/huichiaotsou/migrate-go/types"
	"github.com/huichiaotsou/migrate-go/utils"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	cfg := &types.Config{}
	err := types.SetConfig(cfg)
	if err != nil {
		log.Fatal("Error while setting config", err)
	}

	db := utils.GetDB()
	defer db.Close()

	err = utils.AlterTables(db)
	if err != nil {
		log.Fatal("Error while altering tables", err)
	}

	err = utils.CreateTables(db)
	if err != nil {
		log.Fatal("Error while creating tables: ", err)
	}

	limit := cfg.Limit
	offset := int64(0)

	for {
		fmt.Printf("Migrating from row %v to row %v\n", offset, offset+limit)
		txRows, err := selectRows(limit, offset, db)
		if err != nil {
			log.Fatal("error while selecting transaction rows: ", err)
		}
		err = utils.InsertTransactions(txRows, cfg, db)
		if err != nil {
			log.Fatal(err)
		}

		offset += limit
		if len(txRows) == 0 {
			break
		}
	}

	err = utils.DropMessageByAddressFunc(db)
	if err != nil {
		log.Fatal(err)
	}
	err = utils.CreateMessageByAddressFunc(db)
	if err != nil {
		log.Fatal(err)
	}
}

func selectRows(limit int64, offset int64, db *sqlx.DB) ([]types.TransactionRow, error) {
	stmt := fmt.Sprintf("SELECT * FROM transaction_old ORDER BY height LIMIT %v OFFSET %v", limit, offset)
	var txRows []types.TransactionRow
	err := db.Select(&txRows, stmt)
	if err != nil {
		return nil, err
	}

	return txRows, nil
}
