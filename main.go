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
		log.Fatal("Error while setting config")
	}

	db := utils.GetDB()
	defer db.Close()

	err = utils.AlterTables(db)
	if err != nil {
		log.Fatal("Error while altering tables")
	}

	err = utils.CreateTables(db)
	if err != nil {
		log.Fatal("Error while creating tables")
	}

	txRows, err := selectRows(cfg.Limit, cfg.PartitionSize, db)
	if err != nil {
		log.Fatalf("error while selecting transaction rows: %s", err)
	}

	err = utils.InsertTransactions(txRows, cfg, db)
	if err != nil {
		log.Fatal(err)
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

func selectRows(limit int64, partitionSize int64, db *sqlx.DB) ([]types.TransactionRow, error) {
	stmt := fmt.Sprintf("SELECT * FROM transaction ORDER BY height LIMIT %v OFFSET %v", limit, partitionSize)
	var txRows []types.TransactionRow
	err := db.Select(&txRows, stmt)
	if err != nil {
		return nil, err
	}

	return txRows, nil
}
