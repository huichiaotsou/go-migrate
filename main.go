package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/huichiaotsou/migrate-go/types"
	"github.com/huichiaotsou/migrate-go/utils"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
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

	txRows, err := selectRows(db)
	if err != nil {
		log.Fatalf("error while selecting transaction rows: %s", err)
	}

	err = utils.InsertTransactions(txRows, db)
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

func selectRows(db *sqlx.DB) ([]types.TransactionRow, error) {
	limit, err := strconv.ParseInt(os.Getenv("BATCH"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error while converting LIMIT from string to int64: %s", err)
	}
	partitionSize, err := strconv.ParseInt(os.Getenv("PARTITION_SIZE"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error while converting PARTITION_SIZE from string to int64: %s", err)
	}

	stmt := fmt.Sprintf("SELECT * FROM transaction ORDER BY height LIMIT %v OFFSET %v", limit, partitionSize)
	var txRows []types.TransactionRow
	err = db.Select(&txRows, stmt)
	if err != nil {
		return nil, err
	}

	return txRows, nil
}
