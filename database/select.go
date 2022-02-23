package database

import (
	"fmt"

	"github.com/huichiaotsou/migrate-go/types"
)

func (db *DB) SelectRows(limit int64, offset int64) ([]types.TransactionRow, error) {
	stmt := fmt.Sprintf("SELECT * FROM transaction_old ORDER BY height LIMIT %v OFFSET %v", limit, offset)
	var txRows []types.TransactionRow
	err := db.Sqlx.Select(&txRows, stmt)
	if err != nil {
		return nil, err
	}

	return txRows, nil
}
