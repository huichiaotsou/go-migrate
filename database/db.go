package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/huichiaotsou/migrate-go/types"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func GetDB() *DB {
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s",
		os.Getenv("PGHOST"), os.Getenv("PGPORT"), os.Getenv("PGDATABASE"), os.Getenv("PGUSER"), os.Getenv("PGPASSWORD"),
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error while connecting psql DB: ", err)
	}
	return &DB{
		Db:   db,
		Sqlx: sqlx.NewDb(db, "postgresql"),
	}
}

type DB struct {
	Db   *sql.DB
	Sqlx *sqlx.DB
}

func (db *DB) SelectRows(limit int64, offset int64) ([]types.TransactionRow, error) {
	stmt := fmt.Sprintf("SELECT * FROM transaction_old ORDER BY height LIMIT %v OFFSET %v", limit, offset)
	var txRows []types.TransactionRow
	err := db.Sqlx.Select(&txRows, stmt)
	if err != nil {
		return nil, err
	}

	return txRows, nil
}
