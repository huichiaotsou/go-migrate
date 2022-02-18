package utils

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func GetDB() *sqlx.DB {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s",
		os.Getenv("PGHOST"), os.Getenv("PGPORT"), os.Getenv("PGDATABASE"), os.Getenv("PGUSER"), os.Getenv("PGPASSWORD"),
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error while connecting psql DB: ", err)
	}
	return sqlx.NewDb(db, "postgresql")
}