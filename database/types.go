package database

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type DB struct {
	Db   *sql.DB
	Sqlx *sqlx.DB
}
