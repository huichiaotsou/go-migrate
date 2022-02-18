package utils

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func DropMessageByAddressFunc(db *sqlx.DB) error {
	_, err := db.Exec("DROP FUNCTION IF EXISTS messages_by_address(text[],text[],bigint,bigint);")
	if err != nil {
		return fmt.Errorf("error while dropping messages_by_address function: %s", err)
	}
	return nil
}
