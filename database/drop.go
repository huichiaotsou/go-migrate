package database

import (
	"fmt"
)

func (db *DB) DropMessageByAddressFunc() error {
	_, err := db.Sqlx.Exec("DROP FUNCTION IF EXISTS messages_by_address(text[],text[],bigint,bigint);")
	if err != nil {
		return fmt.Errorf("error while dropping messages_by_address function: %s", err)
	}
	return nil
}
