package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/huichiaotsou/migrate-go/types"
	"github.com/jmoiron/sqlx"
)

func InsertTransactions(txRows []types.TransactionRow, db *sqlx.DB) error {
	stmt := `INSERT INTO transaction 
(hash, height, success, messages, memo, signatures, signer_infos, fee, gas_wanted, gas_used, raw_log, logs, partition_id) VALUES 
`
	var params []interface{}
	for i, tx := range txRows {
		// Create partition table if not exists
		partitionSize, err := strconv.ParseInt(os.Getenv("PARTITION_SIZE"), 10, 64)
		if err != nil {
			return fmt.Errorf("error while parsing partition size to int64 for transaction: %s", err)
		}
		partitionID := tx.Height / partitionSize
		err = CreatePartitionTable("transaction", partitionID, db)
		if err != nil {
			return fmt.Errorf("error while creating transaction partition table: %s", err)
		}

		// Append params
		params = append(params, tx.Hash, tx.Height, tx.Success, tx.Messages, tx.Memo, tx.Signatures,
			tx.SignerInfos, tx.Fee, tx.GasWanted, tx.GasUsed, tx.RawLog, tx.Logs, partitionID)

		// Add columns to stmt
		ai := i * 13
		stmt += fmt.Sprintf("($%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v),",
			ai+1, ai+2, ai+3, ai+4, ai+5, ai+6, ai+7, ai+8, ai+9, ai+10, ai+11, ai+12, ai+13)
	}
	stmt = stmt[:len(stmt)-1] // remove trailing ,
	stmt += " ON CONFLICT DO NOTHING"

	_, err := db.Exec(stmt, params...)
	if err != nil {
		return err
	}

	for _, tx := range txRows {
		// Handle messages of this transaction
		err := InsertMessages(tx, db)
		if err != nil {
			return fmt.Errorf("error while inserting messages: %s", err)
		}
	}

	return nil
}

func InsertMessages(tx types.TransactionRow, db *sqlx.DB) error {
	// Create partition table if not exists
	partitionSize, err := strconv.ParseInt(os.Getenv("PARTITION_SIZE"), 10, 64)
	if err != nil {
		return fmt.Errorf("error while parsing partition size to int64 for transaction: %s", err)
	}
	partitionID := tx.Height / partitionSize
	err = CreatePartitionTable("message", partitionID, db)
	if err != nil {
		return fmt.Errorf("error while creating message partition table: %s", err)
	}

	// Prepare stmt
	stmt := `INSERT INTO messages 
(hash, index, type, value, involved_accounts_addresses, height, partition_id) VALUES `

	// Prepare params
	var params []interface{}

	// Unmarshal messages
	var msgs []map[string]interface{}
	err = json.Unmarshal([]byte(tx.Messages), &msgs)
	if err != nil {
		log.Fatalf("error while unmarshaling messages: %s", err.Error())
	}

	for i, msg := range msgs {
		// Append params
		msgType := msg["@type"].(string)
		involvedAddresses := MessageParser(msg)
		delete(msg, "@type")
		params = append(params, tx.Hash, i, msgType, msg, involvedAddresses, tx.Height, partitionID)

		// Add columns to stmt
		ai := i * 7
		stmt += fmt.Sprintf("($%v, $%v, $%v, $%v, $%v, $%v, $%v),",
			ai+1, ai+2, ai+3, ai+4, ai+5, ai+6, ai+7)
	}

	stmt = stmt[:len(stmt)-1] // remove trailing ,
	stmt += " ON CONFLICT DO NOTHING"

	_, err = db.Exec(stmt, params...)
	if err != nil {
		return err
	}

	return nil
}
