package database

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/huichiaotsou/migrate-go/types"
)

func (db *DB) InsertTransactions(txRows []types.TransactionRow, cfg *types.Config) error {
	stmt := `INSERT INTO transaction 
(hash, height, success, messages, memo, signatures, signer_infos, fee, gas_wanted, gas_used, raw_log, logs, partition_id) VALUES 
`
	var params []interface{}
	for i, tx := range txRows {
		// Create transaction partition table if not exists
		partitionID := tx.Height / int64(cfg.PartitionSize)
		err := db.CreatePartitionTable("transaction", partitionID)
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

	_, err := db.Sqlx.Exec(stmt, params...)
	if err != nil {
		return fmt.Errorf("error while inserting transaction: %s", err)
	}

	for _, tx := range txRows {
		// Handle messages of this transaction
		err := db.InsertMessages(tx)
		if err != nil {
			return fmt.Errorf("error while inserting messages: %s", err)
		}
	}

	return nil
}

func (db *DB) InsertMessages(tx types.TransactionRow) error {
	// Create partition table if not exists
	partitionSize, err := strconv.ParseInt(os.Getenv("PARTITION_SIZE"), 10, 64)
	if err != nil {
		return fmt.Errorf("error while parsing partition size to int64 for transaction: %s", err)
	}
	partitionID := tx.Height / partitionSize

	// Create message partition table if not exists
	err = db.CreatePartitionTable("message", partitionID)
	if err != nil {
		return fmt.Errorf("error while creating message partition table: %s", err)
	}

	// Prepare stmt
	stmt := `INSERT INTO message 
(transaction_hash, index, type, value, involved_accounts_addresses, height, partition_id) VALUES `

	// Prepare params
	var params []interface{}

	// Unmarshal messages
	var msgs []map[string]interface{}
	err = json.Unmarshal([]byte(tx.Messages), &msgs)
	if err != nil {
		log.Fatalf("error while unmarshaling messages: %s", err.Error())
	}

	for i, m := range msgs {
		// Append params
		msgType := m["@type"].(string)[1:] // remove head "/"
		involvedAddresses := types.MessageParser(m)
		delete(m, "@type")
		mBz, err := json.Marshal(&m)
		if err != nil {
			return fmt.Errorf("error while marshaling msg value to json: %s", err)
		}
		params = append(params, tx.Hash, i, msgType, string(mBz), involvedAddresses, tx.Height, partitionID)

		// Add columns to stmt
		ai := i * 7
		stmt += fmt.Sprintf("($%v, $%v, $%v, $%v, $%v, $%v, $%v),",
			ai+1, ai+2, ai+3, ai+4, ai+5, ai+6, ai+7)
	}

	stmt = stmt[:len(stmt)-1] // remove trailing ","
	stmt += " ON CONFLICT DO NOTHING"

	_, err = db.Sqlx.Exec(stmt, params...)
	if err != nil {
		return err
	}

	return nil
}
