package utils

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func CreateTables(db *sqlx.DB) error {
	err := createTxTable(db)
	if err != nil {
		return fmt.Errorf("error while creating transaction table: %s", err)
	}

	err = createMsgTable(db)
	if err != nil {
		return fmt.Errorf("error while creating messaage table: %s", err)
	}

	return nil
}

func createTxTable(db *sqlx.DB) error {
	_, err := db.Exec(`CREATE TABLE transaction
	(
		hash         TEXT    NOT NULL,
		height       BIGINT  NOT NULL REFERENCES block (height),
		success      BOOLEAN NOT NULL,

		/* Body */
		messages     JSONB   NOT NULL DEFAULT '[]'::JSONB,
		memo         TEXT,
		signatures   TEXT[]  NOT NULL,

		/* AuthInfo */
		signer_infos JSONB   NOT NULL DEFAULT '[]'::JSONB,
		fee          JSONB   NOT NULL DEFAULT '{}'::JSONB,

		/* Tx response */
		gas_wanted   BIGINT           DEFAULT 0,
		gas_used     BIGINT           DEFAULT 0,
		raw_log      TEXT,
		logs         JSONB,

		/* Psql partition */
		partition_id BIGINT NOT NULL,
		UNIQUE (hash, partition_id)
	)PARTITION BY LIST(partition_id);
	CREATE INDEX transaction_hash_index ON transaction (hash);
	CREATE INDEX transaction_height_index ON transaction (height);
	CREATE INDEX transaction_partition_id_index ON transaction (partition_id);
	GRANT ALL PRIVILEGES ON transaction TO forbole;`)

	if err != nil {
		return err
	}

	return nil
}

func createMsgTable(db *sqlx.DB) error {
	_, err := db.Exec(`CREATE TABLE message
    (
          transaction_hash            TEXT   NOT NULL,
          index                       BIGINT NOT NULL,
          type                        TEXT   NOT NULL,
          value                       JSONB  NOT NULL,
          involved_accounts_addresses TEXT[] NOT NULL,
  
          /* Psql partition */
          partition_id                BIGINT NOT NULL,
          height                      BIGINT NOT NULL,
          FOREIGN KEY (transaction_hash, partition_id) REFERENCES transaction (hash, partition_id)
      )PARTITION BY LIST(partition_id);
      CREATE INDEX message_transaction_hash_index ON message (transaction_hash);
      CREATE INDEX message_type_index ON message (type);
      CREATE INDEX message_involved_accounts_index ON message (involved_accounts_addresses);
      GRANT ALL PRIVILEGES ON message TO forbole;`)

	if err != nil {
		return err
	}

	return nil
}

func CreatePartitionTable(table string, partitionID int64, db *sqlx.DB) error {
	partitionTable := fmt.Sprintf("%s_%v", table, partitionID)
	stmt := `CREATE TABLE IF NOT EXISTS $1 PARTITION OF transaction FOR VALUES IN ($2)`
	_, err := db.Exec(stmt, partitionTable, partitionID)
	if err != nil {
		return fmt.Errorf("error while creating %s partition table: %s", table, err)
	}
	fmt.Println("partitionTable: ", partitionTable)
	fmt.Println("stmt", stmt)
	return nil
}

func CreateMessageByAddressFunc(db *sqlx.DB) error {
	_, err := db.Exec(`CREATE FUNCTION messages_by_address(
		addresses TEXT [],
		types TEXT [],
		"limit" BIGINT = 100,
		"offset" BIGINT = 0
	  ) RETURNS SETOF message AS $$
	  SELECT
		  message.transaction_hash,
		  message.index,
		  message.type,
		  message.value,
		  message.involved_accounts_addresses,
		  message.partition_id,
		  message.height
	  FROM
		  message
	  WHERE
		  ( cardinality(types) = 0  OR type = ANY (types))
		  AND involved_accounts_addresses && addresses
	  ORDER BY
		  height DESC,
		  involved_accounts_addresses
	  LIMIT
		  "limit" OFFSET "offset" $$ LANGUAGE sql STABLE;`)

	if err != nil {
		return fmt.Errorf("error while creating messages_by_address function: %s", err)
	}

	return nil
}
