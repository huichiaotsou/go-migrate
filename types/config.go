package types

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func GetEnvConfig(cfg *Config) error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error while loading .env file: %s", err)
	}

	limit, err := strconv.ParseInt(os.Getenv("BATCH"), 10, 64)
	if err != nil {
		return fmt.Errorf("error while converting LIMIT from string to int64: %s", err)
	}
	partitionSize, err := strconv.ParseInt(os.Getenv("PARTITION_SIZE"), 10, 64)
	if err != nil {
		return fmt.Errorf("error while converting PARTITION_SIZE from string to int64: %s", err)
	}

	cfg.Limit = limit
	cfg.PartitionSize = partitionSize
	cfg.PGUSER = os.Getenv("PGUSER")

	return nil
}
