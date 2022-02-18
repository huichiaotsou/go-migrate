package utils

import (
	"github.com/huichiaotsou/migrate-go/types"
)

func MessageParser(msg map[string]interface{}) (addresses string) {
	accountParser := append(types.DefaultAccountParser, types.CustomAccountParser...)

	addresses += "{"
	for _, role := range accountParser {
		if address, ok := msg[role].(string); ok {
			addresses += address + ","
		}
	}

	if input, ok := msg["input"].([]map[string]interface{}); ok {
		for _, i := range input {
			addresses += i["address"].(string) + ","
		}
	}

	if output, ok := msg["output"].([]map[string]interface{}); ok {
		for _, i := range output {
			addresses += i["address"].(string) + ","
		}
	}

	if len(addresses) == 1 {
		return "{}"
	}

	addresses = addresses[:len(addresses)-1] // remove trailing ,
	addresses += "}"

	return addresses
}
