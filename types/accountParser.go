package types

var CustomAccountParser = []string{ // for desmos
	"receiver", "user", "counterparty", "blocker", "blocked",
}

var DefaultAccountParser = []string{
	"signer", "sender", "to_address", "from_address", "delegator_address",
	"validator_address", "submitter", "proposer", "depositor", "voter",
	"validator_dst_address", "validator_src_address",
}
