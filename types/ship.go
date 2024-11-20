package types

import "time"

type PermissionLevel struct {
	Actor      string `json:"actor"`
	Permission string `json:"permission"`
}

type AccountAuthSequence struct {
	Account  string `json:"account"`
	Sequence uint64 `json:"sequence"`
}

type ActionReceipt struct {
	Receiver       string                `json:"receiver"`
	ActDigest      string                `json:"act_digest"`
	GlobalSequence uint64                `json:"global_sequence"`
	RecvSequence   uint64                `json:"recv_sequence"`
	AuthSequence   []AccountAuthSequence `json:"auth_sequence"`
	CodeSequence   uint32                `json:"code_sequence"`
	ABISequence    uint32                `json:"abi_sequence"`
}

type ActionTrace struct {
	ID                   uint64            `json:"id"`
	BlockNum             uint32            `json:"blocknum"`
	Timestamp            time.Time         `json:"blocktimestamp"`
	TxID                 string            `json:"tx_id"`
	ActionOrdinal        uint              `json:"action_ordinal"`
	CreatorActionOrdinal uint              `json:"creator_action_ordinal"`
	Receipt              *ActionReceipt    `json:"receipt,omitempty"`
	Account              string            `json:"account"`
	Name                 string            `json:"name"`
	Receiver             string            `json:"receiver"`
	FirstReceiver        bool              `json:"first_receiver"`
	Data                 interface{}       `json:"data"`
	Authorization        []PermissionLevel `json:"authorization"`
	Elapsed              int64             `json:"elapsed"`
	Console              string            `json:"console"`
	Except               string            `json:"except"`
	Error                uint64            `json:"error"`
	Return               []byte            `json:"return"`
}
