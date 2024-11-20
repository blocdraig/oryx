SET allow_experimental_json_type = 1;

CREATE TABLE IF NOT EXISTS blocks (
    id UInt32,
    block_hash String,
    parent_hash String,
    block_time UInt64,
    tx_count UInt64,
    ax_count UInt64
) ENGINE = MergeTree()
ORDER BY id;

CREATE INDEX IF NOT EXISTS blocks_tx_count_idx ON blocks (tx_count) TYPE minmax GRANULARITY 1;
CREATE INDEX IF NOT EXISTS blocks_ax_count_idx ON blocks (ax_count) TYPE minmax GRANULARITY 1;


CREATE TABLE IF NOT EXISTS actions (
    id UInt64,
    block_num UInt64,
    block_timestamp DateTime,
    tx_id String,
    action_ordinal UInt64,
    creator_action_ordinal UInt64,
    receipt_receiver String,
    receipt_act_digest String,
    receipt_global_sequence UInt64,
    receipt_recv_sequence UInt64,
    receipt_auth_sequence Array(Tuple(String, UInt64)),
    receipt_code_sequence UInt32,
    receipt_abi_sequence UInt32,
    account String,
    name String,
    receiver String,
    first_receiver UInt8,
    data JSON,
    authorization Array(Tuple(String, String)),
    console Nullable(String),
    except Nullable(String),
    error Nullable(UInt64),
    return Nullable(String)
) ENGINE = MergeTree()
ORDER BY id;