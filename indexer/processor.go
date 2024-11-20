package main

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/blocdraig/oryx/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shufflingpixels/antelope-go/chain"
)

type Processor struct {
	Abi        *chain.Abi
	ChConn     driver.Conn
	PgPool     *pgxpool.Pool
	StatsD     *statsd.Client
	AbiManager *utils.AbiManager
	Timer      *utils.TimeManager
}

// Create a new Processor
func NewProcessor(chConn driver.Conn, pgPool *pgxpool.Pool, statsd *statsd.Client, abiManager *utils.AbiManager, timer *utils.TimeManager) *Processor {
	return &Processor{
		ChConn:     chConn,
		PgPool:     pgPool,
		StatsD:     statsd,
		AbiManager: abiManager,
		Timer:      timer,
	}
}
