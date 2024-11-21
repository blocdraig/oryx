package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/blocdraig/oryx/types"
	"github.com/blocdraig/oryx/utils"
	shipclient "github.com/eosswedenorg-go/antelope-ship-client"
	"github.com/golang-migrate/migrate/v4"
	chxm "github.com/golang-migrate/migrate/v4/database/clickhouse"
	pgxm "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/shufflingpixels/antelope-go/api"
	"github.com/shufflingpixels/antelope-go/chain"
	"github.com/shufflingpixels/antelope-go/ship"
)

func updateAbiFromAction(abiManager *utils.AbiManager, act *chain.Action) error {
	set_abi := struct {
		Account chain.Name
		Abi     chain.Bytes
	}{}

	if err := act.DecodeInto(&set_abi); err != nil {
		return err
	}

	abi := chain.Abi{}
	decoder := chain.NewDecoder(bytes.NewReader(set_abi.Abi))
	if err := decoder.Decode(&abi); err != nil {
		return err
	}

	return abiManager.SetAbi(set_abi.Account, &abi)
}

func migrateDatabase(pgUrl string, chUrl string) error {
	log.Println("Migrate database")

	// Postgres migration
	db, err := sql.Open("pgx", pgUrl)
	if err != nil {
		log.Fatalln("Migrate PG open error: ", err)
		return err
	}
	pgDriver, err := pgxm.WithInstance(db, &pgxm.Config{})
	if err != nil {
		log.Fatalln("Migrate PG instance error: ", err)
		return err
	}
	defer func() {
		if err := pgDriver.Close(); err != nil {
			log.Fatalln("Migrate PG close error: ", err)
		}
	}()
	m, err := migrate.NewWithDatabaseInstance(
		"file://./migrations/postgres",
		"postgres", pgDriver)
	if err != nil {
		log.Fatalln("Migrate PG migrate error: ", err)
		return err
	}
	if err := m.Up(); err != nil {
		if err != migrate.ErrNoChange {
			log.Fatalln("Migrate PG up error: ", err)
		}
	}

	// ClickHouse migration
	chInstance := &chxm.ClickHouse{}
	chDriver, err := chInstance.Open(chUrl)
	if err != nil {
		log.Fatalln("Migrate CH open error: ", err)
		return err
	}
	defer func() {
		if err := chDriver.Close(); err != nil {
			log.Fatalln("Migrate CH close error: ", err)
		}
	}()
	m, err = migrate.NewWithDatabaseInstance(
		"file://./migrations/clickhouse",
		"clickhouse", chDriver)
	if err != nil {
		log.Fatalln("Migrate CH migrate error: ", err)
		return err
	}
	if err := m.Up(); err != nil {
		if err != migrate.ErrNoChange {
			log.Fatalln("Migrate CH up error: ", err)
		}
	}

	return nil
}

func (processor *Processor) processAction(blockNum uint32, block *ship.SignedBlock, trace *ship.TransactionTraceV0, action *ship.ActionTraceV1, ABI *chain.Abi) {
	act := &types.ActionTrace{
		ID:                   action.Receipt.V0.GlobalSequence,
		BlockNum:             blockNum,
		Timestamp:            block.BlockHeader.Timestamp.Time().UTC(),
		TxID:                 trace.ID.String(),
		ActionOrdinal:        action.ActionOrdinal,
		CreatorActionOrdinal: action.CreatorActionOrdinal,
		Account:              action.Act.Account.String(),
		Name:                 action.Act.Name.String(),
		Receiver:             action.Receiver.String(),
		FirstReceiver:        action.Act.Account.String() == action.Receiver.String(),
	}

	if action.Receipt != nil {
		receipt := action.Receipt.V0
		act.Receipt = &types.ActionReceipt{
			Receiver:       receipt.Receiver.String(),
			ActDigest:      receipt.ActDigest.String(),
			GlobalSequence: receipt.GlobalSequence,
			RecvSequence:   receipt.RecvSequence,
			CodeSequence:   uint32(receipt.CodeSequence),
			ABISequence:    uint32(receipt.ABISequence),
		}

		for _, auth := range receipt.AuthSequence {
			act.Receipt.AuthSequence = append(act.Receipt.AuthSequence, types.AccountAuthSequence{
				Account:  auth.Account.String(),
				Sequence: auth.Sequence,
			})
		}
	}

	for _, auth := range action.Act.Authorization {
		act.Authorization = append(act.Authorization, types.PermissionLevel{
			Actor:      auth.Actor.String(),
			Permission: auth.Permission.String(),
		})
	}

	data, err := action.Act.Decode(ABI)
	if err != nil {
		log.Println("Failed to decode action: ", err)
	}
	act.Data = data

	authSequenceArray := make([]string, len(act.Receipt.AuthSequence))
	for i, auth := range act.Receipt.AuthSequence {
		authSequenceArray[i] = fmt.Sprintf("('%s', %d)", auth.Account, auth.Sequence)
	}

	authorizationArray := make([]string, len(act.Authorization))
	for i, perm := range act.Authorization {
		authorizationArray[i] = fmt.Sprintf("('%s', '%s')", perm.Actor, perm.Permission)
	}

	dataJson, err := json.Marshal(act.Data)
	if err != nil {
		log.Fatalf("Failed to serialize Data field: %v", err)
	}

	err = processor.ChConn.Exec(
		context.Background(),
		"INSERT INTO actions (id, block_num, block_timestamp, tx_id, action_ordinal, creator_action_ordinal, receipt_receiver, receipt_act_digest, receipt_global_sequence, receipt_recv_sequence, receipt_auth_sequence, receipt_code_sequence, receipt_abi_sequence, account, name, receiver, first_receiver, data, authorization, except, error, return) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		act.ID,
		act.BlockNum,
		act.Timestamp,
		act.TxID,
		act.ActionOrdinal,
		act.CreatorActionOrdinal,
		act.Receipt.Receiver,
		act.Receipt.ActDigest,
		act.Receipt.GlobalSequence,
		act.Receipt.RecvSequence,
		authSequenceArray,
		act.Receipt.CodeSequence,
		act.Receipt.ABISequence,
		act.Account,
		act.Name,
		act.Receiver,
		act.FirstReceiver,
		string(dataJson),
		authorizationArray,
		act.Except,
		act.Error,
		act.Return,
	)
	if err != nil {
		log.Fatalf("unable to execute query: %v\n", err)
	}
}

func (processor *Processor) InitHandler(abi *chain.Abi) {
	processor.Abi = abi
	log.Println("Server abi: ", abi.Version)
}

func (processor *Processor) ProcessBlock(block *ship.GetBlocksResultV0) {
	signedBlock := ship.SignedBlock{}
	block.Block.Unpack(&signedBlock)

	// TODO: Implement fork handling

	processor.StatsD.Gauge("current_block", float64(block.ThisBlock.BlockNum), nil, 1)
	processor.StatsD.Gauge("head_block", float64(block.Head.BlockNum), nil, 1)

	if block.ThisBlock.BlockNum%1000 == 0 {
		// fmt.Print("\033[H\033[2J")
		fmt.Printf("Current: %d, Head: %d\n", block.ThisBlock.BlockNum, block.Head.BlockNum)

		// print time behind to console
		remainingTime := processor.Timer.CalculateTimeBehind(block.ThisBlock.BlockNum, block.Head.BlockNum)
		fmt.Printf("Blocks behind: %d (%s)\n", block.Head.BlockNum-block.ThisBlock.BlockNum, remainingTime)
	}

	// calculate blocks per second every 1000 blocks
	if block.ThisBlock.BlockNum%1000 == 0 {
		processor.Timer.EndTimer(block.ThisBlock.BlockNum)
		bps := processor.Timer.CalculateBPS()
		if bps > 0 {
			processor.StatsD.Gauge("blocks_per_second", float64(bps), nil, 1)
			fmt.Printf("Blocks per second: %d\n", bps)
			// print remaining time to console
			remainingTime := processor.Timer.CalculateRemainingTime(block.ThisBlock.BlockNum, block.Head.BlockNum, bps)
			fmt.Printf("Estimated time to sync: %s\n", remainingTime)
		}
		processor.Timer.StartTimer(block.ThisBlock.BlockNum)
	}

	var txCount uint32 = 0
	var axCount uint32 = 0

	// Process the block traces
	if block.Traces != nil {
		unpackedTraces := []ship.TransactionTrace{}
		if err := block.Traces.Unpack(&unpackedTraces); err != nil {
			log.Fatalf("Failed to unpack traces: %s\n", err)
		} else {
			txCount = uint32(len(unpackedTraces) - 1)
			var blockPosition uint32 = 0
			for _, trace := range unpackedTraces {
				blockPosition++
				var traceAxCount uint32 = uint32(len(trace.V0.ActionTraces) - 1)
				axCount += traceAxCount
				var fistGlobalSequence uint64 = 0
				// Actions
				for _, actionTraceVar := range trace.V0.ActionTraces {
					var act_trace *ship.ActionTraceV1
					if actionTraceVar.V0 != nil {
						// convert to v1
						act_trace = &ship.ActionTraceV1{
							ActionOrdinal:        actionTraceVar.V0.ActionOrdinal,
							CreatorActionOrdinal: actionTraceVar.V0.CreatorActionOrdinal,
							Receipt:              actionTraceVar.V0.Receipt,
							Receiver:             actionTraceVar.V0.Receiver,
							Act:                  actionTraceVar.V0.Act,
							ContextFree:          actionTraceVar.V0.ContextFree,
							Elapsed:              actionTraceVar.V0.Elapsed,
							Console:              actionTraceVar.V0.Console,
							AccountRamDeltas:     actionTraceVar.V0.AccountRamDeltas,
							Except:               actionTraceVar.V0.Except,
							ErrorCode:            actionTraceVar.V0.ErrorCode,
							ReturnValue:          []byte{},
						}
					} else {
						act_trace = actionTraceVar.V1
					}

					if act_trace.Receipt != nil && fistGlobalSequence == 0 {
						fistGlobalSequence = act_trace.Receipt.V0.GlobalSequence
					}

					// Check if this is a setabi action
					if act_trace.Act.Account == chain.N("eosio") && act_trace.Act.Name == chain.N("setabi") {
						err := updateAbiFromAction(processor.AbiManager, &act_trace.Act)
						if err != nil {
							log.Fatalln("Failed to update abi", err)
						}
					}

					// log.Println("Process:", act_trace.Act.Account.String()+"::"+act_trace.Act.Name.String())
					ABI, err := processor.AbiManager.GetAbi(act_trace.Act.Account)
					if err == nil {
						// // START Temporary debug
						// var dataRaw interface{}
						// if err = decode(abi, act_trace.Act, &dataRaw); err != nil {
						// 	log.Fatalln("Failed to decode action", err)
						// }
						// dataJson, err := json.MarshalIndent(dataRaw, "", "  ")
						// if err != nil {
						// 	log.Fatalf("Marshaling error: %s\n", err)
						// }
						// fmt.Println(string(dataJson))
						// // END Temporary debug

						processor.processAction(block.ThisBlock.BlockNum, &signedBlock, trace.V0, act_trace, ABI)
					} else {
						log.Fatalln("Failed to get abi for "+act_trace.Act.Account.String(), err)
					}
				}

				err := processor.ChConn.Exec(
					context.Background(),
					"INSERT INTO transactions (tx_id, block_num, block_timestamp, sequence, block_position, ax_count, cpu_usage_us, net_usage) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
					trace.V0.ID.String(),
					block.ThisBlock.BlockNum,
					signedBlock.Timestamp.Time().UnixMilli(),
					fistGlobalSequence,
					blockPosition,
					traceAxCount,
					trace.V0.CPUUsageUS,
					trace.V0.NetUsage,
				)
				if err != nil {
					log.Fatalf("unable to execute query: %v\n", err)
				}
			}
		}
	}

	var parentHash string = ""
	if block.PrevBlock != nil {
		parentHash = block.PrevBlock.BlockID.String()
	}

	// log.Println("Block:", block.ThisBlock.BlockNum, "Parent:", parentHash, "Tx:", txCount, "Ax:", axCount)

	err := processor.ChConn.Exec(
		context.Background(),
		"INSERT INTO blocks (id, block_hash, parent_hash, block_timestamp, producer, tx_count, ax_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
		block.ThisBlock.BlockNum,
		block.ThisBlock.BlockID.String(),
		parentHash,
		signedBlock.Timestamp.Time().UnixMilli(),
		signedBlock.Producer.String(),
		txCount,
		axCount,
	)
	if err != nil {
		log.Fatalf("unable to execute query: %v\n", err)
	}
}

func processStatus(status *ship.GetStatusResultV0) {
	log.Println("-- Status START --")
	log.Println("Head", status.Head.BlockNum, status.Head.BlockID)
	log.Println("ChainStateBeginBlock", status.ChainStateBeginBlock, "ChainStateEndBlock", status.ChainStateEndBlock)
	log.Println("-- Status END --")
}

func main() {
	var startBlock uint32 = 1

	// Load environment variables
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalln("Error loading .env file", err)
	}

	// Settings
	var startFresh bool = os.Getenv("START_FRESH") == "true"

	// ClickHouse connection
	var chHost string = os.Getenv("CLICKHOUSE_HOST")
	var chPort string = os.Getenv("CLICKHOUSE_PORT")
	var chUser string = os.Getenv("CLICKHOUSE_USER")
	var chPassword string = os.Getenv("CLICKHOUSE_PASSWORD")
	var chDatabase string = os.Getenv("CLICKHOUSE_DATABASE")
	var chUrl string = fmt.Sprintf("clickhouse://%v:%v@%v:%v/%v?x-multi-statement=true&debug=false",
		chUser, chPassword, chHost, chPort, chDatabase)

	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chHost + ":" + chPort},
		Auth: clickhouse.Auth{
			Database: chDatabase,
			Username: chUser,
			Password: chPassword,
		},
		Debug: false,
	})
	if err != nil {
		log.Fatalln("Unable to connect to ClickHouse", err)
	}
	defer chConn.Close()

	// Postgres connection
	var pgHost string = os.Getenv("PG_HOST")
	var pgPort string = os.Getenv("PG_PORT")
	var pgUser string = os.Getenv("PG_USER")
	var pgPassword string = os.Getenv("PG_PASSWORD")
	var pgDatabase string = os.Getenv("PG_DATABASE")
	var pgUrl string = fmt.Sprintf("postgres://%v:%v@%v:%v/%v",
		pgUser, pgPassword, pgHost, pgPort, pgDatabase)

	pgPool, err := pgxpool.New(context.Background(), pgUrl)
	if err != nil {
		log.Fatalln("Unable to connect to Postgres", err)
	}
	defer pgPool.Close()

	// Get starting block from database
	var newStartBlock uint32
	if startFresh {
		chConn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+chDatabase)
		chConn.Exec(context.Background(), "CREATE DATABASE "+chDatabase)
		migrateDatabase(pgUrl, chUrl)
	} else {
		row := chConn.QueryRow(context.Background(), "SELECT max(id) FROM blocks")
		err = row.Scan(&newStartBlock)
		if err != nil {
			if err != pgx.ErrNoRows {
				chConn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+chDatabase)
				chConn.Exec(context.Background(), "CREATE DATABASE "+chDatabase)
				migrateDatabase(pgUrl, chUrl)
			}
		}
	}

	log.Println("Starting block:", newStartBlock)

	if newStartBlock > 0 {
		startBlock = newStartBlock + 1
	}

	// Redis connection
	var redisHost string = os.Getenv("REDIS_HOST")
	var redisPort string = os.Getenv("REDIS_PORT")
	var redisDatabase int = 0
	if os.Getenv("REDIS_DATABASE") != "" {
		redisDatabase, err = strconv.Atoi(os.Getenv("REDIS_DATABASE"))
		if err != nil {
			redisDatabase = 0
		}
	}
	var redisServer string = redisHost + ":" + redisPort
	var redisClient = redis.NewClient(&redis.Options{
		Addr: redisServer,
		DB:   redisDatabase,
	})
	redisClient.FlushAll(context.Background())

	// StatsD connection
	var statsdHost string = os.Getenv("STATSD_HOST")
	var statsdPort string = os.Getenv("STATSD_PORT")
	var statsdServer string = statsdHost + ":" + statsdPort
	statsd, err := statsd.New(statsdServer)
	if err != nil {
		log.Fatalln("StatsD:", err)
	}

	// Antelope client
	var apiUrl string = os.Getenv("API_URL")
	var apiClient = api.New(apiUrl)
	_, err = apiClient.GetInfo(context.Background())
	if err != nil {
		log.Fatalln("Failed to get info:", err)
	}

	// ABI Manager
	var abiManager = utils.NewAbiManager(redisClient, apiClient)

	// Processor
	var processor = NewProcessor(chConn, pgPool, statsd, abiManager, utils.NewTimeManager())

	// SHIP client
	var shipUrl string = os.Getenv("SHIP_URL")

	log.Printf("Connecting to ship starting at block: %d\n", startBlock)

	stream := shipclient.NewStream(shipclient.WithStartBlock(startBlock), shipclient.WithMaxMessagesInFlight(1)) //, shipclient.WithEndBlock(startBlock+100)
	stream.InitHandler = processor.InitHandler
	stream.BlockHandler = processor.ProcessBlock
	stream.StatusHandler = processStatus
	stream.TraceHandler = func(*ship.TransactionTraceArray) {}
	stream.TableDeltaHandler = func(*ship.TableDeltaArray) {}

	// Connect to SHIP client
	err = stream.Connect(shipUrl)
	if err != nil {
		log.Fatalln(err)
	}

	// Request streaming of blocks from ship
	err = stream.SendBlocksRequest()
	if err != nil {
		log.Fatalln(err)
	}

	err = stream.SendStatusRequest()
	if err != nil {
		log.Fatalln(err)
	}

	// Spawn message read loop in another thread.
	go func() {
		// Create interrupt channels.
		interrupt := make(chan os.Signal, 1)

		// Register interrupt channel to receive interrupt messages
		signal.Notify(interrupt, os.Interrupt)

		// Enter event loop in main thread
		//lint:ignore S1000 This works as expected
		for {
			select {
			case <-interrupt:
				log.Println("Interrupt, closing")

				// Cleanly close the connection by sending a close message and then
				// waiting (with timeout) for the server to close the connection.
				err := stream.Shutdown()
				if err != nil {
					log.Println("Failed to close stream: ", err)
				}
				return
			}
		}
	}()

	err = stream.Run()
	log.Println(err)
}
