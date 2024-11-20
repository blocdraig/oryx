package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/cache/v9"
	"github.com/redis/go-redis/v9"
	"github.com/shufflingpixels/antelope-go/api"
	"github.com/shufflingpixels/antelope-go/chain"
)

type AbiManager struct {
	Redis *cache.Cache
	Api   *api.Client
	Ctx   context.Context
}

func NewAbiManager(redis *redis.Client, api *api.Client) *AbiManager {
	return &AbiManager{
		Redis: cache.New(&cache.Options{
			Redis:        redis,
			StatsEnabled: false,
			LocalCache:   cache.NewTinyLFU(1000, time.Minute),
		}),
		Api: api,
		Ctx: context.Background(),
	}
}

// Set an ABI in redis
func (abiManager *AbiManager) SetAbi(account chain.Name, abi *chain.Abi) error {
	ctx, cancel := context.WithTimeout(abiManager.Ctx, time.Millisecond*500)
	defer cancel()
	return abiManager.Redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   account.String(),
		Value: *abi,
		TTL:   0,
	})
}

// Get an ABI from redis or the API
func (abiManager *AbiManager) GetAbi(account chain.Name) (*chain.Abi, error) {
	var abi chain.Abi
	err := abiManager.GetAbiFromRedis(account, &abi)
	if err != nil {
		ctx, cancel := context.WithTimeout(abiManager.Ctx, time.Second)
		defer cancel()
		resp, err := abiManager.Api.GetAbi(ctx, account.String())
		if err != nil {
			return nil, fmt.Errorf("AbiManager API: %s", err)
		}
		abi = resp.Abi
		err = abiManager.SetAbi(account, &abi)
		if err != nil {
			return nil, fmt.Errorf("AbiManager SetAbi: %s", err)
		}
	}
	return &abi, nil
}

func (abiManager *AbiManager) GetAbiFromRedis(account chain.Name, value any) error {
	ctx, cancel := context.WithTimeout(abiManager.Ctx, time.Millisecond*500)
	defer cancel()
	err := abiManager.Redis.Get(ctx, account.String(), value)
	if err != nil {
		return fmt.Errorf("AbiManager Redis: %s", err)
	}
	return nil
}
