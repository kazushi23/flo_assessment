package redisUtil

import (
	"context"
	"flo/assessment/config/toml"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var Redis *RedisClient

// RedisClient extends the client and has its own functions
type RedisClient struct {
	*redis.Client
}

// Initialize the Redis client
func NewRedisClient() error {
	if Redis != nil {
		return nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:     toml.GetConfig().Redis.Urls[0],
		Password: toml.GetConfig().Redis.Password,
		DB:       0,
		PoolSize: 10, // Connection pool size
		// Timeouts
		DialTimeout:  5 * time.Second, // Connection establishment timeout, default 5 seconds.
		ReadTimeout:  3 * time.Second, // Read timeout, default 3 seconds, -1 means cancel read timeout
		WriteTimeout: 3 * time.Second, // Write timeout, default equals read timeout
		PoolTimeout:  4 * time.Second, // The maximum wait time for the client to wait for an available connection when all connections are busy, default is read timeout + 1 second.

		// Idle connection checks include IdleTimeout and MaxConnAge
		IdleCheckFrequency: 60 * time.Second, // The cycle for idle connection checks, default is 1 minute, -1 means no periodic checks, only handle idle connections when the client fetches a connection.
		IdleTimeout:        5 * time.Minute,  // Idle timeout, default 5 minutes, -1 means cancel idle timeout check
		MaxConnAge:         0 * time.Second,  // Connection lifespan, starts timing from creation, if exceeds the specified duration, the connection is closed, default is 0, which means do not close connections with long lifespans.

		// Retry strategy for command execution failures
		MaxRetries:      0,                      // Maximum number of retries when command execution fails, default is 0 (no retries)
		MinRetryBackoff: 8 * time.Millisecond,   // Lower limit for calculating retry backoff time, default 8 milliseconds, -1 means cancel backoff
		MaxRetryBackoff: 512 * time.Millisecond, // Upper limit for calculating retry backoff time, default 512 milliseconds, -1 means cancel backoff

		// This hook function is called only when the client needs to fetch a connection from the connection pool when executing commands.
		// If the connection pool needs to create a new connection, this hook function will be called.
		OnConnect: func(ctx context.Context, conn *redis.Conn) error {
			fmt.Printf("Creating a new connection: %v\n", conn)
			return nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	Redis = &RedisClient{client}
	return nil
}

// Initialize the Redis client
func init() {
	fmt.Println("redis init")
	err := NewRedisClient()
	if err != nil {
		fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!failed to connect redis client")
	}
}

func (redis *RedisClient) RSet(key string, value interface{}, ex int) *redis.StatusCmd {
	return redis.Set(context.TODO(), key, value, time.Second*time.Duration(ex))
}

func (redis *RedisClient) RGet(key string) string {
	value, err := redis.Get(context.TODO(), key).Result()
	if err != nil {
		return ""
	}
	return value
}

// Return the expiration time of the key
func (redis *RedisClient) RTTL(key string) int {
	value, err := redis.TTL(context.TODO(), key).Result()
	if err != nil {
		return 0
	}
	return int(value.Seconds() / 1)
}

func (redis *RedisClient) RDel(key string) {
	redis.Del(context.TODO(), key)
}

// Close the Redis client
func (redis *RedisClient) Close() {
	if redis.Client != nil {
		redis.Client.Close()
	}
}

// Get the Redis client; if the client is not initialized
// create the Redis client
func GetRedisClient() (*RedisClient, error) {
	if Redis == nil {
		err := NewRedisClient()
		if err != nil {
			return nil, err
		}
		return Redis, nil
	}
	return Redis, nil
}
