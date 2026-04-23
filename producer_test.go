package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProducer(t *testing.T) {
	t.Run("creates a new producer", func(tt *testing.T) {
		p, err := NewProducer()
		require.NoError(tt, err)

		assert.NotNil(tt, p)
	})
}

func TestNewProducerWithOptions(t *testing.T) {
	t.Run("creates a new producer", func(tt *testing.T) {
		p, err := NewProducerWithOptions(&ProducerOptions{})
		require.NoError(tt, err)

		assert.NotNil(tt, p)
	})

	t.Run("allows custom *redis.Client", func(tt *testing.T) {
		rc := newRedisClient(nil)

		p, err := NewProducerWithOptions(&ProducerOptions{
			RedisClient: rc,
		})
		require.NoError(tt, err)

		assert.NotNil(tt, p)
		assert.Equal(tt, rc, p.redis)
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		_, err := NewProducerWithOptions(&ProducerOptions{
			RedisOptions: &RedisOptions{Addr: "localhost:0"},
		})
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}

func TestEnqueue(t *testing.T) {
	t.Run("puts the message in the stream", func(tt *testing.T) {
		p, err := NewProducerWithOptions(&ProducerOptions{})
		require.NoError(t, err)

		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		res, err := p.redis.XRange(context.TODO(), msg.Stream, msg.ID, msg.ID).Result()
		require.NoError(tt, err)
		assert.Equal(tt, "value", res[0].Values["test"])
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		p, err := NewProducerWithOptions(&ProducerOptions{ApproximateMaxLength: true})
		require.NoError(t, err)

		msg := &Message{
			Stream: tt.Name(),
		}
		err = p.Enqueue(msg)
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "wrong number of arguments")
	})

	t.Run("enqueues with StreamRetentionPeriod", func(tt *testing.T) {
		p, err := NewProducerWithOptions(&ProducerOptions{
			StreamRetentionPeriod: 1 * time.Hour,
			ApproximateMaxLength:  true,
		})
		require.NoError(tt, err)

		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		res, err := p.redis.XRange(context.TODO(), msg.Stream, msg.ID, msg.ID).Result()
		require.NoError(tt, err)
		assert.Equal(tt, "value", res[0].Values["test"])
	})

	t.Run("trims old messages with StreamRetentionPeriod", func(tt *testing.T) {
		p, err := NewProducerWithOptions(&ProducerOptions{
			StreamRetentionPeriod: 50 * time.Millisecond,
			ApproximateMaxLength:  false,
		})
		require.NoError(tt, err)

		msg1 := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value1"},
		}
		err = p.Enqueue(msg1)
		require.NoError(tt, err)

		// wait for msg1 to age past the retention period
		time.Sleep(60 * time.Millisecond)

		msg2 := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value2"},
		}
		err = p.Enqueue(msg2)
		require.NoError(tt, err)

		// msg1 should have been trimmed
		res, err := p.redis.XRange(context.TODO(), msg1.Stream, "-", "+").Result()
		require.NoError(tt, err)
		assert.Len(tt, res, 1)
		assert.Equal(tt, msg2.ID, res[0].ID)
	})
}
