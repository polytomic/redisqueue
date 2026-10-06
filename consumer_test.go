package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type commandHook struct {
	process   func(redis.Cmder)
	intercept func(context.Context, redis.Cmder, redis.ProcessHook) error
}

func (h commandHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h commandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.process != nil {
			h.process(cmd)
		}
		if h.intercept != nil {
			return h.intercept(ctx, cmd, next)
		}
		return next(ctx, cmd)
	}
}

func (h commandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type redisError string

func (e redisError) Error() string { return string(e) }

func (redisError) RedisError() {}

func hasStringArg(args []interface{}, want string) bool {
	for _, arg := range args {
		s, ok := arg.(string)
		if ok && strings.EqualFold(s, want) {
			return true
		}
	}
	return false
}

func TestNewConsumer(t *testing.T) {
	t.Run("creates a new consumer", func(tt *testing.T) {
		c, err := NewConsumer()
		require.NoError(tt, err)

		assert.NotNil(tt, c)
	})
}

func TestNewConsumerWithOptions(t *testing.T) {
	t.Run("creates a new consumer", func(tt *testing.T) {
		c, err := NewConsumerWithOptions(&ConsumerOptions{})
		require.NoError(tt, err)

		assert.NotNil(tt, c)
	})

	t.Run("sets defaults for Name, GroupName, BlockingTimeout, ReclaimTimeout, and ReclaimShare", func(tt *testing.T) {
		c, err := NewConsumerWithOptions(&ConsumerOptions{})
		require.NoError(tt, err)

		hostname, err := os.Hostname()
		require.NoError(tt, err)

		assert.Equal(tt, hostname, c.options.Name)
		assert.Equal(tt, "redisqueue", c.options.GroupName)
		assert.Equal(tt, 5*time.Second, c.options.BlockingTimeout)
		assert.Equal(tt, 1*time.Second, c.options.ReclaimInterval)
		assert.Equal(tt, 4, c.options.ReclaimShare)
	})

	t.Run("allows override of Name, GroupName, BlockingTimeout, ReclaimTimeout, ReclaimShare, and RedisClient", func(tt *testing.T) {
		rc := newRedisClient(nil)

		c, err := NewConsumerWithOptions(&ConsumerOptions{
			Name:            "test_name",
			GroupName:       "test_group_name",
			BlockingTimeout: 10 * time.Second,
			ReclaimInterval: 10 * time.Second,
			ReclaimShare:    2,
			RedisClient:     rc,
		})
		require.NoError(tt, err)

		assert.Equal(tt, rc, c.redis)
		assert.Equal(tt, "test_name", c.options.Name)
		assert.Equal(tt, "test_group_name", c.options.GroupName)
		assert.Equal(tt, 10*time.Second, c.options.BlockingTimeout)
		assert.Equal(tt, 10*time.Second, c.options.ReclaimInterval)
		assert.Equal(tt, 2, c.options.ReclaimShare)
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		_, err := NewConsumerWithOptions(&ConsumerOptions{
			RedisOptions: &RedisOptions{Addr: "localhost:0"},
		})
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}

func TestRunRetriesGroupCreateTimeouts(t *testing.T) {
	var c *Consumer
	attempts := 0

	rc := newRedisClient(nil)
	rc.AddHook(commandHook{intercept: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if strings.EqualFold(cmd.Name(), "xgroup") && hasStringArg(cmd.Args(), "create") {
			attempts++
			if attempts == 1 {
				return context.DeadlineExceeded
			}
			c.Shutdown()
			return nil
		}
		return next(ctx, cmd)
	}})

	var err error
	c, err = NewConsumerWithOptions(&ConsumerOptions{
		VisibilityTimeout: 0,
		BlockingTimeout:   time.Millisecond,
		BufferSize:        1,
		Concurrency:       1,
		RedisClient:       rc,
		GroupCreateRetry: RetryOptions{
			MaxAttempts: 2,
		},
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 1)
	c.Register(t.Name(), func(msg *Message) error { return nil })

	c.Run()

	assert.Equal(t, 2, attempts)
	select {
	case err := <-c.Errors:
		t.Fatalf("unexpected error: %v", err)
	default:
	}
}

func TestWorkerRetriesAckTimeouts(t *testing.T) {
	var c *Consumer
	attempts := 0
	processed := false

	rc := newRedisClient(nil)
	rc.AddHook(commandHook{intercept: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if strings.EqualFold(cmd.Name(), "xack") {
			attempts++
			if attempts == 1 {
				return context.DeadlineExceeded
			}
			c.stopWorkers <- struct{}{}
			return nil
		}
		return next(ctx, cmd)
	}})

	var err error
	c, err = NewConsumerWithOptions(&ConsumerOptions{
		BufferSize:  1,
		Concurrency: 1,
		RedisClient: rc,
		AckRetry: RetryOptions{
			MaxAttempts: 2,
		},
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 1)
	c.Register(t.Name(), func(msg *Message) error {
		processed = true
		return nil
	})

	c.wg.Add(1)
	done := make(chan struct{})
	go func() {
		c.work()
		close(done)
	}()
	c.queue <- &Message{ID: "1-0", Stream: t.Name()}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}

	assert.True(t, processed)
	assert.Equal(t, 2, attempts)
	select {
	case err := <-c.Errors:
		t.Fatalf("unexpected error: %v", err)
	default:
	}
}

func TestReclaimBatchUsesIdleFilter(t *testing.T) {
	xpendingArgs := make(chan []interface{}, 1)
	rc := newRedisClient(nil)
	rc.AddHook(commandHook{process: func(cmd redis.Cmder) {
		if strings.EqualFold(cmd.Name(), "xpending") {
			xpendingArgs <- append([]interface{}{}, cmd.Args()...)
		}
	}})

	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: time.Minute,
		BufferSize:        100,
		RedisClient:       rc,
	})
	require.NoError(t, err)

	stream := t.Name()
	c.redis.XGroupDestroy(context.TODO(), stream, c.options.GroupName)
	require.NoError(t, c.redis.XGroupCreateMkStream(context.TODO(), stream, c.options.GroupName, "$").Err())
	c.Register(stream, func(msg *Message) error { return nil })

	c.reclaimBatch(stream, "-", "+")

	require.Equal(t, []interface{}{
		"xpending",
		stream,
		c.options.GroupName,
		"idle",
		int64(c.options.VisibilityTimeout / time.Millisecond),
		"-",
		"+",
		int64(c.options.BufferSize),
	}, <-xpendingArgs)
}

func TestReclaimFallsBackWhenIdleFilterUnsupported(t *testing.T) {
	const staleID = "1-0"
	const freshID = "2-0"

	stream := t.Name()
	xpendingArgs := make([][]interface{}, 0)
	xpendingWithoutIdleCalls := 0

	rc := newRedisClient(nil)
	rc.AddHook(commandHook{intercept: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		switch strings.ToLower(cmd.Name()) {
		case "xpending":
			if summary, ok := cmd.(*redis.XPendingCmd); ok {
				summary.SetVal(&redis.XPending{Count: 2, Lower: staleID, Higher: freshID})
				return nil
			}
			args := append([]interface{}{}, cmd.Args()...)
			xpendingArgs = append(xpendingArgs, args)

			if hasStringArg(args, "idle") {
				return redisError("ERR syntax error")
			}

			xpendingWithoutIdleCalls++
			if xpendingWithoutIdleCalls == 1 {
				cmd.(*redis.XPendingExtCmd).SetVal([]redis.XPendingExt{
					{ID: staleID, Consumer: "failed_consumer", Idle: 2 * time.Minute},
					{ID: freshID, Consumer: "failed_consumer", Idle: 30 * time.Second},
				})
			}
			return nil
		default:
			return next(ctx, cmd)
		}
	}})

	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: time.Minute,
		BufferSize:        100,
		RedisClient:       rc,
	})
	require.NoError(t, err)
	c.Register(stream, func(msg *Message) error { return nil })
	candidates := collectReclaimCandidates(t, c)

	c.reclaimPendingMessages()

	require.Len(t, xpendingArgs, 3)
	assert.True(t, hasStringArg(xpendingArgs[0], "idle"))
	assert.False(t, hasStringArg(xpendingArgs[1], "idle"))
	assert.False(t, hasStringArg(xpendingArgs[2], "idle"))

	assert.Equal(t, staleID, nextReclaimCandidate(t, candidates).ID)

	select {
	case msg := <-candidates:
		t.Fatalf("expected fresh message to stay pending, got %q", msg.ID)
	case <-time.After(50 * time.Millisecond):
	}
}

// collectReclaimCandidates stands in for the workers, receiving the candidates
// reclaim hands off without claiming them.
func collectReclaimCandidates(t *testing.T, c *Consumer) <-chan *Message {
	t.Helper()
	candidates := make(chan *Message, 100)
	go func() {
		for {
			select {
			case m := <-c.reclaimQueue:
				candidates <- m
			case <-c.options.Context.Done():
				return
			}
		}
	}()
	t.Cleanup(c.options.contextCancel)
	return candidates
}

func nextReclaimCandidate(t *testing.T, candidates <-chan *Message) *Message {
	t.Helper()
	select {
	case m := <-candidates:
		return m
	case <-time.After(time.Second):
		t.Fatal("expected a reclaim candidate")
		return nil
	}
}

// addStalePending adds n messages to stream and delivers them to a consumer
// that never acknowledges them, returning their IDs.
func addStalePending(t *testing.T, rc redis.UniversalClient, stream, group string, n int) []string {
	t.Helper()
	ctx := context.TODO()
	rc.Del(ctx, stream)
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "$").Err())

	ids := make([]string, n)
	for i := range ids {
		id, err := rc.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"i": i},
		}).Result()
		require.NoError(t, err)
		ids[i] = id
	}
	require.NoError(t, rc.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "dead_consumer",
		Streams:  []string{stream, ">"},
		Count:    int64(n),
	}).Err())
	return ids
}

// Model a stream whose pending tail grows on every scan. Failed messages
// stay pending, so each new pass must return to the oldest ID.
func TestReclaimBoundsPassWhilePendingTailGrows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tail, scans, snapshots := 1, 0, 0
	rc := newRedisClient(nil)
	rc.AddHook(commandHook{intercept: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		switch cmd.Name() {
		case "ping":
			cmd.(*redis.StatusCmd).SetVal("PONG")
		case "xpending":
			if summary, ok := cmd.(*redis.XPendingCmd); ok {
				snapshots++
				summary.SetVal(&redis.XPending{Count: int64(tail), Lower: "1-0", Higher: fmt.Sprintf("%d-0", tail)})
				return nil
			}
			scans++
			if scans > 8 {
				cancel() // Keep the regression finite on the unbounded implementation.
				return context.Canceled
			}
			tail++
			args := cmd.Args()
			start, end := 1, tail
			if args[5] != "-" {
				var sequence int
				_, err := fmt.Sscanf(args[5].(string), "%d-%d", &start, &sequence)
				require.NoError(t, err)
				if sequence > 0 {
					start++
				}
			}
			if args[6] != "+" {
				_, err := fmt.Sscanf(args[6].(string), "%d-0", &end)
				require.NoError(t, err)
			}
			var pending []redis.XPendingExt
			if start <= end {
				pending = append(pending, redis.XPendingExt{ID: fmt.Sprintf("%d-0", start), Idle: time.Hour})
			}
			cmd.(*redis.XPendingExtCmd).SetVal(pending)
		default:
			return next(ctx, cmd)
		}
		return nil
	}})
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Context: ctx, RedisClient: rc, BufferSize: 20, VisibilityTimeout: time.Minute,
	})
	require.NoError(t, err)
	defer c.options.contextCancel()
	c.Errors = make(chan error, 10)
	c.Register(t.Name(), func(*Message) error { return errors.New("processing failed") })
	candidates := collectReclaimCandidates(t, c)

	c.reclaimPendingMessages()
	require.NoError(t, ctx.Err(), "the pass followed the growing pending tail")
	first := nextReclaimCandidate(t, candidates)
	require.Equal(t, "1-0", first.ID)
	require.Empty(t, candidates)
	require.Error(t, c.process(first)) // The failed message is never acknowledged.

	c.reclaimPendingMessages()
	require.NoError(t, ctx.Err())
	require.Equal(t, 2, snapshots)
	require.Equal(t, first.ID, nextReclaimCandidate(t, candidates).ID, "the next pass must retry the oldest failure")
}

func TestReclaimDrainsBacklogWhenQueueIsEmpty(t *testing.T) {
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: 50 * time.Millisecond,
		BufferSize:        1,
		Concurrency:       1,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 10)
	defer c.options.contextCancel()

	stream := t.Name()
	ids := addStalePending(t, c.redis, stream, c.options.GroupName, 5)
	time.Sleep(100 * time.Millisecond)

	processed := make(chan string, len(ids))
	c.Register(stream, func(msg *Message) error {
		processed <- msg.ID
		return nil
	})
	c.wg.Add(1)
	go c.work()

	c.reclaimPendingMessages()

	for _, id := range ids {
		select {
		case got := <-processed:
			assert.Equal(t, id, got)
		case <-time.After(time.Second):
			t.Fatalf("expected reclaimed message %q", id)
		}
	}
}

func TestReclaimTakesTurnsAcrossStreams(t *testing.T) {
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: 50 * time.Millisecond,
		BufferSize:        1,
		Concurrency:       1,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 10)
	defer c.options.contextCancel()

	streams := []string{t.Name() + ":a", t.Name() + ":b", t.Name() + ":c"}
	ids := make(map[string][]string, len(streams))
	processed := make(chan string, 6)
	for _, stream := range streams {
		ids[stream] = addStalePending(t, c.redis, stream, c.options.GroupName, 2)
		c.Register(stream, func(msg *Message) error {
			processed <- msg.Stream + "/" + msg.ID
			return nil
		})
	}
	time.Sleep(100 * time.Millisecond)

	var want []string
	for i := 0; i < 2; i++ {
		for _, stream := range streams {
			want = append(want, stream+"/"+ids[stream][i])
		}
	}

	c.wg.Add(1)
	go c.work()

	c.reclaimPendingMessages()

	got := make([]string, 0, len(want))
	for range want {
		select {
		case msg := <-processed:
			got = append(got, msg)
		case <-time.After(time.Second):
			t.Fatalf("expected %d reclaimed messages, got %v", len(want), got)
		}
	}
	assert.Equal(t, want, got)
}

// With new and reclaimed messages both always ready, workers take one reclaimed
// message for every ReclaimShare messages, whatever the buffer size.
func TestWorkersGiveReclaimedMessagesTheirShare(t *testing.T) {
	for name, bufferSize := range map[string]int{"buffered": 4, "unbuffered": 0} {
		t.Run(name, func(t *testing.T) {
			rc := newRedisClient(nil)
			rc.AddHook(commandHook{intercept: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
				if cmd.Name() != "xclaim" {
					return next(ctx, cmd)
				}
				args := cmd.Args()
				cmd.(*redis.XMessageSliceCmd).SetVal([]redis.XMessage{{ID: args[len(args)-1].(string)}})
				return nil
			}})
			c, err := NewConsumerWithOptions(&ConsumerOptions{
				Name:              "test_consumer",
				GroupName:         "test_group",
				VisibilityTimeout: time.Minute,
				BufferSize:        bufferSize,
				Concurrency:       1,
				ReclaimShare:      3,
				RedisClient:       rc,
			})
			require.NoError(t, err)
			c.Errors = make(chan error, 100)

			stream := t.Name()
			processed := make(chan string, 100)
			c.Register(stream, func(msg *Message) error {
				processed <- msg.ID
				// Give both feeders time to offer their next message.
				time.Sleep(10 * time.Millisecond)
				return nil
			})
			feed := func(ch chan *Message, id string) {
				for {
					select {
					case ch <- &Message{ID: id, Stream: stream}:
					case <-c.options.Context.Done():
						return
					}
				}
			}
			go feed(c.queue, "new")
			go feed(c.reclaimQueue, "reclaimed")
			time.Sleep(20 * time.Millisecond)

			c.wg.Add(1)
			go c.work()
			t.Cleanup(func() { c.options.contextCancel(); c.wg.Wait() })

			want := []string{"new", "new", "reclaimed", "new", "new", "reclaimed", "new", "new", "reclaimed"}
			got := make([]string, 0, len(want))
			for range want {
				select {
				case id := <-processed:
					got = append(got, id)
				case <-time.After(time.Second):
					t.Fatalf("expected %d messages, got %v", len(want), got)
				}
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestReclaimReachesWorkersWhileNewMessagesKeepQueueFull(t *testing.T) {
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: 50 * time.Millisecond,
		BufferSize:        1,
		Concurrency:       1,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 100)
	defer c.options.contextCancel()

	stream := t.Name()
	stale := addStalePending(t, c.redis, stream, c.options.GroupName, 3)
	time.Sleep(100 * time.Millisecond)

	processed := make(chan string, len(stale))
	isStale := make(map[string]bool, len(stale))
	for _, id := range stale {
		isStale[id] = true
	}
	c.Register(stream, func(msg *Message) error {
		if isStale[msg.ID] {
			processed <- msg.ID
		}
		return nil
	})

	// Stand in for poll with an endless supply of new messages.
	go func() {
		for i := 1; ; i++ {
			select {
			case c.queue <- &Message{ID: fmt.Sprintf("0-%d", i), Stream: stream}:
			case <-c.options.Context.Done():
				return
			}
		}
	}()
	require.Eventually(t, func() bool { return len(c.queue) == 1 }, time.Second, time.Millisecond)

	c.wg.Add(1)
	go c.work()
	go c.reclaimPendingMessages()

	for _, id := range stale {
		select {
		case got := <-processed:
			assert.Equal(t, id, got)
		case <-time.After(2 * time.Second):
			t.Fatalf("expected reclaimed message %q to reach a worker", id)
		}
	}
}

func TestReclaimStopsWhenConsumerShutsDown(t *testing.T) {
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: 50 * time.Millisecond,
		BufferSize:        1,
	})
	require.NoError(t, err)

	stream := t.Name()
	addStalePending(t, c.redis, stream, c.options.GroupName, 1)
	time.Sleep(100 * time.Millisecond)
	c.Register(stream, func(msg *Message) error { return nil })

	done := make(chan struct{})
	go func() {
		c.reclaimPendingMessages()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected reclaim to wait while no worker is free")
	case <-time.After(100 * time.Millisecond):
	}

	c.options.contextCancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected reclaim to stop after shutdown")
	}
}

func TestPollWaitsForQueueCapacity(t *testing.T) {
	readCounts := make(chan interface{}, 10)
	rc := newRedisClient(nil)
	rc.AddHook(commandHook{process: func(cmd redis.Cmder) {
		if !strings.EqualFold(cmd.Name(), "xreadgroup") {
			return
		}
		args := cmd.Args()
		var count interface{}
		for i := 0; i < len(args)-1; i++ {
			if s, ok := args[i].(string); ok && strings.EqualFold(s, "count") {
				count = args[i+1]
			}
		}
		readCounts <- count
	}})

	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:            "test_consumer",
		GroupName:       "test_group",
		BlockingTimeout: 10 * time.Millisecond,
		BufferSize:      1,
		Concurrency:     1,
		RedisClient:     rc,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 10)

	stream := t.Name()
	rc.Del(context.TODO(), stream)
	require.NoError(t, rc.XGroupCreateMkStream(context.TODO(), stream, c.options.GroupName, "$").Err())
	for i := 0; i < 3; i++ {
		require.NoError(t, rc.XAdd(context.TODO(), &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"i": i},
		}).Err())
	}
	c.Register(stream, func(msg *Message) error { return nil })
	c.streams = []string{stream, ">"}
	c.queue <- &Message{ID: "queued", Stream: stream}

	done := make(chan struct{})
	go func() {
		c.poll()
		close(done)
	}()
	defer func() {
		c.options.contextCancel()
		for {
			select {
			case <-done:
				return
			case <-c.queue:
			}
		}
	}()

	select {
	case count := <-readCounts:
		t.Fatalf("expected no XREADGROUP with a full queue, got COUNT %v", count)
	case <-time.After(50 * time.Millisecond):
	}

	<-c.queue
	c.dequeued <- struct{}{}

	select {
	case count := <-readCounts:
		assert.EqualValues(t, 1, count)
	case <-time.After(time.Second):
		t.Fatal("expected XREADGROUP once the queue had capacity")
	}

	require.Eventually(t, func() bool { return len(c.queue) == 1 }, time.Second, time.Millisecond)
	pending, err := rc.XPending(context.TODO(), stream, c.options.GroupName).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, pending.Count)
}

func TestPollReadsOneAtATimeForUnbufferedQueue(t *testing.T) {
	readCounts := make(chan interface{}, 10)
	rc := newRedisClient(nil)
	rc.AddHook(commandHook{process: func(cmd redis.Cmder) {
		if !strings.EqualFold(cmd.Name(), "xreadgroup") {
			return
		}
		args := cmd.Args()
		var count interface{}
		for i := 0; i < len(args)-1; i++ {
			if s, ok := args[i].(string); ok && strings.EqualFold(s, "count") {
				count = args[i+1]
			}
		}
		readCounts <- count
	}})

	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:            "test_consumer",
		GroupName:       "test_group",
		BlockingTimeout: 10 * time.Millisecond,
		Concurrency:     1,
		RedisClient:     rc,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 10)

	stream := t.Name()
	rc.Del(context.TODO(), stream)
	require.NoError(t, rc.XGroupCreateMkStream(context.TODO(), stream, c.options.GroupName, "$").Err())
	for i := 0; i < 3; i++ {
		require.NoError(t, rc.XAdd(context.TODO(), &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"i": i},
		}).Err())
	}
	c.Register(stream, func(msg *Message) error { return nil })
	c.streams = []string{stream, ">"}

	done := make(chan struct{})
	go func() {
		c.poll()
		close(done)
	}()
	defer func() {
		c.options.contextCancel()
		for {
			select {
			case <-done:
				return
			case <-c.queue:
			}
		}
	}()

	select {
	case msg := <-c.queue:
		assert.Equal(t, stream, msg.Stream)
	case <-time.After(time.Second):
		t.Fatal("expected a message handed off through the unbuffered queue")
	}
	select {
	case count := <-readCounts:
		assert.EqualValues(t, 1, count)
	case <-time.After(time.Second):
		t.Fatal("expected an XREADGROUP")
	}
}

func TestReclaimWaitsForWorkerBeforeClaiming(t *testing.T) {
	t.Run("buffered", func(t *testing.T) { testReclaimWaitsForWorkerBeforeClaiming(t, 1) })
	t.Run("unbuffered", func(t *testing.T) { testReclaimWaitsForWorkerBeforeClaiming(t, 0) })
}

func testReclaimWaitsForWorkerBeforeClaiming(t *testing.T, bufferSize int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listed := make(chan struct{}, 4)
	claimed := make(chan string, 4)
	rc := newRedisClient(nil)
	rc.AddHook(commandHook{process: func(cmd redis.Cmder) {
		switch cmd.Name() {
		case "xpending":
			if _, ok := cmd.(*redis.XPendingExtCmd); ok {
				listed <- struct{}{}
			}
		case "xclaim":
			args := cmd.Args()
			claimed <- args[len(args)-1].(string)
		}
	}})
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Context: ctx, RedisClient: rc, BufferSize: bufferSize, Concurrency: 1,
		Name: "test_consumer", GroupName: "test_group", VisibilityTimeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 10)
	stream := t.Name()
	ids := addStalePending(t, rc, stream, c.options.GroupName, 2)
	time.Sleep(100 * time.Millisecond)

	started := make(chan string, 3)
	releaseBusy := make(chan struct{})
	releaseFirst := make(chan struct{})
	c.RegisterContext(stream, func(ctx context.Context, msg *Message) error {
		started <- msg.ID
		var release <-chan struct{}
		switch msg.ID {
		case "busy":
			release = releaseBusy
		case ids[0]:
			release = releaseFirst
		default:
			return nil
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})
	c.wg.Add(1)
	go c.work()
	t.Cleanup(func() { c.options.contextCancel(); c.wg.Wait() })
	c.queue <- &Message{ID: "busy", Stream: stream}
	require.Equal(t, "busy", <-started)
	done := make(chan struct{})
	go func() { c.reclaimPendingMessages(); close(done) }()

	waitListed := func() {
		t.Helper()
		select {
		case <-listed:
		case <-time.After(time.Second):
			t.Fatal("pending batch was not listed")
		}
	}
	assertNotClaimed := func() {
		t.Helper()
		select {
		case id := <-claimed:
			t.Fatalf("claimed %q while the only worker was busy", id)
		case <-time.After(50 * time.Millisecond):
		}
	}
	waitStarted := func(want string) {
		t.Helper()
		select {
		case id := <-started:
			require.Equal(t, want, id)
		case <-time.After(time.Second):
			t.Fatal("worker did not start reclaimed message")
		}
	}
	waitListed()
	assertNotClaimed()
	close(releaseBusy)
	waitStarted(ids[0])
	require.Equal(t, ids[0], <-claimed)
	waitListed()
	assertNotClaimed()
	close(releaseFirst)
	waitStarted(ids[1])
	require.Equal(t, ids[1], <-claimed)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reclaim pass did not finish")
	}
}

func TestReclaimRechecksIdleAfterWaitingForWorker(t *testing.T) {
	listed, attempted := make(chan struct{}, 1), make(chan struct{}, 1)
	rc := newRedisClient(nil)
	rc.AddHook(commandHook{intercept: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		switch cmd.Name() {
		case "xpending":
			if _, ok := cmd.(*redis.XPendingExtCmd); ok {
				listed <- struct{}{}
			}
		case "xclaim":
			attempted <- struct{}{}
		}
		return err
	}})
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		RedisClient: rc, BufferSize: 0, Concurrency: 1, Name: "test_consumer",
		GroupName: "test_group", VisibilityTimeout: time.Minute,
	})
	require.NoError(t, err)
	c.Errors = make(chan error, 10)
	other := newRedisClient(nil)
	defer other.Close()
	stream := t.Name()
	ids := addStalePending(t, other, stream, c.options.GroupName, 1)
	require.NoError(t, other.Do(context.Background(), "XCLAIM", stream, c.options.GroupName,
		"dead_consumer", 0, ids[0], "IDLE", int64((2*time.Minute)/time.Millisecond)).Err())

	started := make(chan string, 3)
	release := make(chan struct{})
	c.RegisterContext(stream, func(ctx context.Context, msg *Message) error {
		started <- msg.ID
		if msg.ID == "busy" {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return nil
	})
	c.wg.Add(1)
	go c.work()
	t.Cleanup(func() { c.options.contextCancel(); c.wg.Wait() })
	c.queue <- &Message{ID: "busy", Stream: stream}
	require.Equal(t, "busy", <-started)
	done := make(chan struct{})
	go func() { c.reclaimBatch(stream, "-", "+"); close(done) }()
	select {
	case <-listed:
	case <-time.After(time.Second):
		t.Fatal("pending batch was not listed")
	}
	// A different consumer claims the candidate while our worker remains busy.
	require.NoError(t, other.XClaim(context.Background(), &redis.XClaimArgs{
		Stream: stream, Group: c.options.GroupName, Consumer: "other_consumer", Messages: ids,
	}).Err())
	close(release)
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt claim")
	}
	// A normal message proves the worker finished the unsuccessful claim and
	// returned to receiving work without processing the now-fresh candidate.
	select {
	case c.queue <- &Message{ID: "sentinel", Stream: stream}:
	case <-time.After(time.Second):
		t.Fatal("worker did not resume receiving work")
	}
	require.Equal(t, "sentinel", <-started)
	pending, err := other.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: c.options.GroupName, Start: "-", End: "+", Count: 1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "other_consumer", pending[0].Consumer)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reclaim batch did not finish")
	}
}

func TestReclaimErrorsIncludeContext(t *testing.T) {
	c, err := NewConsumerWithOptions(&ConsumerOptions{
		Name:              "test_consumer",
		GroupName:         "test_group",
		VisibilityTimeout: time.Minute,
		BufferSize:        100,
	})
	require.NoError(t, err)

	stream := t.Name()
	c.redis.XGroupDestroy(context.TODO(), stream, c.options.GroupName)
	c.Register(stream, func(msg *Message) error { return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- <-c.Errors }()

	c.reclaimBatch(stream, "-", "+")

	err = <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), stream)
	assert.Contains(t, err.Error(), c.options.GroupName)
	assert.Contains(t, err.Error(), c.options.Name)
}

func TestRegister(t *testing.T) {
	fn := func(msg *Message) error {
		return nil
	}

	t.Run("set the function", func(tt *testing.T) {
		c, err := NewConsumer()
		require.NoError(tt, err)

		c.Register(tt.Name(), fn)

		assert.Len(tt, c.consumers, 1)
	})
}

func TestRegisterWithLastID(t *testing.T) {
	fn := func(msg *Message) error {
		return nil
	}

	tests := []struct {
		name   string
		stream string
		id     string
		want   map[string]registeredConsumer
	}{
		{
			name: "custom_id",
			id:   "42",
			want: map[string]registeredConsumer{
				"test": {id: "42", fn: fn},
			},
		},
		{
			name: "no_id",
			id:   "",
			want: map[string]registeredConsumer{
				"test": {id: "0", fn: fn},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer()
			require.NoError(t, err)

			c.RegisterWithLastID("test", tt.id, fn)

			assert.Len(t, c.consumers, 1)
			assert.Contains(t, c.consumers, "test")
			assert.Equal(t, c.consumers["test"].id, tt.want["test"].id)
			assert.NotNil(t, c.consumers["test"].fn)
		})
	}
}

func TestRun(t *testing.T) {
	t.Run("sends an error if no ConsumerFuncs are registered", func(tt *testing.T) {
		c, err := NewConsumer()
		require.NoError(tt, err)

		go func() {
			err := <-c.Errors
			require.Error(tt, err)
			assert.Equal(tt, "at least one consumer function needs to be registered", err.Error())
		}()

		c.Run()
	})

	t.Run("calls the ConsumerFunc on for a message", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer()
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			c.Shutdown()
			return nil
		})

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// run the consumer
		c.Run()
	})

	t.Run("reclaims pending messages according to ReclaimInterval", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 5 * time.Millisecond,
			BlockingTimeout:   10 * time.Millisecond,
			ReclaimInterval:   1 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer()
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, msg.ID, m.ID)
			c.Shutdown()
			return nil
		})

		// read the message but don't acknowledge it
		res, err := c.redis.XReadGroup(context.TODO(), &redis.XReadGroupArgs{
			Group:    c.options.GroupName,
			Consumer: "failed_consumer",
			Streams:  []string{tt.Name(), ">"},
			Count:    1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.Len(tt, res[0].Messages, 1)
		require.Equal(tt, msg.ID, res[0].Messages[0].ID)

		// wait for more than VisibilityTimeout + ReclaimInterval to ensure that
		// the pending message is reclaimed
		time.Sleep(6 * time.Millisecond)

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// run the consumer
		c.Run()
	})

	t.Run("doesn't reclaim if there is no VisibilityTimeout set", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			BlockingTimeout: 10 * time.Millisecond,
			ReclaimInterval: 1 * time.Millisecond,
			BufferSize:      100,
			Concurrency:     10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducerWithOptions(&ProducerOptions{
			StreamMaxLength:      2,
			ApproximateMaxLength: false,
		})
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		msg1 := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		msg2 := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value2"},
		}
		err = p.Enqueue(msg1)
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, msg2.ID, m.ID)
			c.Shutdown()
			return nil
		})

		// read the message but don't acknowledge it
		res, err := c.redis.XReadGroup(context.TODO(), &redis.XReadGroupArgs{
			Group:    c.options.GroupName,
			Consumer: "failed_consumer",
			Streams:  []string{tt.Name(), ">"},
			Count:    1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.Len(tt, res[0].Messages, 1)
		require.Equal(tt, msg1.ID, res[0].Messages[0].ID)

		// add another message to the stream to let the consumer consume it
		err = p.Enqueue(msg2)
		require.NoError(tt, err)

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// run the consumer
		c.Run()

		// check if the pending message is still there
		pendingRes, err := c.redis.XPendingExt(context.TODO(), &redis.XPendingExtArgs{
			Stream: tt.Name(),
			Group:  c.options.GroupName,
			Start:  "-",
			End:    "+",
			Count:  1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, pendingRes, 1)
		require.Equal(tt, msg1.ID, pendingRes[0].ID)
	})

	t.Run("acknowledges pending messages that have already been deleted", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 5 * time.Millisecond,
			BlockingTimeout:   10 * time.Millisecond,
			ReclaimInterval:   1 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducerWithOptions(&ProducerOptions{
			StreamMaxLength:      1,
			ApproximateMaxLength: false,
		})
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		// register a noop handler that should never be called
		c.Register(tt.Name(), func(m *Message) error {
			t.Fail()
			return nil
		})

		// read the message but don't acknowledge it
		res, err := c.redis.XReadGroup(context.TODO(), &redis.XReadGroupArgs{
			Group:    c.options.GroupName,
			Consumer: "failed_consumer",
			Streams:  []string{tt.Name(), ">"},
			Count:    1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.Len(tt, res[0].Messages, 1)
		require.Equal(tt, msg.ID, res[0].Messages[0].ID)

		// delete the message
		err = c.redis.XDel(context.TODO(), tt.Name(), msg.ID).Err()
		require.NoError(tt, err)

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// in 10ms, shut down the consumer
		go func() {
			time.Sleep(10 * time.Millisecond)
			c.Shutdown()
		}()

		// run the consumer
		c.Run()

		// check that there are no pending messages
		pendingRes, err := c.redis.XPendingExt(context.TODO(), &redis.XPendingExtArgs{
			Stream: tt.Name(),
			Group:  c.options.GroupName,
			Start:  "-",
			End:    "+",
			Count:  1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, pendingRes, 0)
	})

	t.Run("returns an error on a string panic", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer()
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message, shut down the
		// consumer, and then panic with a string
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			c.Shutdown()
			panic("this is a panic")
		})

		// watch for the panic
		go func() {
			err := <-c.Errors
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), "this is a panic")
		}()

		// run the consumer
		c.Run()
	})

	t.Run("returns an error on an error panic", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer()
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message, shut down the
		// consumer, and then panic with an error
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			c.Shutdown()
			panic(errors.New("this is a panic"))
		})

		// watch for the panic
		go func() {
			err := <-c.Errors
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), "this is a panic")
		}()

		// run the consumer
		c.Run()
	})

	t.Run("we can cancel the context", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer()
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(context.TODO(), tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(context.TODO(), tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		canceled := false
		c.RegisterContext(tt.Name(), func(ctx context.Context, m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			// if the timer fires, we failed to cancel ourselves in time
			t := time.NewTimer(time.Millisecond * 10)
			select {
			case <-t.C:
				tt.Fail()
			case <-ctx.Done():
				canceled = true
			}
			return nil
		})

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// pend a cancelation before running the consumer (which will block)
		go func() {
			time.Sleep(time.Millisecond * 5)
			c.Shutdown()
		}()

		c.Run()

		assert.True(tt, canceled)
	})
}
