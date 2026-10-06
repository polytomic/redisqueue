package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConsumerFunc is a type alias for the functions that will be used to handle
// and process Messages.
type ConsumerFunc func(*Message) error

type ConsumerFuncContext func(context.Context, *Message) error

// RetryOptions configures retry behavior for transient Redis errors.
// MaxAttempts includes the initial attempt. If MaxAttempts is less than 2,
// operations are attempted once with no retry.
type RetryOptions struct {
	// MaxAttempts is the maximum number of attempts, including the initial one.
	MaxAttempts int
	// InitialBackoff is the delay before the first retry. If zero, retries are immediate.
	InitialBackoff time.Duration
	// MaxBackoff caps exponential backoff. If zero, InitialBackoff is used without a cap.
	MaxBackoff time.Duration
	// PerAttemptTimeout bounds each Redis attempt. If zero, the caller's context is used directly.
	PerAttemptTimeout time.Duration
}

type registeredConsumer struct {
	fn  ConsumerFunc
	fnc ConsumerFuncContext
	id  string
}

// ConsumerOptions provide options to configure the Consumer.
type ConsumerOptions struct {
	// Name sets the name of this consumer. This will be used when fetching from
	// Redis. If empty, the hostname will be used.
	Name string
	// GroupName sets the name of the consumer group. This will be used when
	// coordinating in Redis. If empty, the hostname will be used.
	GroupName string
	// VisibilityTimeout dictates the maximum amount of time a message should
	// stay in pending. If there is a message that has been idle for more than
	// this duration, the consumer will attempt to claim it.
	VisibilityTimeout time.Duration
	// BlockingTimeout designates how long the XREADGROUP call blocks for. If
	// this is 0, it will block indefinitely. While this is the most efficient
	// from a polling perspective, if this call never times out, there is no
	// opportunity to yield back to Go at a regular interval. This means it's
	// possible that if no messages are coming in, the consumer cannot
	// gracefully shutdown. Instead, it's recommended to set this to 1-5
	// seconds, or even longer, depending on how long your application can wait
	// to shutdown.
	BlockingTimeout time.Duration
	// ReclaimInterval is the amount of time in between passes over the pending
	// messages of every stream, claiming the ones that have been idle for more
	// than the visibility timeout. Each pass scans up to the highest pending
	// ID captured for each stream at its start. A smaller duration will result
	// in more frequent checks. This
	// will allow messages to be reaped faster, but it will put more load on
	// Redis.
	ReclaimInterval time.Duration
	// ReclaimShare gives reclaimed messages one of every ReclaimShare
	// messages workers take while both new and reclaimed messages are ready,
	// so new messages keep priority without starving a backlog of stale
	// pending messages. A worker that finds no new message ready takes a
	// reclaimed one right away. If 0, it defaults to 4.
	ReclaimShare int
	// BufferSize determines the size of the channel uses to coordinate the
	// processing of the messages. This determines the maximum number of
	// in-flight messages.
	BufferSize int
	// Concurrency dictates how many goroutines to spawn to handle the messages.
	Concurrency int
	// RedisClient supersedes the RedisOptions field, and allows you to inject
	// an already-made Redis Client for use in the consumer. This may be either
	// the standard client or a cluster client.
	RedisClient redis.UniversalClient
	// RedisOptions allows you to configure the underlying Redis connection.
	// More info here:
	// https://pkg.go.dev/github.com/redis/go-redis/v9?tab=doc#Options.
	//
	// This field is used if RedisClient field is nil.
	RedisOptions *RedisOptions

	// Context is the context for the consumer and processing functions.
	Context context.Context
	// AckRetry configures retries for XACK after successful processing and
	// after deleted pending messages are discovered during reclaim.
	AckRetry RetryOptions
	// GroupCreateRetry configures retries for creating consumer groups during Run.
	GroupCreateRetry RetryOptions

	contextCancel context.CancelFunc
}

// Consumer adds a convenient wrapper around dequeuing and managing concurrency.
type Consumer struct {
	// Errors is a channel that you can receive from to centrally handle any
	// errors that may occur either by your ConsumerFuncs or by internal
	// processing functions. Because this is an unbuffered channel, you must
	// have a listener on it. If you don't parts of the consumer could stop
	// functioning when errors occur due to the blocking nature of unbuffered
	// channels.
	Errors chan error

	options   *ConsumerOptions
	redis     redis.UniversalClient
	consumers map[string]registeredConsumer
	streams   []string
	queue     chan *Message
	// reclaimQueue hands stale pending candidates to an available worker,
	// which claims them with XCLAIM.
	reclaimQueue chan *Message
	wg           *sync.WaitGroup

	stopReclaim chan struct{}
	stopPoll    chan struct{}
	stopWorkers chan struct{}
	// dequeued wakes poll when a worker frees a slot in queue.
	dequeued chan struct{}
	// takes counts the times workers have gone looking for a message. Every
	// ReclaimShare-th take prefers reclaimQueue.
	takes atomic.Int64

	xpendingIdleUnsupported bool
}

var defaultConsumerOptions = &ConsumerOptions{
	VisibilityTimeout: 60 * time.Second,
	BlockingTimeout:   5 * time.Second,
	ReclaimInterval:   1 * time.Second,
	BufferSize:        100,
	Concurrency:       10,
	Context:           context.Background(),
}

// NewConsumer uses a default set of options to create a Consumer. It sets Name
// to the hostname, GroupName to "redisqueue", VisibilityTimeout to 60 seconds,
// BufferSize to 100, and Concurrency to 10. In most production environments,
// you'll want to use NewConsumerWithOptions.
func NewConsumer() (*Consumer, error) {
	return NewConsumerWithOptions(defaultConsumerOptions)
}

// NewConsumerWithOptions creates a Consumer with custom ConsumerOptions. If
// Name is left empty, it defaults to the hostname; if GroupName is left empty,
// it defaults to "redisqueue"; if BlockingTimeout is 0, it defaults to 5
// seconds; if ReclaimInterval is 0, it defaults to 1 second; if ReclaimShare is
// 0, it defaults to 4.
func NewConsumerWithOptions(options *ConsumerOptions) (*Consumer, error) {
	hostname, _ := os.Hostname()

	if options.Name == "" {
		options.Name = hostname
	}
	if options.GroupName == "" {
		options.GroupName = "redisqueue"
	}
	if options.BlockingTimeout == 0 {
		options.BlockingTimeout = 5 * time.Second
	}
	if options.ReclaimInterval == 0 {
		options.ReclaimInterval = 1 * time.Second
	}
	if options.ReclaimShare <= 0 {
		options.ReclaimShare = 4
	}

	if options.Context == nil {
		options.Context = context.Background()
	}
	options.Context, options.contextCancel = context.WithCancel(options.Context)

	var r redis.UniversalClient

	if options.RedisClient != nil {
		r = options.RedisClient
	} else {
		r = newRedisClient(options.RedisOptions)
	}

	if err := redisPreflightChecks(r); err != nil {
		return nil, err
	}

	return &Consumer{
		Errors: make(chan error),

		options:      options,
		redis:        r,
		consumers:    make(map[string]registeredConsumer),
		streams:      make([]string, 0),
		queue:        make(chan *Message, options.BufferSize),
		reclaimQueue: make(chan *Message),
		wg:           &sync.WaitGroup{},

		stopReclaim: make(chan struct{}, 1),
		stopPoll:    make(chan struct{}, 1),
		stopWorkers: make(chan struct{}, options.Concurrency),
		dequeued:    make(chan struct{}, 1),
	}, nil
}

// RegisterWithLastID is the same as Register, except that it also lets you
// specify the oldest message to receive when first creating the consumer group.
// This can be any valid message ID, "0" for all messages in the stream, or "$"
// for only new messages.
//
// If the consumer group already exists the id field is ignored, meaning you'll
// receive unprocessed messages.
func (c *Consumer) RegisterWithLastID(stream string, id string, fn ConsumerFunc) {
	if len(id) == 0 {
		id = "0"
	}

	c.consumers[stream] = registeredConsumer{
		fn: fn,
		id: id,
	}
}

// Register takes in a stream name and a ConsumerFunc that will be called when a
// message comes in from that stream. Register must be called at least once
// before Run is called. If the same stream name is passed in twice, the first
// ConsumerFunc is overwritten by the second.
func (c *Consumer) Register(stream string, fn ConsumerFunc) {
	c.RegisterWithLastID(stream, "0", fn)
}

// RegisterWithLastID is the same as Register, except that it also lets you
// specify the oldest message to receive when first creating the consumer group.
// This can be any valid message ID, "0" for all messages in the stream, or "$"
// for only new messages.
//
// If the consumer group already exists the id field is ignored, meaning you'll
// receive unprocessed messages.
func (c *Consumer) RegisterWithLastIDContext(stream string, id string, fnc ConsumerFuncContext) {
	if len(id) == 0 {
		id = "0"
	}

	c.consumers[stream] = registeredConsumer{
		fnc: fnc,
		id:  id,
	}
}

// Register takes in a stream name and a ConsumerFunc that will be called when a
// message comes in from that stream. Register must be called at least once
// before Run is called. If the same stream name is passed in twice, the first
// ConsumerFunc is overwritten by the second.
func (c *Consumer) RegisterContext(stream string, fnc ConsumerFuncContext) {
	c.RegisterWithLastIDContext(stream, "0", fnc)
}

// Run starts all of the worker goroutines and starts processing from the
// streams that have been registered with Register. All errors will be sent to
// the Errors channel. If Register was never called, an error will be sent and
// Run will terminate early. The same will happen if an error occurs when
// creating the consumer group in Redis. Run will block until Shutdown is called
// and all of the in-flight messages have been processed.
func (c *Consumer) Run() {
	if len(c.consumers) == 0 {
		c.Errors <- errors.New("at least one consumer function needs to be registered")
		return
	}

	for stream, consumer := range c.consumers {
		c.streams = append(c.streams, stream)
		err := c.withRetry(c.options.Context, c.options.GroupCreateRetry, func(ctx context.Context) error {
			return c.redis.XGroupCreateMkStream(ctx, stream, c.options.GroupName, consumer.id).Err()
		})
		// ignoring the BUSYGROUP error makes this a noop
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			c.Errors <- fmt.Errorf("error creating consumer group: %w", err)
			return
		}
	}

	for i := 0; i < len(c.consumers); i++ {
		c.streams = append(c.streams, ">")
	}

	go c.reclaim()
	go c.poll()

	stop := newSignalHandler()
	go func() {
		<-stop
		c.Shutdown()
	}()

	c.wg.Add(c.options.Concurrency)

	for i := 0; i < c.options.Concurrency; i++ {
		go c.work()
	}

	c.wg.Wait()
}

// Shutdown stops new messages from being processed and tells the workers to
// wait until all in-flight messages have been processed, and then they exit.
// The order that things stop is 1) the reclaim process (if it's running), 2)
// the polling process, and 3) the worker processes.
func (c *Consumer) Shutdown() {
	c.options.contextCancel()
	c.stopReclaim <- struct{}{}
	if c.options.VisibilityTimeout == 0 {
		c.stopPoll <- struct{}{}
	}
}

// reclaim runs in a separate goroutine and checks the list of pending messages
// in every stream that have been idle for longer than VisibilityTimeout. It
// hands each stale pending message to a worker, which claims it for this
// consumer as ReclaimShare allows. If VisibilityTimeout is 0, this function
// returns early and no messages are reclaimed. It starts a pass over the
// pending messages according to ReclaimInterval with jitter to avoid
// synchronized reclaim bursts.
func (c *Consumer) reclaim() {
	if c.options.VisibilityTimeout == 0 {
		return
	}

	timer := time.NewTimer(c.reclaimDelay())
	defer timer.Stop()

	for {
		select {
		case <-c.stopReclaim:
			// once the reclaim process has stopped, stop the polling process
			c.stopPoll <- struct{}{}
			return
		case <-timer.C:
			c.reclaimPendingMessages()
			timer.Reset(c.reclaimDelay())
		}
	}
}

func (c *Consumer) reclaimDelay() time.Duration {
	return c.options.ReclaimInterval + jitter(c.options.ReclaimInterval)
}

func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}

func (c *Consumer) reclaimPendingMessages() {
	streams := make([]string, 0, len(c.consumers))
	for stream := range c.consumers {
		streams = append(streams, stream)
	}
	sort.Strings(streams)
	starts := make(map[string]string, len(streams))
	ends := make(map[string]string, len(streams))
	// Snapshot every stream before handing off work. New pending IDs belong
	// to the next pass, so a growing tail cannot prevent older failures from
	// being revisited at the next interval.
	remaining := streams[:0]
	for _, stream := range streams {
		pending, err := c.redis.XPending(c.options.Context, stream, c.options.GroupName).Result()
		if err != nil && err != redis.Nil {
			if c.options.Context.Err() != nil {
				return
			}
			c.Errors <- fmt.Errorf("error listing pending messages for %q stream, %q group, and %q consumer: %w", stream, c.options.GroupName, c.options.Name, err)
			continue
		}
		if pending == nil || pending.Count == 0 {
			continue
		}
		starts[stream] = "-"
		ends[stream] = pending.Higher
		remaining = append(remaining, stream)
	}
	streams = remaining

	// Take one batch from each stream in turn so a backlog on one stream
	// cannot hold up the others.
	for len(streams) > 0 {
		remaining := streams[:0]
		for _, stream := range streams {
			if c.options.Context.Err() != nil {
				return
			}
			next, more := c.reclaimBatch(stream, starts[stream], ends[stream])
			if more {
				starts[stream] = next
				remaining = append(remaining, stream)
			}
		}
		streams = remaining
	}
}

// reclaimBatch scans idle pending messages on stream from start through end
// and hands each one to a worker to claim. It returns the ID to resume from and
// whether the stream may have more pending messages.
func (c *Consumer) reclaimBatch(stream, start, end string) (string, bool) {
	count := int64(max(c.options.BufferSize, c.options.Concurrency, 1))

	res, filterByIdle, err := c.xPendingExt(stream, start, end, count)
	if err != nil && err != redis.Nil {
		if c.options.Context.Err() == nil {
			c.Errors <- fmt.Errorf("error listing pending messages for %q stream, %q group, and %q consumer: %w", stream, c.options.GroupName, c.options.Name, err)
		}
		return "", false
	}
	if len(res) == 0 {
		return "", false
	}

	for _, r := range res {
		if filterByIdle && r.Idle < c.options.VisibilityTimeout {
			continue
		}
		// The worker that takes the candidate claims it, so a claimed message
		// does not wait long enough for another consumer to claim it again.
		select {
		case c.reclaimQueue <- &Message{ID: r.ID, Stream: stream}:
		case <-c.options.Context.Done():
			return "", false
		}
	}

	newID, err := incrementMessageID(res[len(res)-1].ID)
	if err != nil {
		c.Errors <- err
		return "", false
	}
	return newID, true
}

// claimPendingMessage rechecks idle time at ownership transfer. Workers call
// it only after accepting a candidate, so waiting for a worker does not
// consume the reclaimed message's visibility timeout.
func (c *Consumer) claimPendingMessage(stream, id string) ([]redis.XMessage, error) {
	claimres, err := c.redis.XClaim(c.options.Context, &redis.XClaimArgs{
		Stream:   stream,
		Group:    c.options.GroupName,
		Consumer: c.options.Name,
		MinIdle:  c.options.VisibilityTimeout,
		Messages: []string{id},
	}).Result()
	if err != nil && err != redis.Nil {
		if c.options.Context.Err() == nil {
			c.Errors <- fmt.Errorf("error claiming pending message for %q stream, %q group, %q consumer, and %q message: %w", stream, c.options.GroupName, c.options.Name, id, err)
		}
		return nil, err
	}
	// If the Redis nil error is returned, it means that
	// the message no longer exists in the stream.
	// However, it is still in a pending state. This
	// could happen if a message was claimed by a
	// consumer, that consumer died, and the message
	// gets deleted (either through a XDEL call or
	// through MAXLEN). Since the message no longer
	// exists, the only way we can get it out of the
	// pending state is to acknowledge it.
	if err == redis.Nil {
		err = c.ackMessage(context.Background(), stream, id)
		if err != nil {
			c.Errors <- fmt.Errorf("error acknowledging after failed claim for %q stream, %q group, and %q message: %w", stream, c.options.GroupName, id, err)
		}
		return nil, nil
	}
	return claimres, nil
}

// xPendingExt lists pending messages, using XPENDING IDLE when Redis supports it.
// The bool return value is true when Redis did not apply the idle filter, so the
// caller must filter returned entries by XPendingExt.Idle locally.
func (c *Consumer) xPendingExt(stream, start, end string, count int64) ([]redis.XPendingExt, bool, error) {
	args := &redis.XPendingExtArgs{
		Stream: stream,
		Group:  c.options.GroupName,
		Start:  start,
		End:    end,
		Count:  count,
	}

	if c.xpendingIdleUnsupported {
		res, err := c.redis.XPendingExt(c.options.Context, args).Result()
		return res, true, err
	}

	args.Idle = c.options.VisibilityTimeout
	res, err := c.redis.XPendingExt(c.options.Context, args).Result()
	if !isXPendingIdleUnsupported(err) {
		return res, false, err
	}

	c.xpendingIdleUnsupported = true
	args.Idle = 0
	res, err = c.redis.XPendingExt(c.options.Context, args).Result()
	return res, true, err
}

func isXPendingIdleUnsupported(err error) bool {
	return redis.HasErrorPrefix(err, "syntax error") || redis.HasErrorPrefix(err, "unknown subcommand")
}

// poll constantly checks the streams using XREADGROUP to see if there are any
// messages for this consumer to process. It blocks for up to 5 seconds instead
// of blocking indefinitely so that it can periodically check to see if Shutdown
// was called.
func (c *Consumer) poll() {
	for {
		select {
		case <-c.stopPoll:
			// once the polling has stopped (i.e. there will be no more messages
			// put onto c.queue), stop all of the workers
			for i := 0; i < c.options.Concurrency; i++ {
				c.stopWorkers <- struct{}{}
			}
			return
		case <-c.options.Context.Done():
			return
		default:
			capacity := c.options.BufferSize - len(c.queue)
			if c.options.BufferSize <= 0 {
				// An unbuffered queue hands each message straight to a worker.
				capacity = 1
			} else if capacity <= 0 {
				// go-redis omits a zero COUNT, and XREADGROUP without COUNT
				// returns every new message on every stream.
				select {
				case <-c.dequeued:
				case <-c.options.Context.Done():
				}
				continue
			}
			res, err := c.redis.XReadGroup(c.options.Context, &redis.XReadGroupArgs{
				Group:    c.options.GroupName,
				Consumer: c.options.Name,
				Streams:  c.streams,
				Count:    int64(capacity),
				Block:    c.options.BlockingTimeout,
			}).Result()
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				}
				if err == redis.Nil || err == context.Canceled || c.options.Context.Err() != nil {
					continue
				}
				c.Errors <- fmt.Errorf("error reading redis stream: %w", err)
				continue
			}

			for _, r := range res {
				c.enqueue(r.Stream, r.Messages)
			}
		}
	}
}

// enqueue takes a slice of XMessages, creates corresponding Messages, and sends
// them on the centralized channel for worker goroutines to process.
func (c *Consumer) enqueue(stream string, msgs []redis.XMessage) {
	for _, m := range msgs {
		msg := &Message{
			ID:     m.ID,
			Stream: stream,
			Values: m.Values,
		}
		c.queue <- msg
	}
}

// work is called in a separate goroutine. The number of work goroutines is
// determined by Concurreny. Once it gets a message from the centralized
// channel, it calls the corrensponding ConsumerFunc depending on the stream it
// came from. If no error is returned from the ConsumerFunc, the message is
// acknowledged in Redis.
func (c *Consumer) ackMessage(ctx context.Context, stream string, id string) error {
	return c.withRetry(ctx, c.options.AckRetry, func(ctx context.Context) error {
		return c.redis.XAck(ctx, stream, c.options.GroupName, id).Err()
	})
}

func (c *Consumer) withRetry(ctx context.Context, options RetryOptions, fn func(context.Context) error) error {
	maxAttempts := options.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptCtx := ctx
		cancel := func() {}
		if options.PerAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, options.PerAttemptTimeout)
		}

		err = fn(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if attempt == maxAttempts || !isRetryableRedisError(err) || ctx.Err() != nil {
			return err
		}
		if delay := retryDelay(options, attempt); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return err
			}
		}
	}

	return err
}

func retryDelay(options RetryOptions, attempt int) time.Duration {
	delay := options.InitialBackoff
	for i := 1; i < attempt; i++ {
		delay *= 2
		if options.MaxBackoff > 0 && delay >= options.MaxBackoff {
			return options.MaxBackoff
		}
	}
	if options.MaxBackoff > 0 && delay > options.MaxBackoff {
		return options.MaxBackoff
	}
	return delay
}

func isRetryableRedisError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (c *Consumer) work() {
	defer c.wg.Done()

	for {
		msg, ok := c.take()
		if !ok {
			return
		}
		if msg == nil {
			continue
		}
		err := c.process(msg)
		if err != nil {
			c.Errors <- fmt.Errorf("error calling ConsumerFunc for %q stream and %q message: %w", msg.Stream, msg.ID, err)
			continue
		}
		err = c.ackMessage(context.Background(), msg.Stream, msg.ID)
		if err != nil {
			c.Errors <- fmt.Errorf("error acknowledging after success for %q stream and %q message: %w", msg.Stream, msg.ID, err)
			continue
		}
	}
}

// take waits for the next message for a worker to process. Every
// ReclaimShare-th take prefers a reclaim candidate and the rest prefer the
// queue; if the preferred source has nothing ready, take waits for whichever
// has a message first. A reclaim candidate is claimed before it is returned,
// and the message is nil if it could no longer be claimed. take returns false
// when the worker should stop.
func (c *Consumer) take() (*Message, bool) {
	first, second := c.queue, c.reclaimQueue
	if c.takes.Add(1)%int64(c.options.ReclaimShare) == 0 {
		first, second = second, first
	}

	var msg *Message
	from := first
	select {
	case msg = <-first:
	case <-c.stopWorkers:
		return nil, false
	case <-c.options.Context.Done():
		return nil, false
	default:
		select {
		case msg = <-first:
		case msg = <-second:
			from = second
		case <-c.stopWorkers:
			return nil, false
		case <-c.options.Context.Done():
			return nil, false
		}
	}

	if from == c.queue {
		select {
		case c.dequeued <- struct{}{}:
		default:
		}
		return msg, true
	}

	messages, err := c.claimPendingMessage(msg.Stream, msg.ID)
	if err != nil || len(messages) == 0 {
		return nil, true
	}
	// XCLAIM was requested for exactly one ID.
	m := messages[0]
	return &Message{ID: m.ID, Stream: msg.Stream, Values: m.Values}, true
}

func (c *Consumer) process(msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = fmt.Errorf("ConsumerFunc panic: %w", e)
				return
			}
			err = fmt.Errorf("ConsumerFunc panic: %v", r)
		}
	}()
	if c.consumers[msg.Stream].fnc != nil {
		err = c.consumers[msg.Stream].fnc(c.options.Context, msg)
	} else {
		err = c.consumers[msg.Stream].fn(msg)
	}
	return
}
