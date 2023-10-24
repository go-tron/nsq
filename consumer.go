package nsq

import (
	"context"
	"errors"
	"fmt"
	baseError "github.com/go-tron/base-error"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"github.com/jinzhu/copier"
	"github.com/nsqio/go-nsq"
	"reflect"
	"time"
)

type Retry struct {
	DefaultRequeueDelay time.Duration
	MaxRequeueDelay     time.Duration
	MaxAttempts         uint16
}

func NewRetry(defaultRequeueDelay time.Duration, maxRequeueDelay time.Duration, maxAttempts uint16) *Retry {
	return &Retry{
		defaultRequeueDelay, maxRequeueDelay, maxAttempts,
	}
}

func defaultRetry() *Retry {
	return NewRetry(time.Second*60, time.Minute*10, 20)
}

type Handler = func(ctx context.Context, msg []byte, finished bool) error

type ConsumerConfig struct {
	NsqLookUpAddr    string
	Broadcasting     bool
	Channel          string
	Name             string
	ServerId         string
	Topic            string
	Retry            *Retry
	RetryMaxAttempts uint16
	RetryStrategy    func(attempts uint16) (delay time.Duration)
	BackoffDisabled  bool
	NsqLogger        logger.Logger
	MsgLogger        logger.Logger
	MsgLoggerLevel   string
	Handler          Handler
	SimpleHandler    func([]byte) error
}

type ConsumerOption func(*ConsumerConfig)

func ConsumerWithNsqLookUpAddr(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.NsqLookUpAddr = val
	}
}
func ConsumerWithBroadcasting() ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Broadcasting = true
	}
}
func ConsumerWithName(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Name = val
	}
}
func ConsumerWithChannel(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Channel = val
	}
}
func ConsumerWithServerId(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.ServerId = val
	}
}
func ConsumerWithTopic(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Topic = val
	}
}
func ConsumerWithRetry(val *Retry) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Retry = val
	}
}
func ConsumerWithBackoffDisabled() ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.BackoffDisabled = true
	}
}
func ConsumerWithHandler(val Handler) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Handler = val
	}
}

func defaultConsumerConfig(c *config.Config) *ConsumerConfig {
	return &ConsumerConfig{
		NsqLookUpAddr: c.GetString("nsq.nsqLookUpAddr"),
		Channel:       c.GetString("application.name"),
		ServerId:      c.GetString("cluster.nodeName"),
		NsqLogger:     logger.NewZapWithConfig(c, "nsq-consumer", "error"),
		MsgLogger:     logger.NewZapWithConfig(c, "mq-consumer", "info"),
	}
}

func NewConsumerWithConfig(c *config.Config, consumerConfig *ConsumerConfig, opts ...ConsumerOption) (*Consumer, error) {
	defaultConfig := defaultConsumerConfig(c)
	if err := copier.CopyWithOption(defaultConfig, consumerConfig, copier.Option{
		IgnoreEmpty: true,
	}); err != nil {
		return nil, err
	}
	for _, apply := range opts {
		if apply != nil {
			apply(defaultConfig)
		}
	}
	return NewConsumer(defaultConfig)
}

func NewConsumer(c *ConsumerConfig) (*Consumer, error) {
	if c == nil {
		return nil, errors.New("config 必须设置")
	}
	if c.NsqLookUpAddr == "" {
		return nil, errors.New("NsqLookUpAddr 必须设置")
	}
	if c.Channel == "" {
		return nil, errors.New("Channel 必须设置")
	}
	if c.Topic == "" {
		return nil, errors.New("Topic 必须设置")
	}
	if c.Handler == nil && c.SimpleHandler == nil {
		return nil, errors.New("Handler 必须设置")
	}
	if c.NsqLogger == nil {
		panic("NsqLogger 必须设置")
	}
	if c.MsgLogger == nil {
		panic("MsgLogger 必须设置")
	}
	if c.MsgLoggerLevel == "" {
		c.MsgLoggerLevel = "error"
	}

	if c.Name != "" {
		c.Channel = c.Channel + "-" + c.Name
	}
	if c.Broadcasting {
		c.Channel = c.Channel + "-" + c.ServerId
	}

	nsqConfig := nsq.NewConfig()

	if c.BackoffDisabled {
		nsqConfig.MaxBackoffDuration = 0
	}

	if c.Retry == nil {
		c.Retry = defaultRetry()
	}
	if c.RetryMaxAttempts > 0 {
		c.Retry.MaxAttempts = c.RetryMaxAttempts
	}
	nsqConfig.DefaultRequeueDelay = c.Retry.DefaultRequeueDelay
	nsqConfig.MaxRequeueDelay = c.Retry.MaxRequeueDelay
	nsqConfig.MaxAttempts = c.Retry.MaxAttempts

	consumer, err := nsq.NewConsumer(c.Topic, c.Channel, nsqConfig)
	if err != nil {
		return nil, err
	}

	consumer.SetLogger(Logger(c.NsqLogger), Level(c.NsqLogger))

	if c.Handler != nil {
		consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
			var ignoreErr error
			var handlerErr error
			var startTime = time.Now()
			defer func() {
				latency := time.Since(startTime).Milliseconds()
				if handlerErr != nil || c.MsgLoggerLevel == "info" {
					c.MsgLogger.Info(
						string(m.Body),
						c.MsgLogger.Field("time", startTime),
						c.MsgLogger.Field("latency", latency),
						c.MsgLogger.Field("topic", c.Topic),
						c.MsgLogger.Field("id", fmt.Sprintf("%s", m.ID)),
						c.MsgLogger.Field("attempts", m.Attempts),
						c.MsgLogger.Field("ignore_err", ignoreErr),
						c.MsgLogger.Field("error", handlerErr),
					)
				}
			}()

			finished := c.Retry.MaxAttempts == m.Attempts
			if handlerErr = c.Handler(context.Background(), m.Body, finished); handlerErr != nil {
				if reflect.TypeOf(handlerErr).String() == "*baseError.Error" && !handlerErr.(*baseError.Error).System {
					ignoreErr = handlerErr
					handlerErr = nil
				}
				if c.RetryStrategy != nil {
					if handlerErr != nil && !finished {
						delay := c.RetryStrategy(m.Attempts)
						if delay == 0 {
							delay = time.Minute
						}
						m.RequeueWithoutBackoff(delay)
					}
					return nil
				}
				return handlerErr
			}
			return nil
		}))
	}

	if c.SimpleHandler != nil {
		consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) (err error) {
			return c.SimpleHandler(m.Body)
		}))
	}

	err = consumer.ConnectToNSQLookupd(c.NsqLookUpAddr)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		c, consumer,
	}, nil
}

type Consumer struct {
	*ConsumerConfig
	*nsq.Consumer
}

func (c *Consumer) Disconnect() error {
	c.Stop()
	return c.DisconnectFromNSQLookupd(c.NsqLookUpAddr)
}

var PostRetryDelays = []time.Duration{
	0,
	time.Second * 30,
	time.Minute,
	time.Minute * 2,
	time.Minute * 5,
	time.Minute * 10,
	time.Minute * 30,
	time.Hour,
	time.Hour * 2,
	time.Hour * 4,
}

func PostRetryStrategy(attempts uint16) (delay time.Duration) {
	var i = int(attempts)
	if i > len(PostRetryDelays)-1 {
		i = len(PostRetryDelays) - 1
	}
	return PostRetryDelays[i]
}

var QueryRetryDelays = []time.Duration{
	0,
	time.Second * 10,
	time.Second * 10,
	time.Second * 10,
	time.Second * 10,
	time.Second * 10,
	time.Second * 10,
	time.Second * 30,
	time.Second * 30,
	time.Second * 30,
	time.Second * 30,
	time.Second * 30,
	time.Second * 30,
	time.Minute,
	time.Minute * 2,
	time.Minute * 5,
	time.Minute * 10,
	time.Minute * 30,
	time.Hour,
	time.Hour * 2,
	time.Hour * 4,
}

func QueryRetryStrategy(attempts uint16) (delay time.Duration) {
	var i = int(attempts)
	if i > len(QueryRetryDelays)-1 {
		i = len(QueryRetryDelays) - 1
	}
	return QueryRetryDelays[i]
}
