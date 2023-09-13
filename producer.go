package nsq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"github.com/nsqio/go-nsq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	uuid "github.com/satori/go.uuid"
	"reflect"
	"time"
)

type ProducerConfig struct {
	NsqdAddr  string
	NsqLogger logger.Logger
	MsgLogger logger.Logger
}

type ProducerOption func(*ProducerConfig)

func ProducerWithNsqdAddr(val string) ProducerOption {
	return func(opts *ProducerConfig) {
		opts.NsqdAddr = val
	}
}

func defaultProducerConfig(c *config.Config) *ProducerConfig {
	return &ProducerConfig{
		NsqdAddr:  c.GetString("nsq.nsqdAddr"),
		NsqLogger: logger.NewZapWithConfig(c, "nsq-producer", "error"),
		MsgLogger: logger.NewZapWithConfig(c, "mq-producer", "info"),
	}
}

func NewProducerWithConfig(c *config.Config, opts ...ProducerOption) *Producer {
	defaultConfig := defaultProducerConfig(c)
	for _, apply := range opts {
		if apply != nil {
			apply(defaultConfig)
		}
	}
	return NewProducer(defaultConfig)
}

func NewProducer(c *ProducerConfig) *Producer {

	if c == nil {
		panic("config 必须设置")
	}
	if c.NsqdAddr == "" {
		panic("NsqdAddr 必须设置")
	}
	if c.NsqLogger == nil {
		panic("NsqLogger 必须设置")
	}
	if c.MsgLogger == nil {
		panic("MsgLogger 必须设置")
	}

	nsqConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(c.NsqdAddr, nsqConfig)
	if err != nil {
		panic(err)
	}

	producer.SetLogger(Logger(c.NsqLogger), Level(c.NsqLogger))

	return &Producer{
		c, producer,
	}
}

type Producer struct {
	*ProducerConfig
	*nsq.Producer
}

func (p *Producer) MessageFormat(data interface{}) (string, error) {
	var body []byte
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type() == reflect.TypeOf(body) {
		body = data.([]byte)
	} else {
		if reflect.TypeOf(data).Kind() == reflect.String {
			body = []byte(data.(string))
		} else {
			var err error
			body, err = json.Marshal(data)
			if err != nil {
				return "", err
			}
		}
	}
	return string(body), nil
}

func (p *Producer) SendSync(topic string, data interface{}, opts ...SendOption) (err error) {
	var startTime = time.Now()
	c := &SendConfig{}
	for _, apply := range opts {
		apply(c)
	}
	headers, span := ProducerTrace(topic, c.Ctx)
	defer func() {
		if span != nil {
			if err != nil {
				ext.LogError(span, err)
			}
			span.Finish()
		}
	}()
	body, err := p.MessageFormat(data)

	defer func() {
		p.MsgLogger.Info(body,
			p.MsgLogger.Field("time", startTime),
			p.MsgLogger.Field("topic", topic),
			p.MsgLogger.Field("request_id", headers["x-request-id"]),
			p.MsgLogger.Field("error", err),
		)
	}()

	if err != nil {
		return err
	}

	message, err := json.Marshal(&Message{
		Headers: headers,
		Body:    body,
	})
	if err != nil {
		return err
	}

	if c.Delay != 0 {
		return p.DeferredPublish(topic, c.Delay, message)
	} else {
		return p.Publish(topic, message)
	}
}

func (p *Producer) SimpleSendSync(topic string, data string) (err error) {
	return p.Publish(topic, []byte(data))
}

func (p *Producer) DeferredSendSync(topic string, delay time.Duration, data interface{}, opts ...SendOption) error {
	opts = append(opts, WithDelay(delay))
	return p.SendSync(topic, data, opts...)
}

type Message struct {
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type SendConfig struct {
	Ctx   context.Context
	Delay time.Duration
}

type SendOption func(*SendConfig)

func WithCtx(val context.Context) SendOption {
	return func(opts *SendConfig) {
		opts.Ctx = val
	}
}
func WithDelay(val time.Duration) SendOption {
	return func(opts *SendConfig) {
		opts.Delay = val
	}
}

func ProducerTrace(topic string, ctx context.Context) (map[string]string, opentracing.Span) {
	var headers = make(map[string]string)
	if ctx != nil {
		var requestId = ""
		if id := ctx.Value("x-request-id"); id != nil {
			requestId = id.(string)
		} else {
			requestId = uuid.NewV4().String()
		}
		headers["x-request-id"] = requestId
		span, _ := opentracing.StartSpanFromContext(ctx, topic+":P")
		if span != nil {
			span.SetTag("x-request-id", requestId)
			ext.SpanKindProducer.Set(span)
			if err := span.Tracer().Inject(
				span.Context(),
				opentracing.TextMap,
				opentracing.TextMapCarrier(headers),
			); err != nil {
				fmt.Println("ProducerTrace Inject Err", err)
			}
		}
		return headers, span
	}
	return headers, nil
}
