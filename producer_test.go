package nsq

import (
	"context"
	"github.com/go-tron/logger"
	"github.com/go-tron/tracer"
	"strconv"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	_, closer := tracer.NewJaeger("nsq-test", "127.0.0.1:9411")
	defer closer.Close()

	producer := NewProducer(&ProducerConfig{
		NsqdAddr:  "127.0.0.1:4150",
		NsqLogger: logger.NewZap("nsq-producer", "error"),
		MsgLogger: logger.NewZap("mq-producer", "info"),
	})

	for i := 0; i < 100; i++ {
		go func(int2 int) {
			producer.SendSync("test-topic", []byte("hi"+strconv.Itoa(int2)), WithCtx(context.Background()))
		}(i)
	}

	time.Sleep(time.Hour)
}
