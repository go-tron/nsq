package nsq

import (
	"context"
	"fmt"
	"github.com/avast/retry-go/v4"
	baseError "github.com/go-tron/base-error"
	"github.com/go-tron/logger"
	"github.com/go-tron/tracer"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	_, closer := tracer.NewJaeger("nsq-test", "127.0.0.1:9411")
	defer closer.Close()

	//var retryDelays = []time.Duration{
	//	0,
	//	time.Second * 1,
	//	time.Second * 2,
	//	time.Second * 3,
	//	time.Second * 4,
	//	time.Second * 5,
	//	time.Second * 6,
	//}
	var i = 0
	_, err := NewConsumer(&ConsumerConfig{
		NsqLookUpAddr: "127.0.0.1:4161",
		Channel:       "test-01",
		Topic:         "test-topic",
		//Retry:            NewRetry(time.Second*1, time.Second*1, 5),
		MaxInFlight: 200,
		Concurrent:  true,
		Retry:       NewRetry(time.Second, time.Second, 10),
		LocalRetry: func(n uint, err error, config *retry.Config) time.Duration {
			return time.Millisecond * 200
		},
		LocalRetryAttempts: 10,
		//RetryStrategy: func(attempts uint16) (delay time.Duration) {
		//	var i = int(attempts)
		//	if i > len(retryDelays)-1 {
		//		i = len(retryDelays) - 1
		//	}
		//	return retryDelays[i]
		//},
		BackoffDisabled: true,
		NsqLogger:       logger.NewZap("nsq-consumer", "error"),
		MsgLogger:       logger.NewZap("mq-consumer", "info"),
		Handler: func(ctx context.Context, msg []byte, finished bool) error {
			i++
			fmt.Println("msg", i, string(msg))
			return baseError.System("1", "asd")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("start consumer")
	time.Sleep(time.Hour)
}

func TestRetryTime(t *testing.T) {
	r := NewRetry(time.Second*60, time.Minute*10, 20)
	var i time.Duration = 1
	var total time.Duration = 0
	for uint16(i) <= r.MaxAttempts {
		var v time.Duration = 0
		if r.DefaultRequeueDelay*i > r.MaxRequeueDelay {
			v = r.MaxRequeueDelay
		} else {
			v = r.DefaultRequeueDelay * i
		}
		t.Log(i, v.Minutes())
		total += v
		i++
	}
	t.Log("total", total.Hours())
}
