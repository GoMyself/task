package rocket

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/panjf2000/ants/v2"
	"github.com/valyala/fasthttp"
	"task/modules/common"
)

func batchTransferAgTask() {
	// 初始化投注记录任务队列协程池
	transferPool, _ := ants.NewPoolWithFunc(500, func(bet interface{}) {

		if payload, ok := bet.(string); ok {
			// 站内信处理
			transferHandle(payload)
		}
	})

	topic := prefix + "_transfer_ag"
	merchantConsumer.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {

			fmt.Println("transfer 收到消息：", string(msgs[i].Body))
			// 注单自动确认
			if err := transferPool.Invoke(string(msgs[i].Body)); err != nil {
				fmt.Printf("invoke error: %s\n", err.Error())
				continue
			}
		}

		return consumer.ConsumeSuccess, nil
	})
}

func transferHandle(payload string) {

	common.Log("rocketTransferAg", "transferHandle payload : %v \n", payload)

	param := map[string]interface{}{}
	m := &fasthttp.Args{}
	m.Parse(payload)
	if m.Len() == 0 {
		common.Log("rocketTransferAg", "transferHandle payload  is null\n")
		return
	}

	m.VisitAll(func(key, value []byte) {
		param[string(key)] = string(value)
	})
}
