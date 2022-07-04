package rocket

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	g "github.com/doug-martin/goqu/v9"
	"github.com/panjf2000/ants/v2"
	"github.com/valyala/fasthttp"
)

func batchTransferAgTask() {
	// 初始化投注记录任务队列协程池
	transferPool, _ := ants.NewPoolWithFunc(500, func(bet interface{}) {

		if payload, ok := bet.(map[string]string); ok {
			// 站内信处理
			transferHandle(payload)
		}
	})

	topic := prefix + "_transfer_ag"
	merchantConsumer.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {

			fmt.Println("batchTransferTask 收到消息：", msgs[i].MsgId)

			message := string(msgs[i].Body)

			param := map[string]string{}
			m := &fasthttp.Args{}
			m.Parse(message)
			if m.Len() == 0 {
				fmt.Println("transfer_ag Parse = ", message)
				return consumer.ConsumeSuccess, nil
			}

			m.VisitAll(func(key, value []byte) {
				param[string(key)] = string(value)
			})

			// 注单自动确认
			if err := transferPool.Invoke(param); err != nil {
				fmt.Printf("batchTransferTask invoke error: %s\n", err.Error())
				continue
			}
		}

		return consumer.ConsumeSuccess, nil
	})
}

func transferHandle(param map[string]string) error {

	fmt.Println(param)
	uid, ok := param["uid"]
	if !ok {
		fmt.Println("transferHandle uid null param = ", param)
		return nil
	}

	prefix, ok := param["prefix"]
	if !ok {
		fmt.Println("transferHandle prefix null param = ", param)
		return nil
	}

	parentUid, ok := param["parent_uid"]
	if !ok {
		fmt.Println("transferHandle parent_uid null param = ", param)
		return nil
	}

	parentName, ok := param["parent_name"]
	if !ok {
		fmt.Println("transferHandle parent_name null param = ", param)
		return nil
	}

	topUid, ok := param["top_uid"]
	if !ok {
		fmt.Println("transferHandle top_uid null param = ", param)
		return nil
	}

	topName, ok := param["top_name"]
	if !ok {
		fmt.Println("transferHandle top_name null param = ", param)
		return nil
	}

	ex := g.Ex{
		"uid":    uid,
		"prefix": prefix,
	}
	record := g.Record{
		"parent_uid":  parentUid,
		"parent_name": parentName,
		"top_uid":     topUid,
		"top_name":    topName,
	}

	query, _, _ := dialect.Update("tbl_member_dividend").Set(record).Where(ex).ToSQL()
	_, err := db.Exec(query)
	if err != nil {
		fmt.Printf("transferHandle  query : %s  \n err : %s", query, err.Error())
		return err
	}

	query, _, _ = dialect.Update("tbl_member_adjust").Set(record).Where(ex).ToSQL()
	_, err = db.Exec(query)
	if err != nil {
		fmt.Printf("transferHandle  query : %s  \n err : %s", query, err.Error())
		return err
	}

	query, _, _ = dialect.Update("tbl_deposit").Set(record).Where(ex).ToSQL()
	_, err = db.Exec(query)
	if err != nil {
		fmt.Printf("transferHandle  query : %s  \n err : %s", query, err.Error())
		return err
	}

	query, _, _ = dialect.Update("tbl_withdraw").Set(record).Where(ex).ToSQL()
	_, err = db.Exec(query)
	if err != nil {
		fmt.Printf("transferHandle  query : %s  \n err : %s", query, err.Error())
		return err
	}

	return nil
}
