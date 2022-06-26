package rocket

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	g "github.com/doug-martin/goqu/v9"
	"github.com/panjf2000/ants/v2"
	"github.com/valyala/fasthttp"
	"strconv"
	"strings"
	"task/modules/common"
	"time"
)

func batchMessageTask() {

	// 初始化投注记录任务队列协程池
	messagePool, _ := ants.NewPoolWithFunc(500, func(bet interface{}) {

		if payload, ok := bet.(string); ok {
			// 站内信处理
			messageHandle(payload)
		}
	})

	topic := prefix + "_message"
	merchantConsumer.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {

			fmt.Println("message 收到消息：", string(msgs[i].Body))
			// 注单自动确认
			if err := messagePool.Invoke(string(msgs[i].Body)); err != nil {
				fmt.Printf("invoke error: %s\n", err.Error())
				continue
			}
		}

		return consumer.ConsumeSuccess, nil
	})
}

func messageHandle(payload string) {

	common.Log("rocketMessage", "messageHandle payload : %v \n", payload)

	param := map[string]interface{}{}
	m := &fasthttp.Args{}
	m.Parse(payload)
	if m.Len() == 0 {
		common.Log("rocketMessage", "messageHandle payload  is null\n")
		return
	}

	m.VisitAll(func(key, value []byte) {
		param[string(key)] = string(value)
	})

	//1 发送站内信 2 删除站内信
	flag, ok := param["flag"].(string)
	if !ok {
		common.Log("rocketMessage", "messageHandle flag param null : %v \n", param)
		return
	}

	switch flag {
	case "1":
		sendHandle(param)
	case "2":
		deleteHandle(param)
	}
}

func deleteHandle(param map[string]interface{}) {

	msgID, ok := param["message_id"].(string)
	if !ok {
		common.Log("rocketMessage", "deleteHandle msgID param null : %v \n", param)
		return
	}

	var tss []string
	ex := g.Ex{
		"prefix":     prefix,
		"message_id": msgID,
	}
	query, _, _ := dialect.From("messages").Select("ts").Where(ex).ToSQL()
	fmt.Println(query)
	err := td.Select(&tss, query)
	if err != nil {
		common.Log("rocketMessage", "query : %s, error : %v \n", query, err)
		return
	}

	var records []g.Record
	for _, v := range tss {
		// 2022-06-07T16:28:26.285+07:00
		t, err := time.ParseInLocation("2006-01-02T15:04:05.999999+07:00", v, loc)
		if err != nil {
			common.Log("message", "query : %s, error : %v \n", query, err)
			return
		}

		record := g.Record{
			"ts":        t.UnixMicro(),
			"is_delete": 1,
		}
		records = append(records, record)
	}
	query, _, _ = dialect.Insert("messages").Rows(records).ToSQL()
	fmt.Println(query)
	_, err = td.Exec(query)
	if err != nil {
		fmt.Println("insert messages = ", err.Error(), records)
	}
}

func sendHandle(param map[string]interface{}) {

	msgID, ok := param["message_id"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle msgID param null : %v \n", param)
		return
	}
	//标题
	title, ok := param["title"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle title param null : %v \n", param)
		return
	}
	//副标题
	subTitle, ok := param["sub_title"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle sub_title param null : %v \n", param)
		return
	}
	//内容
	content, ok := param["content"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle content param null : %v \n", param)
		return
	}
	//0不置顶 1置顶
	isTop, ok := param["is_top"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle is_top param null : %v \n", param)
		return
	}

	isTops := map[string]bool{
		"0": true,
		"1": true,
	}
	if _, ok := isTops[isTop]; !ok {
		common.Log("rocketMessage", "sendHandle is_top param err : %s \n", isTop)
		return
	}

	iIsTop, _ := strconv.Atoi(isTop)
	//0不推送 1推送
	isPush, ok := param["is_push"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle is_push param null : %v \n", param)
		return
	}

	//0非vip站内信 1vip站内信
	isVip, ok := param["is_vip"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle is_vip param null : %v \n", param)
		return
	}

	isVips := map[string]bool{
		"0": true,
		"1": true,
	}
	if _, ok := isVips[isVip]; !ok {
		common.Log("rocketMessage", "sendHandle is_vip param err : %s \n", isVip)
		return
	}

	iIsVip, _ := strconv.Atoi(isVip)
	//1站内消息 2活动消息
	ty, ok := param["ty"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle ty param null : %v \n", param)
		return
	}

	tys := map[string]bool{
		"1": true,
		"2": true,
	}
	if _, ok := tys[ty]; !ok {
		common.Log("rocketMessage", "sendHandle ty param err : %s \n", ty)
		return
	}

	iTy, _ := strconv.Atoi(ty)
	//发送人名
	sendName, ok := param["send_name"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle send_name param null : %v \n", param)
		return
	}
	//商户前缀
	prefix, ok := param["prefix"].(string)
	if !ok {
		common.Log("rocketMessage", "sendHandle prefix param null : %v \n", param)
		return
	}

	var sendState int
	ex := g.Ex{
		"id": msgID,
	}
	query, _, _ := dialect.From("tbl_messages").Select("send_state").Where(ex).ToSQL()
	fmt.Println(query)
	err := db.Get(&sendState, query)
	if err != nil {
		common.Log("rocketMessage", "query : %s, error : %v \n", query, err)
		return
	}

	if sendState == 2 {
		common.Log("rocketMessage", "duplicate process \n")
		return
	}

	switch isVip {
	case "0": //站内消息
		//会员名
		usernames, ok := param["usernames"].(string)
		if !ok || usernames == "" {
			common.Log("rocketMessage", "sendHandle level param null : %v \n", param)
			return
		}

		names := strings.Split(usernames, ",")
		count := len(names)
		p := count / 100
		l := count % 100
		// 分页发送
		for j := 0; j < p; j++ {
			offset := j * 100
			err := sendMessage(msgID, title, subTitle, content, isPush, sendName, prefix, iIsTop, iIsVip, iTy, names[offset:offset+100])
			if err != nil {
				return
			}
		}
		// 最后一页
		if l > 0 {
			err := sendMessage(msgID, title, subTitle, content, isPush, sendName, prefix, iIsTop, iIsVip, iTy, names[p*100:])
			if err != nil {
				return
			}
		}
	case "1": //vip站内信
		//会员等级
		level, ok := param["level"].(string)
		if !ok {
			common.Log("rocketMessage", "sendHandle level param null : %v \n", param)
			return
		}

		lvs := strings.Split(level, ",")
		for _, v := range lvs {
			err := sendLevelMessage(msgID, title, subTitle, content, isPush, sendName, prefix, v, iIsTop, iIsVip, iTy)
			if err != nil {
				return
			}
		}
	}

	record := g.Record{
		"send_state": 2,
		"send_at":    time.Now().Unix(),
	}
	query, _, _ = dialect.Update("tbl_messages").Set(record).Where(ex).ToSQL()
	fmt.Println(query)
	_, err = db.Exec(query)
	if err != nil {
		common.Log("rocketMessage", "query : %s, error : %v \n", query, err)
		return
	}
}

func sendLevelMessage(msgID, title, subTitle, content, isPush, sendName, prefix, level string, isTop, isVip, ty int) error {

	ex := g.Ex{
		"level": level,
	}
	count, err := common.MembersCount(db, ex)
	if err != nil {
		common.Log("rocketMessage", "error : %v", err)
		return err
	}

	fmt.Printf("count : %d\n", count)

	if count == 0 {
		return nil
	}

	p := count / 100
	l := count % 100
	if l > 0 {
		p += 1
	}

	for j := 1; j <= p; j++ {
		ns, err := common.MembersPageNames(db, j, 100, ex)
		if err != nil {
			common.Log("rocketMessage", "MembersPageNames error : %v \n", err)
			return err
		}

		err = sendMessage(msgID, title, subTitle, content, isPush, sendName, prefix, isTop, isVip, ty, ns)
		if err != nil {
			common.Log("rocketMessage", "sendMessage error : %v \n", err)
			return err
		}
	}

	return nil
}

func sendMessage(msgID, title, subTitle, content, isPush, sendName, prefix string, isTop, isVip, ty int, names []string) error {

	record := g.Record{
		"message_id": msgID,
		"title":      title,
		"sub_title":  subTitle,
		"content":    content,
		"send_name":  sendName,
		"prefix":     prefix,
		"is_top":     isTop,
		"is_vip":     isVip,
		"is_read":    0,
		"is_delete":  0,
		"send_at":    time.Now().Unix(),
		"ty":         ty,
	}
	for k, v := range names {
		record["ts"] = time.Now().UnixMicro() + int64(k)
		record["username"] = v
		query, _, _ := dialect.Insert("messages").Rows(record).ToSQL()
		fmt.Println(query)
		_, err := td.Exec(query)
		if err != nil {
			fmt.Println("insert messages = ", err.Error(), query)
		}
	}

	return nil
}
