package message

import (
	"context"
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"github.com/olivere/elastic/v7"
	"github.com/panjf2000/ants/v2"
	cpool "github.com/silenceper/pool"
	"strconv"
	"strings"
	"task/contrib/conn"
	"task/contrib/helper"
	"task/modules/common"
	"time"
)

var (
	db       *sqlx.DB
	esCli    *elastic.Client
	beanPool cpool.Pool
	ctx      = context.Background()
	prefix   string
	esPrefix string
	dialect  = g.Dialect("mysql")
)

func Parse(endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)

	prefix = conf.Prefix
	esPrefix = conf.EsPrefix

	// 初始化db
	db = conn.InitDB(conf.Db.Master.Addr, conf.Db.Master.MaxIdleConn, conf.Db.Master.MaxIdleConn)
	// 初始化beanstalk
	beanPool = conn.InitBeanstalk(conf.Beanstalkd.Addr, 50, 50, 100)
	// 初始化es
	esCli = conn.InitES(conf.Es.Host, conf.Es.Username, conf.Es.Password)

	// 初始化td
	td := conn.InitTD(conf.Td.Addr, conf.Td.MaxIdleConn, conf.Td.MaxOpenConn)
	common.InitTD(td)

	batchMessageTask()
}

// 场馆转账订单确认
func batchMessageTask() {

	// 初始化场馆转账订单任务队列协程池
	messagePool, _ := ants.NewPoolWithFunc(500, func(payload interface{}) {

		if fn, ok := payload.(common.BeansFnParam); ok {
			// 场馆转账订单确认
			messageHandle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	topic := fmt.Sprintf("%s_message", prefix)
	attr := common.BeansWatcherAttr{
		TubeName:       topic,
		ReserveTimeOut: 2 * time.Minute,
		Pool:           messagePool,
	}

	// 场馆转账订单确认队列
	common.BeanstalkWatcher(beanPool, attr)
}

func messageHandle(param map[string]interface{}) {

	common.Log("message", "messageHandle param : %v \n", param)

	//1 发送站内信 2 删除站内信
	flag, ok := param["flag"].(string)
	if !ok {
		common.Log("message", "messageHandle flag param null : %v \n", param)
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

	msgID, ok := param["msg_id"].(string)
	if !ok {
		common.Log("message", "deleteHandle msgID param null : %v \n", param)
		return
	}

	query := elastic.NewBoolQuery().Filter(
		elastic.NewTermQuery("msg_id", msgID),
		elastic.NewTermQuery("prefix", prefix))
	id, ok := param["id"].(string)
	if ok {
		if id != "" {
			var ids []interface{}
			for _, v := range strings.Split(id, ",") {
				ids = append(ids, v)
			}
			query = elastic.NewBoolQuery().Filter(
				elastic.NewTermQuery("msg_id", msgID),
				elastic.NewTermsQuery("_id", ids...),
				elastic.NewTermQuery("prefix", prefix))
		}
	}

	_, err := esCli.DeleteByQuery(esPrefix + "messages").
		Query(query).ProceedOnVersionConflict().Do(ctx)
	if err != nil {
		common.Log("message", "deleteHandle error : %v \n", err)
		return
	}
}

func sendHandle(param map[string]interface{}) {

	msgID, ok := param["msg_id"].(string)
	if !ok {
		common.Log("message", "sendHandle msgID param null : %v \n", param)
		return
	}
	//标题
	title, ok := param["title"].(string)
	if !ok {
		common.Log("message", "sendHandle title param null : %v \n", param)
		return
	}
	//副标题
	subTitle, ok := param["sub_title"].(string)
	if !ok {
		common.Log("message", "sendHandle sub_title param null : %v \n", param)
		return
	}
	//内容
	content, ok := param["content"].(string)
	if !ok {
		common.Log("message", "sendHandle content param null : %v \n", param)
		return
	}
	//0不置顶 1置顶
	isTop, ok := param["is_top"].(string)
	if !ok {
		common.Log("message", "sendHandle is_top param null : %v \n", param)
		return
	}

	isTops := map[string]bool{
		"0": true,
		"1": true,
	}
	if _, ok := isTops[isTop]; !ok {
		common.Log("message", "sendHandle is_top param err : %s \n", isTop)
		return
	}

	iIsTop, _ := strconv.Atoi(isTop)
	//0不推送 1推送
	isPush, ok := param["is_push"].(string)
	if !ok {
		common.Log("message", "sendHandle is_push param null : %v \n", param)
		return
	}

	//0非vip站内信 1vip站内信
	isVip, ok := param["is_vip"].(string)
	if !ok {
		common.Log("message", "sendHandle is_vip param null : %v \n", param)
		return
	}

	isVips := map[string]bool{
		"0": true,
		"1": true,
	}
	if _, ok := isVips[isVip]; !ok {
		common.Log("message", "sendHandle is_vip param err : %s \n", isVip)
		return
	}

	iIsVip, _ := strconv.Atoi(isVip)
	//1站内消息 2活动消息
	ty, ok := param["ty"].(string)
	if !ok {
		common.Log("message", "sendHandle ty param null : %v \n", param)
		return
	}

	tys := map[string]bool{
		"1": true,
		"2": true,
	}
	if _, ok := tys[ty]; !ok {
		common.Log("message", "sendHandle ty param err : %s \n", ty)
		return
	}

	iTy, _ := strconv.Atoi(ty)
	//发送人名
	sendName, ok := param["send_name"].(string)
	if !ok {
		common.Log("message", "sendHandle send_name param null : %v \n", param)
		return
	}
	//商户前缀
	prefix, ok := param["prefix"].(string)
	if !ok {
		common.Log("message", "sendHandle prefix param null : %v \n", param)
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
		common.Log("message", "query : %s, error : %v \n", query, err)
		return
	}

	if sendState == 2 {
		common.Log("message", "duplicate process \n")
		return
	}

	switch isVip {
	case "0": //站内消息
		//会员名
		usernames, ok := param["usernames"].(string)
		if !ok || usernames == "" {
			common.Log("message", "sendHandle level param null : %v \n", param)
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
			common.Log("message", "sendHandle level param null : %v \n", param)
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
		common.Log("message", "query : %s, error : %v \n", query, err)
		return
	}
}

func sendLevelMessage(msgID, title, subTitle, content, isPush, sendName, prefix, level string, isTop, isVip, ty int) error {

	ex := g.Ex{
		"level": level,
	}
	count, err := common.MembersCount(db, ex)
	if err != nil {
		common.Log("message", "error : %v", err)
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
			common.Log("message", "MembersPageNames error : %v \n", err)
			return err
		}

		err = sendMessage(msgID, title, subTitle, content, isPush, sendName, prefix, isTop, isVip, ty, ns)
		if err != nil {
			common.Log("message", "sendMessage error : %v \n", err)
			return err
		}
	}

	return nil
}

func sendMessage(msgID, title, subTitle, content, isPush, sendName, prefix string, isTop, isVip, ty int, names []string) error {

	data := common.Message{
		MsgID:    msgID,
		Title:    title,
		SubTitle: subTitle,
		Content:  content,
		IsTop:    isTop,
		IsVip:    isVip,
		IsRead:   0,
		Ty:       ty,
		SendName: sendName,
		SendAt:   time.Now().Unix(),
		Prefix:   prefix,
	}
	bulkRequest := esCli.Bulk().Index(esPrefix + "messages")
	for _, v := range names {
		data.Username = v
		doc := elastic.NewBulkIndexRequest().Id(helper.GenId()).Doc(data)
		bulkRequest = bulkRequest.Add(doc)
	}

	_, err := bulkRequest.Refresh("wait_for").Do(ctx)
	if err != nil {
		return err
	}

	if isPush == "1" {
		// todo 推送
	}

	return nil
}
