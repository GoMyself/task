package sms

import (
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/panjf2000/ants/v2"
	cpool "github.com/silenceper/pool"
	"strconv"
	"task/contrib/conn"
	"task/modules/common"
	"time"
)

var (
	td       *sqlx.DB
	beanPool cpool.Pool
	prefix   string
	dialect  = g.Dialect("mysql")
)

func Parse(service *common.BuildInfo, endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	prefix = conf.Prefix
	// 初始化beanstalk
	beanPool = conn.InitBeanstalk(conf.Beanstalkd.Addr, 50, 50, 100)
	// 初始化td
	td = conn.InitTD(conf.Td.Addr, conf.Td.MaxIdleConn, conf.Td.MaxOpenConn)
	common.InitTD(td, prefix)
	go service.Start()

	tdTask()
}

// 批量红利派发
func tdTask() {

	common.Log("sms", "短信自动过期脚本开始")

	// 初始化红利批量发放任务队列协程池
	tdPool, _ := ants.NewPoolWithFunc(10, func(payload interface{}) {

		if fn, ok := payload.(common.BeansFnParam); ok {
			tdHandle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	topic := fmt.Sprintf("%s_sms", prefix)
	fmt.Printf("topic : %s\n", topic)
	attr := common.BeansWatcherAttr{
		TubeName:       topic,
		ReserveTimeOut: 2 * time.Minute,
		Pool:           tdPool,
	}

	common.BeanstalkWatcher(beanPool, attr)
}

type SMSState struct {
	State string `json:"state" db:"state"`
}

//短信自动过期
func tdHandle(m map[string]interface{}) {

	fmt.Printf("bean data %#v \n", m)
	if m == nil {
		return
	}
	ts, ok := m["ts"].(string)
	if !ok {
		return
	}

	its, e := strconv.ParseInt(ts, 10, 64)
	if e != nil {
		fmt.Println("parse int err:", e)
	}

	t := dialect.From("sms_log")
	ex := g.Ex{
		"ts": its,
	}

	data := make([]SMSState, 0)

	query, _, _ := t.Select("state").Where(ex).ToSQL()
	fmt.Println("read query = ", query)

	err := td.Select(&data, query)
	if err != nil {
		common.Log("sms", err.Error())
	}

	fmt.Println("state = ", data[0].State)
	fmt.Println("==== Will Update TD ===")

	if data[0].State == "0" {
		tdInsert("sms_log", g.Record{
			"ts":         its,
			"state":      "2",
			"updated_at": time.Now().Unix(),
		})
	}
	fmt.Println("==== End Update TD ===")
}

func tdInsert(tbl string, record g.Record) {

	query, _, _ := dialect.Insert(tbl).Rows(record).ToSQL()
	fmt.Println(query)
	_, err := td.Exec(query)
	if err != nil {
		fmt.Println(err)
		common.Log("sms", "update td = error : %s , sql : %s", err.Error(), query)
	}
}
