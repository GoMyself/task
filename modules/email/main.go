package email

import (
	"database/sql"
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

	common.Log("mail", "短信自动过期脚本开始")

	// 初始化红利批量发放任务队列协程池
	tdPool, _ := ants.NewPoolWithFunc(10, func(payload interface{}) {

		if fn, ok := payload.(common.BeansFnParam); ok {
			tdHandle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	topic := fmt.Sprintf("%s_mail", prefix)
	fmt.Printf("topic : %s\n", topic)
	attr := common.BeansWatcherAttr{
		TubeName:       topic,
		ReserveTimeOut: 2 * time.Minute,
		Pool:           tdPool,
	}

	common.BeanstalkWatcher(beanPool, attr)
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
		return
	}

	var state int
	ex := g.Ex{
		"ts": its,
	}
	query, _, _ := dialect.From("mail_log").Select("state").Where(ex).ToSQL()
	fmt.Println(query)
	err := td.Get(&state, query)
	if err != nil {
		if err != sql.ErrNoRows {
			common.Log("mail", err.Error())
		}

		return
	}

	fmt.Println("state = ", state)
	fmt.Println("==== Will Update TD ===")

	if state == 1 {
		record := g.Record{
			"ts":         its,
			"state":      "3",
			"updated_at": time.Now().Unix(),
		}
		query, _, _ = dialect.Insert("mail_log").Rows(record).ToSQL()
		fmt.Println(query)
		_, err := td.Exec(query)
		if err != nil {
			common.Log("mail", "update td = error : %s , sql : %s", err.Error(), query)
			return
		}
	}
	fmt.Println("==== End Update TD ===")
}
