package promo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/panjf2000/ants/v2"
	cpool "github.com/silenceper/pool"
	"task/contrib/conn"
	"task/contrib/helper"
	"task/modules/common"
	"time"
)

// 活动流水计算脚本
var (
	db            *sqlx.DB
	cli           *redis.ClusterClient
	beanPool      cpool.Pool
	prefix        string
	ctx           = context.Background()
	dialect       = g.Dialect("mysql")
	colsPromoJson = helper.EnumFields(PromoJson{})
)

func Parse(service *common.BuildInfo, endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	prefix = conf.Prefix

	// 初始化db
	db = conn.InitDB(conf.Db.Master.Addr, conf.Db.Master.MaxIdleConn, conf.Db.Master.MaxIdleConn)
	// 初始化redis
	cli = conn.InitRedisCluster(conf.Redis.Addr, conf.Redis.Password)
	// 初始化beanstalk
	beanPool = conn.InitBeanstalk(conf.Beanstalkd.Addr, 50, 50, 100)

	// 初始化td
	logTD := conn.InitTD(conf.Td.Log.Addr, conf.Td.Log.MaxIdleConn, conf.Td.Log.MaxOpenConn)
	common.InitTD(logTD, prefix)
	go service.Start()

	promoTask()
}

// 批量红利派发
func promoTask() {

	common.Log("promo", "活动脚本开启")

	// 初始化红利批量发放任务队列协程池
	promoPool, _ := ants.NewPoolWithFunc(10, func(payload interface{}) {

		if fn, ok := payload.(common.BeansFnParam); ok {
			promoHandle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	topic := fmt.Sprintf("%s_promo", prefix)
	attr := common.BeansWatcherAttr{
		TubeName:       topic,
		ReserveTimeOut: 2 * time.Minute,
		Pool:           promoPool,
	}

	// 场馆转账订单确认队列
	common.BeanstalkWatcher(beanPool, attr)
}

func promoHandle(m map[string]interface{}) {

	if m == nil {
		return
	}

	ty, ok := m["ty"].(string)
	if !ok {
		return
	}

	pid, ok := m["pid"].(string)
	if !ok {
		return
	}

	handle(pid, ty)
}

func handle(pid, ty string) {

	// 活动状态 1关闭 2开启 3已过期
	var state int
	ex := g.Ex{
		"id":     pid,
		"prefix": prefix,
	}
	query, _, _ := dialect.From("tbl_promo").Select("state").Where(ex).ToSQL()
	common.Log("promo", query)
	err := db.Get(&state, query)
	if err != nil && err != sql.ErrNoRows {
		common.Log("promo", err.Error())
		return
	}

	if err == sql.ErrNoRows {
		return
	}

	record := g.Record{}
	switch ty {
	case "show":
		// 已经是开启状态
		if state == 2 {
			return
		}
		record["state"] = 2
	case "hide":
		// 已经是过期状态
		if state == 3 {
			return
		}
		record["state"] = 3
	default:
		return
	}

	query, _, _ = dialect.Update("tbl_promo").Set(record).Where(ex).ToSQL()
	common.Log("promo", query)
	_, err = db.Exec(query)
	if err != nil {
		common.Log("promo", err.Error())
		return
	}

	_ = ToCache()
}

func ToCache() error {

	var (
		data []PromoJson
		list string
	)
	query, _, _ := dialect.From("tbl_promo").Select(colsPromoJson...).Where(g.Ex{}).ToSQL()
	common.Log("promo", query)
	err := db.Select(&data, query)
	if err != nil {
		common.Log("promo", err.Error())
		return err
	}

	if len(data) == 0 {
		return errors.New(helper.RecordNotExistErr)
	}

	pipe := cli.TxPipeline()
	defer pipe.Close()

	list = ""
	for _, v := range data {

		key := fmt.Sprintf("%s:promo:%s:%s", prefix, v.Flag, v.ID)
		pipe.Unlink(ctx, key)
		configKey := fmt.Sprintf("%s:promo:%s:config:%s", prefix, v.Flag, v.ID)
		pipe.Unlink(ctx, configKey)

		if v.State == 2 {

			s := fmt.Sprintf(`{"static":%s,"rules":%s,"config":%s}`, v.StaticJson, v.RulesJson, v.ConfigJson)
			pipe.Set(ctx, key, s, 100*time.Hour)
			pipe.Persist(ctx, key)

			pipe.Set(ctx, configKey, v.ConfigJson, 100*time.Hour)
			pipe.Persist(ctx, configKey)

			ls := fmt.Sprintf(`{"static":%s,"id":"%s","state":%d,"flag":"%s"}`, v.StaticJson, v.ID, v.State, v.Flag)

			if list != "" {
				list += ","
			}
			list += ls
		}
	}
	list = "[" + list
	list += "]"

	key := fmt.Sprintf("%s:promo:list", prefix)
	pipe.Unlink(ctx, key)
	pipe.Set(ctx, key, list, 100*time.Hour)
	pipe.Persist(ctx, key)

	_, err = pipe.Exec(ctx)
	if err != nil {
		common.Log("promo", fmt.Sprintf("redis error[%s]", err.Error()))
	}

	return nil
}
