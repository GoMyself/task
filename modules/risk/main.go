package risk

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

const (
	risksKey   = "receive"
	risksState = "receiveState"
)

var (
	db       *sqlx.DB
	cli      *redis.ClusterClient
	beanPool cpool.Pool
	ctx      = context.Background()
	dialect  = g.Dialect("mysql")
)

func Parse(service *common.BuildInfo, endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	prefix := conf.Prefix
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

	task()
}

func task() {

	common.Log("risk", "风控自动派单服务开始")

	// 初始化红利批量发放任务队列协程池
	pool, _ := ants.NewPoolWithFunc(5, func(payload interface{}) {

		if fn, ok := payload.(common.BeansFnParam); ok {
			// 场馆转账订单确认
			handle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	attr := common.BeansWatcherAttr{
		TubeName:       "risk",
		ReserveTimeOut: 2 * time.Minute,
		Pool:           pool,
	}

	// 场馆转账订单确认队列
	common.BeanstalkWatcher(beanPool, attr)
}

func handle(m map[string]interface{}) {

	// 非自动派单模式，丢弃消息
	exist, _ := cli.Get(ctx, risksState).Result()
	if exist != "1" {
		common.Log("risk", "manual pick drop : %v", m)
		return
	}

	common.Log("risk", "risk data : %v", m)

	id := m["id"].(string)
	// 订单级加锁，防止并发竞争
	key := fmt.Sprintf("risk:%s", id)
	err := common.Lock(cli, key)
	if err != nil {
		// 已经有协程在处理同一个订单，丢弃消息
		common.Log("risk", "err : %v", err)
		return
	}

	defer common.Unlock(cli, key)

	// 根据订单id获取订单信息，检查订单状态
	var state int
	ex := g.Ex{
		"id": id,
	}
	t := dialect.From("tbl_withdraw")
	query, _, _ := t.Select("state").Where(ex).Limit(1).ToSQL()
	err = db.Get(&state, query)
	if err != nil {
		// 获取订单信息失败，消息重新放入队列
		common.Log("risk", "err : %v", err)
		beanBack(id)
		return
	}

	// 只处理审核中状态的提款订单，订单状态不为审核中状态，说明订单已经被处理过了，丢弃消息
	if state != common.WithdrawReviewing {
		common.Log("risk", "state error")
		return
	}

	// 无可分配的风控人员，消息重新放入队列
	confirmUid, err := getRisksUID()
	if err != nil || confirmUid == "" {
		common.Log("risk", "no dealer")
		beanBack(id)
		return
	}

	// 获取风控审核人的name，消息重新放入队列
	confirmName, err := adminGetName(confirmUid)
	if err != nil || confirmName == "" {
		common.Log("risk", "get admin name uid [%s] err : %v", confirmUid, err)
		beanBack(id)
		return
	}

	// 不使用事务，数据库更新成功后更新redis

	// 更新订单状态
	record := g.Record{
		"state":        common.WithdrawDispatched,
		"receive_at":   time.Now().Unix(),
		"confirm_uid":  confirmUid,
		"confirm_name": confirmName,
	}
	query, _, _ = dialect.Update("tbl_withdraw").Set(record).Where(ex).ToSQL()
	res, err := db.Exec(query)
	if err != nil {
		// 更新失败，消息重新放入队列
		common.Log("risk", "update err : %v", err)
		beanBack(id)
		return
	}

	if r, _ := res.RowsAffected(); r == 0 {
		// 更新生效条数为0，消息重新放入队列
		common.Log("risk", "affected 0 row: %s", query)
		beanBack(id)
		return
	}

	// 订单分配风控人员
	err = setRisksOrder(confirmUid, id, 1)
	if err != nil {
		// 未成功分配，消息重新放入队列
		common.Log("risk", "beanstalk add err : %v", err)
		beanBack(id)
		return
	}
}

// 未处理，投递回beanstalk
func beanBack(id string) {

	beanTask := common.BeansProducerAttr{
		Tube: "risk",
		Message: map[string]interface{}{
			"id": id, //提款订单id
		},
		Delay: 10 * time.Second,
		PRI:   1,
		TTR:   10 * time.Second,
	}
	err := common.BeansAddTask(beanPool, beanTask)
	if err != nil {
		common.Log("risk", "beanstalk add err : %v", err)
	}
}

//返水风控审核人员的UID
func getRisksUID() (string, error) {

	// 查询最大接单数量
	max, err := cli.Get(ctx, "R:num").Uint64()
	if err != nil && err != redis.Nil {
		return "0", err
	}

	// 如果最大接单数量小于等于0则直接返回
	if max <= 0 {
		return "0", errors.New("max acceptable order quality less or equal to 0")
	}

	// 查询在自动派单列表中的总人数
	c, err := cli.LLen(ctx, risksKey).Result()
	if err != nil {
		return "0", err
	}

	for i := int64(0); i < c; i++ {
		uid, err := cli.RPopLPush(ctx, risksKey, risksKey).Result()
		if err != nil && err != redis.Nil {
			return "0", err
		}

		// 查询结果可能是redis.Nil
		if uid == "" {
			continue
		}

		key := fmt.Sprintf("R:%s", uid)
		// 查询当前未处理的订单
		current, err := cli.LLen(ctx, key).Result()
		if err != nil {
			return "0", err
		}

		// 如果当前未处理的订单小于最大接单数量 则派单给该风控人员
		if current < int64(max) {
			return uid, nil
		}
	}

	// 从头循环到尾,没有找到合适风控用户
	return "0", errors.New(helper.RequestBusy)
}

//删除或者新增list的订单号
func setRisksOrder(admin, billNo string, diff int) error {

	if admin == "" || admin == "0" || billNo == "" {
		return errors.New(helper.ParamNull)
	}

	key := fmt.Sprintf("R:%s", admin)
	if diff == -1 {
		_, err := cli.LRem(ctx, key, 0, billNo).Result()
		if err != nil {
			return err
		}

		return nil
	}

	_, err := cli.LPush(ctx, key, billNo).Result()
	if err != nil {
		return err
	}

	return nil
}

// 获取admin的name
func adminGetName(id string) (string, error) {

	var name string
	query, _, _ := dialect.From("tbl_admins").Select("name").Where(g.Ex{"id": id}).ToSQL()
	err := db.Get(&name, query)
	if err != nil && err != sql.ErrNoRows {
		return name, err
	}

	return name, nil
}
