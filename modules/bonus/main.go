package bonus

import (
	"context"
	"github.com/go-redis/redis/v8"
	"lukechampine.com/frand"
	"strconv"
	"task/contrib/conn"
	"task/modules/common"
	"time"
)

var (
	cli *redis.ClusterClient
	ctx = context.Background()
)

func Parse(service *common.BuildInfo, endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	// 初始化redis
	cli = conn.InitRedisCluster(conf.Redis.Addr, conf.Redis.Password)
	// 初始化td
	td := conn.InitTD(conf.Td.Addr, conf.Td.MaxIdleConn, conf.Td.MaxOpenConn)
	common.InitTD(td, conf.Prefix)
	go service.Start()

	handle()
}

// 默认数字为64287325,41K
//每秒钟递增，递增数字在25-300中随机增加
func handle() {

	key := "bonusPool"
	var b uint64 = 6428732541

	bonus, err := cli.Get(ctx, key).Result()
	if err != nil {
		// 没有数据 初始化
		if err == redis.Nil {
			cli.Set(ctx, key, b, 100*time.Hour)
		}

		// redis 执行错误
		if err != redis.Nil {
			common.Log("bonus", "redis error: %s", err.Error())
			return
		}
	}

	b, err = strconv.ParseUint(bonus, 10, 64)
	if err != nil {
		cli.Set(ctx, key, b, 100*time.Hour)
		//common.Log("bonus", "str to int err : %s", err.Error())
		//return
	}

	for {
		// 五秒后开始生成新的奖金
		time.Sleep(time.Second * 5)
		// 生成随机数 25-300
		b += uint64(frand.Intn(275) + 25)
		cli.Set(ctx, key, b, 100*time.Hour)
	}

}
