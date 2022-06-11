package banner

import (
	"context"
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/panjf2000/ants/v2"
	cpool "github.com/silenceper/pool"
	"github.com/valyala/fastjson"
	"math"
	"strings"
	"task/contrib/conn"
	"task/contrib/helper"
	"task/modules/common"
	"time"
)

type Banner struct {
	ID          string `json:"id" db:"id" rule:"none"`                                                                       //
	Title       string `json:"title" db:"title" rule:"filter"`                                                               //标题
	Device      string `json:"device" db:"device" rule:"sDigit" msg:"device error" name:"device"`                            //设备类型(1,2)
	RedirectURL string `json:"redirect_url" db:"redirect_url" rule:"url" msg:"redirect_url error" name:"redirect_url"`       //跳转地址
	Images      string `json:"images" db:"images" rule:"none"`                                                               //图片路径
	Seq         string `json:"seq" db:"seq" rule:"digit" min:"1" max:"100" msg:"seq error" name:"seq"`                       //排序
	Flags       string `json:"flags" db:"flags" rule:"digit" min:"1" max:"10" msg:"flags error" name:"flags"`                //广告类型
	ShowType    string `json:"show_type" db:"show_type" rule:"digit" min:"1" max:"2" msg:"show_type error" name:"show_type"` //1 永久有效 2 指定时间
	ShowAt      string `json:"show_at" db:"show_at" rule:"dateTime" msg:"show_at error" name:"show_at"`                      //开始展示时间
	HideAt      string `json:"hide_at" db:"hide_at" rule:"dateTime" msg:"hide_at error" name:"hide_at"`                      //结束展示时间
	URLType     string `json:"url_type" db:"url_type" rule:"digit" min:"0" max:"3" msg:"url_type error" name:"url_type"`     //链接类型 1站内 2站外
	UpdatedName string `json:"updated_name" db:"updated_name" rule:"none"`                                                   //更新人name
	UpdatedUID  string `json:"updated_uid" db:"updated_uid" rule:"none"`                                                     //更新人id
	UpdatedAt   string `json:"updated_at" db:"updated_at" rule:"none"`                                                       //更新时间
	State       string `json:"state" db:"state" rule:"none"`                                                                 //0:关闭1:开启
}

var (
	db        *sqlx.DB
	prefix    string
	beanPool  cpool.Pool
	cli       *redis.ClusterClient
	ctx       = context.Background()
	dialect   = g.Dialect("mysql")
	colBanner = helper.EnumFields(Banner{})
)

func Parse(service *common.BuildInfo, endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	prefix = conf.Prefix

	// 初始化redis
	cli = conn.InitRedisCluster(conf.Redis.Addr, conf.Redis.Password)
	// 初始化db
	db = conn.InitDB(conf.Db.Master.Addr, conf.Db.Master.MaxIdleConn, conf.Db.Master.MaxIdleConn)
	// 初始化beanstalk
	beanPool = conn.InitBeanstalk(conf.Beanstalkd.Addr, 50, 50, 100)

	// 初始化td
	td := conn.InitTD(conf.Td.Addr, conf.Td.MaxIdleConn, conf.Td.MaxOpenConn)
	common.InitTD(td, prefix)
	go service.Start()

	bannerTask()
}

// 批量红利派发
func bannerTask() {

	common.Log("banner", "banner自动开关服务开始")

	// 初始化红利批量发放任务队列协程池
	promoPool, _ := ants.NewPoolWithFunc(10, func(payload interface{}) {

		if fn, ok := payload.(common.BeansFnParam); ok {
			bannerHandle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	attr := common.BeansWatcherAttr{
		TubeName:       "Banner",
		ReserveTimeOut: 2 * time.Minute,
		Pool:           promoPool,
	}

	// 场馆转账订单确认队列
	common.BeanstalkWatcher(beanPool, attr)
}

// banner自动开关
func bannerHandle(m map[string]interface{}) {

	var tm int64
	tyVal, ok := m["ty"]
	if !ok {
		return
	}
	ty := tyVal.(string)
	id, ok := m["id"]
	if !ok {
		return
	}

	ex := g.Ex{
		"id":     id.(string),
		"prefix": prefix,
	}
	record := g.Record{
		"state": ty,
	}

	field := "show_at" // 开始展示
	if ty == "3" {
		//结束展示
		field = "hide_at"
	}

	query, _, _ := dialect.From("tbl_banner").Select(field).Where(ex).ToSQL()
	err := db.Get(&tm, query)
	if err != nil {
		common.Log("banner", "error: %v", err)
		return
	}

	now := time.Now().Unix()
	t := math.Abs(float64(tm - now))
	// 时间误差超过10s，丢弃消息
	if int(t) > 10 {
		common.Log("banner", "time deviation too large, drop")
		return
	}

	query, _, _ = dialect.Update("tbl_banner").Set(record).Where(ex).ToSQL()
	_, err = db.Exec(query)
	if err != nil {
		common.Log("banner", "update error: %s, sql: %s", err.Error(), query)
	}

	_ = bannerRefreshToCache
}

func bannerSplash(pipe redis.Pipeliner) error {

	record := Banner{}
	device := []int{24, 30}
	for i := device[0]; i < device[1]; i++ {
		key := fmt.Sprintf("G1%d", i)
		pipe.Del(ctx, key)
	}

	ex := g.Ex{
		"state":  2,
		"flags":  1,
		"prefix": prefix,
	}
	query, _, _ := dialect.From("tbl_banner").Select(colBanner...).Where(ex).ToSQL()
	err := db.Get(&record, query)
	if err != nil {
		_, _ = pipe.Exec(ctx)

		return err
	}

	base := fastjson.MustParse(record.Images)
	base.Set("url", fastjson.MustParse(fmt.Sprintf(`"%s"`, record.RedirectURL)))
	base.Set("flags", fastjson.MustParse(fmt.Sprintf(`"%s"`, record.URLType)))

	di := strings.SplitN(record.Device, ",", 8)
	for _, val := range di {
		key := fmt.Sprintf("G1%s", val)

		pipe.Set(ctx, key, base.String(), 100*time.Hour)
		pipe.Persist(ctx, key)
	}

	return nil
}

func bannerCarousel(pipe redis.Pipeliner) error {

	device := []int{24, 30}
	var recs []Banner
	results := map[string][]string{}

	for i := device[0]; i < device[1]; i++ {
		key := fmt.Sprintf("G2%d", i)
		pipe.Unlink(ctx, key)
	}

	ex := g.Ex{
		"state":  2,
		"flags":  2,
		"prefix": prefix,
	}
	query, _, _ := dialect.From("tbl_banner").Select(colBanner...).Where(ex).Order(g.C("seq").Asc()).ToSQL()
	err := db.Select(&recs, query)
	if err != nil {
		_, _ = pipe.Exec(ctx)

		return err
	}

	for _, val := range recs {

		img := fastjson.GetBytes([]byte(val.Images), "ad")
		str := fmt.Sprintf(`{"title":"%s", "url":"%s", "sort":"%s", "img":"%s", "flags":"%s"}`,
			val.Title, val.RedirectURL, val.Seq, string(img), val.URLType)

		if val.Device == "0" {
			for i := device[0]; i < device[1]; i++ {
				key := fmt.Sprintf("G2%d", i)
				results[key] = append(results[key], str)
			}
		} else {
			di := strings.SplitN(val.Device, ",", 8)
			for _, d := range di {
				key := fmt.Sprintf("G2%s", d)
				results[key] = append(results[key], str)
			}
		}
	}

	arr := new(fastjson.Arena)
	for key, value := range results {

		aa := arr.NewArray()
		for k, v := range value {
			aa.SetArrayItem(k, fastjson.MustParse(v))
		}

		pipe.Set(ctx, key, aa.String(), 100*time.Hour)
		pipe.Persist(ctx, key)
		arr.Reset()
	}
	arr = nil

	return nil
}

func bannerRefreshToCache(id string) error {

	record := Banner{}

	ex := g.Ex{
		"id":     id,
		"prefix": prefix,
	}
	query, _, _ := dialect.From("tbl_banner").Select(colBanner...).Where(ex).ToSQL()
	err := db.Get(&record, query)
	if err != nil {
		return err
	}

	pipe := cli.TxPipeline()
	defer pipe.Close()

	if record.Flags == "1" {
		err = bannerSplash(pipe)
	}

	if record.Flags == "2" {
		err = bannerCarousel(pipe)
	}

	_, err = pipe.Exec(ctx)

	return err
}
