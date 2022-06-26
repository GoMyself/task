package rocket

import (
	"github.com/apache/rocketmq-client-go/v2"
	g "github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"log"
	"task/contrib/conn"
	"task/modules/common"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
)

var (
	db               *sqlx.DB
	td               *sqlx.DB
	merchantConsumer rocketmq.PushConsumer
	prefix           string
	loc              *time.Location
	dialect          = g.Dialect("mysql")
)

func Parse(service *common.BuildInfo, endpoints []string, path, flag string) {

	var err error
	conf := common.ConfParse(endpoints, path)
	prefix = conf.Prefix
	loc, _ = time.LoadLocation("Asia/Bangkok")
	// 初始化db
	db = conn.InitDB(conf.Db.Master.Addr, conf.Db.Master.MaxIdleConn, conf.Db.Master.MaxIdleConn)
	// 初始化td
	td = conn.InitTD(conf.Td.Addr, conf.Td.MaxIdleConn, conf.Td.MaxOpenConn)
	common.InitTD(td, prefix)
	go service.Start()

	merchantConsumer, err = rocketmq.NewPushConsumer(
		consumer.WithGroupName("merchant"),
		consumer.WithNameServer(conf.Rocketmq),
		consumer.WithConsumerModel(consumer.Clustering),
	)
	if err != nil {
		log.Fatalln(err)
	}

	switch flag {
	case "message":
		batchMessageTask()
	case "transferag":
		batchTransferAgTask()
	}

	err = merchantConsumer.Start()
	if err != nil {
		log.Fatalln(err)
	}
	for {
		time.Sleep(30 * time.Minute)
	}
}
