package main

import (
	"fmt"
	_ "go.uber.org/automaxprocs"
	"os"
	"strings"
	"task/contrib/helper"
	"task/modules/banner"
	"task/modules/bonus"
	"task/modules/common"
	"task/modules/email"
	"task/modules/evo"
	"task/modules/message"
	"task/modules/promo"
	"task/modules/risk"
	"task/modules/rocket"
	"task/modules/sms"
)

var (
	gitReversion   = ""
	buildTime      = ""
	buildGoVersion = ""
)

type fn func(*common.BuildInfo, []string, string)

var cb = map[string]fn{
	"banner":  banner.Parse,  //活动流水更新
	"bonus":   bonus.Parse,   //电子游戏奖金池
	"risk":    risk.Parse,    //风控自动派单脚本
	"evo":     evo.Parse,     //evo
	"message": message.Parse, //站内信批量发送
	"promo":   promo.Parse,   //活动流水更新
	"sms":     sms.Parse,     //短信自动过期
	"email":   email.Parse,   //邮件自动过期
	"rocket":  rocket.Parse,  //rocketMQ 消息站内信消息
}

func main() {

	argc := len(os.Args)
	if argc != 5 {
		fmt.Printf("%s <etcds> <cfgPath> [upgrade][transferConfirm][dividend][birthDividend][monthlyDividend][rebate][message] <proxy>\n", os.Args[0])
		return
	}

	endpoints := strings.Split(os.Args[1], ",")

	fmt.Printf("gitReversion = %s\r\nbuildGoVersion = %s\r\nbuildTime = %s\r\n", gitReversion, buildGoVersion, buildTime)

	if val, ok := cb[os.Args[3]]; ok {
		service := common.NewService(os.Args[3], gitReversion, buildTime, buildGoVersion, helper.ServiceTask)
		val(&service, endpoints, os.Args[2])
	}

	fmt.Println(os.Args[3], "done")
}
