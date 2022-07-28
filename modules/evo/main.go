package evo

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	g "github.com/doug-martin/goqu/v9"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"
	cpool "github.com/silenceper/pool"
	"github.com/spaolacci/murmur3"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpproxy"
	"os"
	"strings"
	"task/contrib/conn"
	"task/contrib/helper"
	"task/modules/common"
	"time"
)

var (
	prefix                string
	betDB                 *sqlx.DB
	db                    *sqlx.DB
	beanPool              cpool.Pool
	cli                   *redis.ClusterClient
	fc                    *fasthttp.Client
	OrderLock             = 30 * (24 * time.Hour)
	Ctx                   = context.Background()
	dialect               = g.Dialect("mysql")
	Kvnd                  = decimal.NewFromInt(1000)
	colTransaction        = helper.EnumFields(MemberTransaction{})
	colMemberPromoBalance = helper.EnumFields(MemberPromoBalance{})
)

const (
	apiTimeOut = time.Second * 12
)

func Parse(service *common.BuildInfo, endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	prefix = conf.Prefix
	// 初始化beanstalk
	beanPool = conn.InitBeanstalk(conf.Beanstalkd.Addr, 50, 50, 100)
	// 初始化db
	betDB = conn.InitDB(conf.Db.Bet.Addr, conf.Db.Bet.MaxIdleConn, conf.Db.Bet.MaxIdleConn)
	// 初始化db
	db = conn.InitDB(conf.Db.Master.Addr, conf.Db.Master.MaxIdleConn, conf.Db.Master.MaxIdleConn)
	// redis
	cli = conn.InitRedisCluster(conf.Redis.Addr, conf.Redis.Password)
	// 初始化td
	logTD := conn.InitTD(conf.Td.Log.Addr, conf.Td.Log.MaxIdleConn, conf.Td.Log.MaxOpenConn)
	common.InitTD(logTD, prefix)
	go service.Start()

	News(os.Args[4])
	batchTask()
}

func batchTask() {
	// 初始化投注记录任务队列协程池
	confirmPool, _ := ants.NewPoolWithFunc(500, func(evo interface{}) {

		if fn, ok := evo.(common.BeansFnParam); ok {
			// 场馆转账订单确认
			handle(fn.M)
			// 删除job
			_ = fn.Conn.Delete(fn.ID)
		}
	})

	attr := common.BeansWatcherAttr{
		TubeName:       "evo",
		ReserveTimeOut: 2 * time.Minute,
		Pool:           confirmPool,
	}

	// 投注记录确认队列
	common.BeanstalkWatcher(beanPool, attr)
}

func handle(param map[string]interface{}) {

	txnId := param["txn_id"].(string)
	apiType := param["api_type"].(string)

	ex := g.Ex{
		"bill_no":  txnId,
		"flag":     "0",
		"api_type": apiType,
	}

	var id uint64
	query, _, _ := dialect.From("tbl_game_record").Select("id").Where(ex).ToSQL()

	//查询evo场馆订单是否有未结算订单
	err := betDB.Get(&id, query)
	if err != nil && err != sql.ErrNoRows {
		//已结算直接返回
		return
	}

	headers := map[string]string{
		"ag-code":      param["ag_code"].(string),
		"ag-token":     param["ag_token"].(string),
		"Content-Type": "application/json",
	}

	prdId, ok := param["prd_id"].(string)
	if !ok {
		fmt.Println("prd_id", param)
		prdId = "1"
	}

	requestURI := fmt.Sprintf("%s/results/%s/%s", param["api_url"].(string), prdId, param["txn_id"].(string))

	statusCode, body, err := httpGetHeader(requestURI, headers)
	if err != nil {
		fmt.Printf("evo error %prdId\n", err)
		//写入队列重新处理
		LoopAdd(param)
		return
	}

	fmt.Printf("evo uri = %s\n", requestURI)
	fmt.Printf("evo body = %s\n", string(body))

	if statusCode != fasthttp.StatusOK {
		fmt.Printf("evo request status code %d\n", statusCode)
		//写入队列重新处理
		LoopAdd(param)
		return
	}

	result := EvoResult{}
	err = helper.JsonUnmarshal(body, &result)
	if err != nil {
		//写入队列重新处理
		LoopAdd(param)
		return
	}

	//订单还有处理中10分钟后在处理
	if result.Type == 0 || result.Status == 0 {
		//写入队列重新处理
		LoopAdd(param)
		return
	}

	t := time.Now()
	Credit(txnId, param["username"].(string), result, t)
}

//Credit 增加
func Credit(txnId, username string, info EvoResult, t time.Time) {

	key := fmt.Sprintf("%s:c:%s", EVO, txnId)
	ok := LockWalletKey(key, "1")
	if !ok {
		return
	}

	mem, err := GetMemberCache(username)
	if err != nil || len(mem.Username) < 1 {
		return
	}

	billNo := fmt.Sprintf("%d", MurmurHash(txnId, 0))
	ex := g.Ex{
		"bill_no":   billNo,
		"username":  mem.Username,
		"cash_type": helper.TransactionBet,
	}

	//判断是否有订单
	ty, betAmount, _, err := GetTransactionNew(ex)
	if err != nil {
		return
	}

	if betAmount.Cmp(decimal.Zero) == 0 {
		return
	}

	creditAmount := decimal.NewFromFloat(info.Payout).Div(Kvnd)
	netAmount := creditAmount.Sub(betAmount)

	cashType := helper.TransactionPayout
	flag := "1"
	if info.IsCancel == 1 {
		flag = "3"
		cashType = helper.TransactionBetCancel
	}

	if creditAmount.Cmp(decimal.Zero) > 0 {

		betTran := BetTransaction{
			Username:   mem.Username,
			CreateAt:   t.UnixNano() / 1e6,
			UID:        mem.UID,
			Amount:     creditAmount,
			BillNo:     billNo,
			CashType:   cashType,
			PlatformID: EVOID,
			Remark:     txnId,
			WalletTy:   ty,
		}

		betTrans := []BetTransaction{betTran}
		_, err = WalletTransactions(betTrans)
		if err != nil {
			return
		}
	}

	plat, err := PlatformRedis(EVOID)
	if err != nil {
		return
	}

	createdAt := t.UnixNano() / 1e6

	//这个放到 消息队列 去处理
	param := map[string]interface{}{
		"row_id":           plat["code"].(string) + billNo,
		"flag":             flag,
		"valid_bet_amount": netAmount.Abs().String(),
		"net_amount":       netAmount.String(),
		"settle_time":      fmt.Sprintf("%d", createdAt),
		"api_settle_time":  fmt.Sprintf("%d", createdAt),
	}

	fmt.Printf("evo settle row_id=%s\n", plat["code"].(string)+billNo)

	_, err = BeanPut("bet", beanPool, param, 0)
	if err != nil {
		fmt.Println("evo settle BeanPut err:", err.Error())
	}
}

func PlatformRedis(pid string) (map[string]interface{}, error) {

	plat := map[string]interface{}{}
	k := fmt.Sprintf("plat:%s", pid)
	res, err := cli.Get(Ctx, k).Result()
	if err == redis.Nil || err != nil {
		return plat, err
	}

	err = helper.JsonUnmarshal([]byte(res), &plat)
	if err != nil {
		return plat, err
	}

	return plat, nil
}

func transactionsOperate(cashType int) string {

	switch cashType {
	// 投注,重新结算减币,取消派彩
	case helper.TransactionBet, helper.TransactionResettleDeduction, helper.TransactionCancelPayout:
		return "-"
		// 投注取消,派彩,重新结算加币
	case helper.TransactionBetCancel, helper.TransactionPayout, helper.TransactionResettlePlus,
		//EVO红利
		helper.TransactionEVOPrize,
		helper.TransactionEVOPromote,
		helper.TransactionEVOJackpot:
		return "+"
		// 错误的帐变类型
	default:
		return ""
	}
}

func walletTranRecord(item BetTransaction, memberBalance map[string]map[int]decimal.Decimal) (g.Record, g.Record, g.Ex, error) {

	ex := g.Ex{
		"uid": item.UID,
	}

	amountColName := "balance"
	if item.WalletTy == 2 {
		amountColName = "lock_amount"
	}

	balance := decimal.Zero
	walletAmount, ok := memberBalance[item.Username]
	if ok {
		balance, ok = walletAmount[item.WalletTy]
	}

	if !ok {

		memberData, err := GetMemberWallet(item.UID, item.Username, item.PlatformID, 4)
		if err != nil {
			return nil, nil, nil, errors.New(helper.UserNotExist)
		}

		if item.WalletTy == 1 {
			balance = memberData.Balance
		}

		if item.WalletTy == 2 {
			balance = memberData.PlatBalance
		}

		if walletAmount == nil {
			memberBalance[item.Username] = map[int]decimal.Decimal{}
		}

		memberBalance[item.Username][item.WalletTy] = balance
	}

	operate := transactionsOperate(item.CashType)
	if balance.Cmp(item.Amount) == -1 && operate == OperateSub {
		return nil, nil, nil, errors.New(helper.BalanceErr)
	}

	afterAmount := balance.Add(item.Amount)
	if operate == OperateSub {
		afterAmount = balance.Sub(item.Amount)
	}

	tran := g.Record{
		"id":            helper.GenId(),
		"bill_no":       item.BillNo,
		"uid":           item.UID,
		"username":      item.Username,
		"cash_type":     item.CashType,
		"amount":        item.Amount,
		"before_amount": balance.String(),
		"after_amount":  afterAmount.String(),
		"created_at":    item.CreateAt,
		"remark":        item.Remark,
	}

	balanceTran := g.Record{
		amountColName: g.L(fmt.Sprintf("%s%s%s", amountColName, operate, item.Amount.Truncate(3).String())),
	}

	memberBalance[item.Username][item.WalletTy] = afterAmount

	return balanceTran, tran, ex, nil
}

//GetMemberWallet 会员余额明细
func GetMemberWallet(uid, userName, platId string, precision uint) (MemberBalanceData, error) {

	data := MemberBalanceData{
		Balance:     decimal.Zero,
		PlatBalance: decimal.Zero,
		PlatState:   PlatWalletUnLock,
	}
	//查询余额缓存
	memberBalance, err := common.MemberBalance(db, uid)
	if err != nil && err != sql.ErrNoRows {
		return data, err
	}

	platBalance, state, _, err := GetPlatBalanceRedisPrecision(db, uid, userName, platId, precision)
	if err != nil && err != sql.ErrNoRows {
		return data, err
	}

	fb, err := decimal.NewFromString(platBalance)
	if err != nil {
		return data, err
	}

	data.PlatBalance = fb.Truncate(int32(precision))
	data.PlatState = state
	data.Balance = memberBalance.Truncate(int32(precision))

	data.Total = data.Balance

	if data.PlatState == PlatWalletLock {
		data.Total = data.PlatBalance
	}

	return data, nil
}

//GetPlatBalanceRedisPrecision 读取redis场馆余额数据
func GetPlatBalanceRedisPrecision(db *sqlx.DB, uid, name, platId string, precision uint) (string, string, string, error) {

	if precision > 4 {
		return "0", "0", "", errors.New("invalid precision")
	}

	mbBalance := MemberPromoBalance{}

	ex := g.Ex{
		"uid":         uid,
		"platform_id": platId,
	}

	t := dialect.From("tbl_member_promo_balance")
	query, _, _ := t.Select(colMemberPromoBalance...).Where(ex).Limit(1).ToSQL()
	err := db.Get(&mbBalance, query)
	if err != nil && err != sql.ErrNoRows {
		return "0.00", PlatWalletUnLock, "", err
	}

	if len(mbBalance.LockAmount) == 0 {
		mbBalance.LockAmount = "0.00"
		mbBalance.State = PlatWalletUnLock
	}

	return mbBalance.LockAmount, mbBalance.State, "", nil
}

// walletTransactions 钱包操作
func WalletTransactions(trans []BetTransaction) (string, error) {

	tx, err := db.Begin()
	if err != nil {
		return "db", errors.New(helper.TransErr)
	}

	memBalance := map[string]map[int]decimal.Decimal{}
	for _, v := range trans {

		balanceTran, tran, ex, err := walletTranRecord(v, memBalance)
		if err != nil {
			_ = tx.Rollback()
			return "db", errors.New(helper.TransErr)
		}

		query := ""
		tranQuery := ""
		if v.WalletTy == 1 {
			tranQuery, _, _ = dialect.Insert("tbl_balance_transaction").Rows(tran).ToSQL()
			query, _, _ = dialect.Update("tbl_members").Set(balanceTran).Where(ex).ToSQL()
		}

		if v.WalletTy == 2 {
			tranQuery, _, _ = dialect.Insert("tbl_member_promo_transaction").Rows(tran).ToSQL()
			query, _, _ = dialect.Update("tbl_member_promo_balance").Set(balanceTran).Where(ex).ToSQL()
		}

		_, err = tx.Exec(query)
		if err != nil {
			fmt.Println("walletBetTransaction query = ", query)
			_ = tx.Rollback()
			return "db", errors.New(helper.TransErr)
		}

		_, err = tx.Exec(tranQuery)
		if err != nil {
			fmt.Println("walletBetTransaction transQuery = ", tranQuery)
			_ = tx.Rollback()
			return "db", errors.New(helper.TransErr)
		}

	}

	_ = tx.Commit()

	return "", nil
}

//GetTransactionNew 获取指定条件的账变记录并将帐变金额转换为decimal
func GetTransactionNew(ex g.Ex) (int, decimal.Decimal, MemberTransaction, error) {

	amount, trans, err := getMemberTransaction(ex)
	if err == nil {
		//余额钱包有帐变则返回信息
		return WalletTyBalance, amount, trans, err
	}

	ex["platform_id"] = EVOID
	ty, amount, mbTrans, err := getTransactionPromo(ex)

	if ty == WalletTyPlatform {
		//需要判断当前活动钱是否解锁
		ex = g.Ex{
			"uid":         mbTrans.UID,
			"platform_id": EVOID,
		}

		tbl := dialect.From("tbl_member_promo_balance")
		query, _, _ := tbl.Select("state").Where(ex).Order(g.C("created_at").Desc()).Limit(1).ToSQL()
		var state string
		err := db.Get(&state, query)
		//未查询到活动记录，则为余额钱包
		if err != nil {
			return WalletTyBalance, amount, mbTrans, nil
		}

		//活动钱包状态为锁定则返回活动钱包
		if state == PlatWalletLock {
			return ty, amount, mbTrans, nil
		}

		//活动钱包状态解锁则为余额钱包
		return WalletTyBalance, amount, mbTrans, nil
	}

	//活动动钱包帐变则返回信息
	return ty, amount, mbTrans, err
}

func getTransactionPromo(ex g.Ex) (int, decimal.Decimal, MemberTransaction, error) {
	//判断是否有订单
	mbTrans := MemberTransaction{}
	tbl := dialect.From("tbl_member_promo_transaction")
	query, _, _ := tbl.Select(colTransaction...).Where(ex).Order(g.C("created_at").Desc()).Limit(1).ToSQL()
	err := db.Get(&mbTrans, query)
	if err != nil || len(mbTrans.Username) < 1 {
		return WalletTyBalance, decimal.Zero, mbTrans, err
	}

	amount, err := decimal.NewFromString(mbTrans.Amount)
	if err != nil {
		fmt.Printf("transaction amount to decimal error %s \n", err.Error())
		return WalletTyBalance, decimal.Zero, mbTrans, err
	}

	return WalletTyPlatform, amount, mbTrans, nil
}

func getMemberTransaction(ex g.Ex) (decimal.Decimal, MemberTransaction, error) {
	//判断是否有订单
	mbTrans := MemberTransaction{}
	tbl := dialect.From("tbl_balance_transaction")
	query, _, _ := tbl.Select(colTransaction...).Where(ex).Order(g.C("created_at").Desc()).Limit(1).ToSQL()
	err := db.Get(&mbTrans, query)
	if err != nil || len(mbTrans.Username) < 1 {
		return decimal.Zero, mbTrans, err
	}

	amount, err := decimal.NewFromString(mbTrans.Amount)
	if err != nil {
		fmt.Printf("transaction amount to decimal error %s \n", err.Error())
		return decimal.Zero, mbTrans, err
	}

	return amount, mbTrans, nil
}

func LoopAdd(param map[string]interface{}) {

	m := time.Minute * 10
	_, err := BeanPut("evo", beanPool, param, int(m.Seconds()))
	if err != nil {
		fmt.Println("evo BeanPut err:", err.Error())
	}
}

func BeanPut(name string, mq cpool.Pool, param map[string]interface{}, delay int) (string, error) {

	m := &fasthttp.Args{}
	for k, v := range param {
		if _, ok := v.(string); ok {
			m.Set(k, v.(string))
		}
	}

	c, err := mq.Get()
	if err != nil {
		return "sys", err
	}

	if conn, ok := c.(*beanstalk.Conn); ok {

		tube := &beanstalk.Tube{Conn: conn, Name: name}
		_, err = tube.Put(m.QueryString(), 1, time.Duration(delay)*time.Second, 10*time.Minute)
		if err != nil {
			mq.Put(c)
			return "sys", err
		}
	}

	//将连接放回连接池中
	return "", mq.Put(c)
}

//获取用户信息缓存
func GetMemberCache(username string) (common.Member, error) {

	username = strings.Replace(username, prefix, "", 1)
	if username == "" {
		return common.Member{}, errors.New(helper.ParamNull)
	}

	//判断用户是否存在
	return common.MemberFindOne(db, username)
}

//并发处理
func LockWalletKey(key, val string) bool {

	isRepeat, err := cli.SetNX(Ctx, key, val, OrderLock).Result()
	if err != nil || !isRepeat {
		return false
	}
	return true
}

func News(socks5 string) {

	fc = &fasthttp.Client{
		MaxConnsPerHost: 60000,
		TLSConfig:       &tls.Config{InsecureSkipVerify: true},
		ReadTimeout:     apiTimeOut,
		WriteTimeout:    apiTimeOut,
	}

	if socks5 != "0.0.0.0" {
		fc.Dial = fasthttpproxy.FasthttpSocksDialer(socks5)
	}
}

func MurmurHash(str string, seed uint32) uint64 {

	h64 := murmur3.New64WithSeed(seed)
	h64.Write([]byte(str))
	v := h64.Sum64()
	h64.Reset()

	return v
}

func httpGetHeader(requestURI string, headers map[string]string) (int, []byte, error) {

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()

	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	req.SetRequestURI(requestURI)
	req.Header.SetMethod("GET")
	//req.SetBody(requestBody)
	//req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	err := fc.DoTimeout(req, resp, apiTimeOut)

	return resp.StatusCode(), resp.Body(), err
}
