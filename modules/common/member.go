package common

import (
	"errors"
	"fmt"
	"strconv"
	"task/contrib/helper"

	g "github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
)

var (
	dialect              = g.Dialect("mysql")
	colsMember           = helper.EnumFields(Member{})
	colsMemberPlatform   = helper.EnumFields(MemberPlatform{})
	fieldsMemberPlatform = helper.EnumRedisFields(MemberPlatform{})
)

func PlatToMap(m MemberPlatform) map[string]interface{} {

	data := map[string]interface{}{
		"id":                      m.ID,
		"username":                m.Username,
		"password":                m.Password,
		"pid":                     m.Pid,
		"balance":                 m.Balance,
		"state":                   m.State,
		"created_at":              m.CreatedAt,
		"transfer_in":             m.TransferIn,
		"transfer_in_processing":  m.TransferInProcessing,
		"transfer_out":            m.TransferOut,
		"transfer_out_processing": m.TransferOutProcessing,
		"extend":                  m.Extend,
	}

	return data
}

// 查询用户单条数据
func MemberFindOne(db *sqlx.DB, name string) (Member, error) {

	m := Member{}

	t := dialect.From("tbl_members")
	query, _, _ := t.Select(colsMember...).Where(g.Ex{"username": name}).Limit(1).ToSQL()
	fmt.Printf("MemberFindOne : %v\n", query)
	err := db.Get(&m, query)
	if err != nil {
		return m, err
	}

	return m, nil
}

func MemberMCache(db *sqlx.DB, names []string) (map[string]Member, string, error) {

	data := map[string]Member{}

	if len(names) == 0 {
		return data, "", errors.New(helper.ParamNull)
	}

	var mbs []Member
	t := dialect.From("tbl_members")
	query, _, _ := t.Select(colsMember...).Where(g.Ex{"username": names}).ToSQL()
	err := db.Select(&mbs, query)
	if err != nil {
		return data, "db", err
	}

	if len(mbs) > 0 {
		for _, v := range mbs {
			if v.Username != "" {
				data[v.Username] = v
			}
		}
	}

	return data, "", nil
}

// 通过用户名获取用户在redis中的数据
func MemberPlatformCache(cli *redis.Client, username, pid string) (MemberPlatform, string, error) {

	mp := MemberPlatform{}
	key := fmt.Sprintf("%s:%s", username, pid)
	rs := cli.HMGet(ctx, key, fieldsMemberPlatform...)
	if rs.Err() != nil {
		return mp, "redis", rs.Err()
	}

	if err := rs.Scan(&mp); err != nil {
		return mp, "redis", err
	}

	return mp, "", nil
}

func MemberPlatformInsert(db *sqlx.DB, cli *redis.Client, param map[string]interface{}) (MemberPlatform, string, error) {

	mp := MemberPlatform{
		ID:                    param["id"].(string),
		Pid:                   param["pid"].(string),
		Balance:               "0.0000",
		Username:              param["username"].(string),
		Password:              param["password"].(string),
		State:                 1,
		TransferIn:            0,
		TransferOut:           0,
		TransferInProcessing:  0,
		TransferOutProcessing: 0,
	}
	createAt, _ := strconv.ParseInt(param["s"].(string), 10, 64)
	mp.CreatedAt = uint32(createAt)
	mp.Extend = param["extend"].(uint64)

	query, _, _ := dialect.Insert("tbl_member_platform").Rows(&mp).ToSQL()
	_, err := db.Exec(query)
	if err != nil {
		return mp, "db", err
	}

	_, _ = MemberPlatformUpdateCache(db, cli, param["username"].(string), param["pid"].(string))

	return mp, "", nil
}

// 更新redis 用户信息
func MemberPlatformUpdate(db *sqlx.DB, cli *redis.Client, username, pid string, record g.Record) (string, error) {

	ex := g.Ex{
		"username": username,
		"pid":      pid,
	}
	query, _, _ := dialect.Update("tbl_member_platform").Set(record).Where(ex).ToSQL()
	_, err := db.Exec(query)
	if err != nil {
		return "db", err
	}

	_, _ = MemberPlatformUpdateCache(db, cli, username, pid)

	return "", nil
}

// 更新redis 用户信息
func MemberPlatformUpdateCache(db *sqlx.DB, cli *redis.Client, username, pid string) (string, error) {

	mp := MemberPlatform{}

	ex := g.Ex{
		"username": username,
		"pid":      pid,
	}
	t := dialect.From("tbl_member_platform")
	query, _, _ := t.Select(colsMemberPlatform...).Where(ex).Limit(1).ToSQL()
	err := db.Get(&mp, query)
	if err != nil {
		return "db", err
	}

	pipe := cli.TxPipeline()
	defer pipe.Close()

	key := fmt.Sprintf("%s:%s", username, pid)
	pipe.Unlink(ctx, key)
	pipe.HMSet(ctx, key, PlatToMap(mp))
	pipe.Persist(ctx, key)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return "redis", err
	}

	return "", nil
}

//读取余额
func MemberBalance(db *sqlx.DB, uid string) (decimal.Decimal, error) {

	var (
		mb      MBBalance
		balance decimal.Decimal
	)

	ex := g.Ex{
		"uid": uid,
	}
	query, _, _ := dialect.From("tbl_members").Select("uid", "balance").Where(ex).Limit(1).ToSQL()
	err := db.Get(&mb, query)
	if err != nil {
		return balance, err
	}

	balance, err = decimal.NewFromString(mb.Balance)
	if err != nil {
		return balance, err
	}

	return balance, nil
}

func MemberTresSubNames(db *sqlx.DB, ex g.Ex) ([]string, error) {

	var (
		uids      []string
		usernames []string
	)
	query, _, _ := dialect.From("tbl_members_tree").Select("descendant").Where(ex).ToSQL()
	fmt.Println(query)
	err := db.Select(&uids, query)
	if err != nil {
		return usernames, err
	}

	if len(uids) == 0 {
		return usernames, nil
	}

	query, _, _ = dialect.From("tbl_members").Select(g.COUNT("username")).Where(g.Ex{"uid": uids}).ToSQL()
	fmt.Println(query)
	err = db.Select(&usernames, query)

	return usernames, err
}

func MembersCount(db *sqlx.DB, ex g.Ex) (int, error) {

	var count int
	query, _, _ := dialect.From("tbl_members").Select(g.COUNT("uid")).Where(ex).ToSQL()
	fmt.Println(query)
	err := db.Get(&count, query)

	return count, err
}

func MembersPageNames(db *sqlx.DB, page, pageSize int, ex g.Ex) ([]string, error) {

	var v []string
	offset := (page - 1) * pageSize
	query, _, _ := dialect.From("tbl_members").Select("username").
		Where(ex).Offset(uint(offset)).Limit(uint(pageSize)).Order(g.C("created_at").Asc()).ToSQL()
	fmt.Println(query)
	err := db.Select(&v, query)

	return v, err
}
