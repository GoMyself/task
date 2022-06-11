package common

import (
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"runtime"
	"strings"
	"task/contrib/helper"
	"time"
)

var (
	db     *sqlx.DB
	prefix string
)

// InitTD 初始化td
func InitTD(td *sqlx.DB, p string) {
	db = td
	prefix = p
}

func Log(flag, format string, v ...interface{}) {

	_, file, line, _ := runtime.Caller(1)
	paths := strings.Split(file, "/")
	l := len(paths)
	if l > 2 {
		file = paths[l-2] + "/" + paths[l-1]
	}
	path := fmt.Sprintf("%s:%d", file, line)
	msg := fmt.Sprintf(format, v...)

	ts := time.Now()
	id := helper.GenId()

	fields := g.Record{
		"id":       id,
		"content":  msg,
		"project":  "task",
		"flags":    flag,
		"filename": path,
		"ts":       ts.UnixMicro(),
	}

	query, _, _ := dialect.Insert("goerror").Rows(&fields).ToSQL()
	//fmt.Println(query)
	_, err := db.Exec(query)
	if err != nil {
		fmt.Println("insert SMS = ", err.Error(), fields)
	}
}
