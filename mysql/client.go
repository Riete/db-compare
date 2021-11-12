package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySql struct {
	Role     string
	Username string
	Password string
	Host     string
	Port     string
}

func (ms MySql) Connect(database string) *sql.DB {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=10s",
		ms.Username, ms.Password, ms.Host, ms.Port, database,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(errors.New(fmt.Sprintf("%s 数据库连接失败", ms.Host)))
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Minute * 3)
	return db
}

func (ms MySql) QueryContext(ctx context.Context, database, statement string) (*sql.Rows, string, error) {
	db := ms.Connect(database)
	defer db.Close()
	start := time.Now().UnixNano()
	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		if err.Error() == context.Canceled.Error() {
			err = errors.New("SQL执行终止: " + err.Error())
		} else if err.Error() == context.DeadlineExceeded.Error() {
			err = errors.New("SQL执行超时: " + err.Error())
		} else {
			err = errors.New("执行SQL语句报错: " + err.Error() + fmt.Sprintf("[%s]", statement))
		}
	}
	return rows, CalcDuration(start, time.Now().UnixNano()), err
}

func (ms MySql) Query(database, statement string) (*sql.Rows, string, error) {
	return ms.QueryContext(context.Background(), database, statement)
}

func (ms MySql) ParseRows(rows *sql.Rows) ([]string, [][]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return []string{}, [][]string{}, errors.New("解析返回结果报错: " + err.Error())
	}
	var results [][]string
	for rows.Next() {
		var r []interface{}
		for i := 0; i < len(columns); i++ {
			r = append(r, &[]byte{})
		}
		err := rows.Scan(r...)
		if err != nil {
			return []string{}, [][]string{}, errors.New("解析返回结果报错: " + err.Error())
		}
		var result []string
		for _, v := range r {
			result = append(result, string(*v.(*[]byte)))
		}
		results = append(results, result)
	}
	return columns, results, nil
}

func (ms MySql) GetDatabases() ([]string, error) {
	rows, _, err := ms.Query("", "SHOW DATABASES;")
	if err != nil {
		return []string{}, err
	}
	_, databases, err := ms.ParseRows(rows)
	if err != nil {
		return []string{}, err
	}
	var results []string
	for _, db := range databases {
		database := db[0]
		if database == "information_schema" || database == "mysql" || database == "performance_schema" || database == "sys" {
			continue
		}
		results = append(results, database)
	}
	return results, nil
}

func (ms MySql) GetTables(database string) ([]string, error) {
	rows, _, err := ms.Query(database, "SHOW TABLES;")
	if err != nil {
		return []string{}, err
	}
	_, tables, err := ms.ParseRows(rows)
	if err != nil {
		return []string{}, err
	}
	var results []string
	for _, table := range tables {
		results = append(results, table[0])
	}
	return results, nil
}

func CalcDuration(start, end int64) string {
	return fmt.Sprintf("%.3fs", float64(end-start)/1e9)
}
