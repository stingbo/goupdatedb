package main

import (
	"fmt"
	"github.com/unknwon/goconfig"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"

	//"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	//"sync/atomic"
	"github.com/panjf2000/ants/v2"
)

func main() {
	app := &cli.App{
		Name:  "update",
		Usage: "更新数据库结构，请输入要修改的SQL语句",
		Action: func(c *cli.Context) error {
			sql_type := c.Args().Get(0)
			sql := c.Args().Get(1)
			if sql_type == "sql" {
				fmt.Printf(sql)
				if sql == "" {
					fmt.Printf("sql语句不能为空，请检查后重试\n")
				} else {
					update(sql)
				}
			} else if sql_type == "sqlfile" {
				sqlfilepath := ""
				if sql == "" {
					sqlfilepath = "sqlfile/supplier_platform.sql"
				} else {
					sqlfilepath = sql
				}
				// 读取sql文件内容
				sqls, _ := readSqlFile(sqlfilepath)
				if len(sqls) == 0 {
					fmt.Printf("sql文件"+sqlfilepath+"读取错误或为空，没有执行更新数据库更新.\n")
				} else {
					for _, sqlstr := range sqls { //sql数组
						fmt.Printf(sqlstr)
						fmt.Printf("\n")
						update(sqlstr)
					}
					sqlfilenameall := path.Base(sqlfilepath)
					sqlfileext := path.Ext(sqlfilepath)
					sqlfilename := sqlfilenameall[0:len(sqlfilenameall) - len(sqlfileext)]

					exafterSqlFile := "sqlfile/"+sqlfilename+"_"+time.Now().Format("2006-01-02_15:04:05")+".sql"
					os.Rename(sqlfilepath, exafterSqlFile)
				}
			}
			return nil
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getSupplier() map[int]map[string]string {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	DB := InitDB("ace")
	suppliers, _ := DB.Query("select supplier_id from supplier_database_config group by supplier_id")

	return getResutl(suppliers)
}

func update(sql string) {
	config, _ := goconfig.LoadConfigFile("config.ini")
	poolnum, _ := config.Int("pool", "poolnum")
	// Use the common pool.
	var wg sync.WaitGroup

	pool, _ := ants.NewPool(poolnum)
	defer pool.Release()

	/*
	   syncCalculateSum := func() {
	       executeSql(sql)
	       wg.Done()
	   }
	*/

	supplier_ids := getSupplier()
	for _, v := range supplier_ids { //查询出来的数组
		wg.Add(1)
		//_ = ants.Submit(syncCalculateSum)
		pool.Submit(executeSql(sql, v["supplier_id"], &wg))
		//fmt.Println(v["supplier_id"])
		//DB := InitDB("ace_supplier_"+v["supplier_id"])
		//DB.Query(sql)
		//fmt.Println(v["supplier_id"])
	}
	wg.Wait()
	//fmt.Printf("running goroutines: %d\n", ants.Running())
	fmt.Printf("完成所有任务.\n")
	fmt.Println(sql)
}

func executeSql(sql string, supplier_id string, wg *sync.WaitGroup) func() {
	return func() {
		//time.Sleep(time.Second * 1)
		DB := InitDB("ace_supplier_" + supplier_id)
		_, err := DB.Exec(sql)
		//_, err := DB.Query(sql)
		//fmt.Printf(err)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("ace_supplier_" + supplier_id)
		DB.Close()
		//fmt.Println("更新供应商：", supplier_id)
		wg.Done()
	}
	//time.Sleep(10 * time.Millisecond)
	//fmt.Println("Hello World!")
	/*
	   fmt.Println(v["supplier_id"])
	   DB := InitDB("ace_supplier_"+v["supplier_id"])
	   DB.Query(sql)
	*/
}

//Db数据库连接池
//var DB *sql.DB

//注意方法名大写，就是public
func InitDB(dbName string) *sql.DB {
	config, err := goconfig.LoadConfigFile("config.ini")
	if err != nil {
		panic("数据库配置读取失败")
	}
	//构建连接："用户名:密码@tcp(IP:端口)/数据库?charset=utf8"
	host,_ := config.GetValue("mysql", "host")
	port,_ := config.GetValue("mysql", "port")
	username,_ := config.GetValue("mysql", "username")
	password,_ := config.GetValue("mysql", "password")
	path := strings.Join([]string{username, ":", password, "@tcp(", host, ":", port, ")/", dbName, "?charset=utf8"}, "")
	//fmt.Println(path)

	//打开数据库,前者是驱动名，所以要导入： _ "github.com/go-sql-driver/mysql"
	DB, _ := sql.Open("mysql", path)
	//设置数据库最大连接数
	DB.SetConnMaxLifetime(100)
	//设置上数据库最大闲置连接数
	DB.SetMaxIdleConns(10)
	//验证连接
	if err := DB.Ping(); err != nil {
		panic("数据库连接失败")
		//fmt.Println("opon database fail")
		//return
	}

	return DB
}

func getResutl(query *sql.Rows) map[int]map[string]string {
	column, _ := query.Columns()              //读出查询出的列字段名
	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度
	for i := range values {                   //让每一行数据都填充到[][]byte里面
		scans[i] = &values[i]
	}
	results := make(map[int]map[string]string) //最后得到的map
	i := 0
	for query.Next() { //循环，让游标往下移动
		if err := query.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			fmt.Println(err)
			panic("查询错误")
		}
		row := make(map[string]string) //每行数据
		for k, v := range values {     //每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		results[i] = row //装入结果集中
		i++
	}

	// for k, v := range results { //查询出来的数组
	//fmt.Println(k, v)
	//}
	return results
}

func readSqlFile(path string) ([]string, error) {
	result := []string{}
	sqlfile, err := ioutil.ReadFile(path)
	if err != nil {
		return result, nil
	}

	sfile := string(sqlfile)
	for _, sql := range strings.Split(sfile, "\n") {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		result = append(result, sql)
	}

	return result, nil
}