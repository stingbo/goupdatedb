##### 采购平台更新分库表结构脚本

1. `mv config.example.ini config.ini`，修改数据库连接，协程池数量配置

2. `go run main.go sql "sql语句"`

3. `go run main.go sqlfile [sql文件名]`，如果没有传sql文件名，则默认取当前目录下`sqlfile/supplier_platform_xxxx-xx-xx.sql`文件，`xxxx-xx-xx`为当前日期.

##### SUMMARY

* [读取配置文件包](https://github.com/unknwon/goconfig)
* [命令行应用程序构建包](https://github.com/urfave/cli)
* [高性能且低损耗的 goroutine 池](https://github.com/panjf2000/ants)
* 获取所有要更新的供应商id，循环使用协程连接数据库，执行SQL语句，释放连接
