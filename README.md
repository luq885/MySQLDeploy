# MySQLDeploy
mysql 自动更新部署工具
可以自动执行文件夹中ver(X)中的sql语句，并记录当前执行版本，修改数据库只要增加新的ver(X+1)文件夹，放入sql语句，运行即可部署新版本

```
./DBDeploy -server 127.0.0.1 -port 3306 -password 123456 -name testDB -sql ./testSQL
```