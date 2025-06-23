# raselper
 运维辅助工具

## 运行raseper

### filehelper

**打包zip文件并生成对应格式化的日期名**
**批量复制文件**
**批量对文件重命名**
**批量替换文件中的内容**
**过滤文件中特定的行并生成新文件**

### md5

**列出指定文件的md5值**
**删除目录下所有重复文件**

```shell
# 运行名称批量修改
filehelper rname catalina.out .out .log
# 运行切割日志
filehelper filter D:\Temporary\log\catalina.out [Thread-25] catalina.log
```

## 打包
**raseper linux**
```shell
GOOS=linux GOARCH=amd64 go build -o rasepler rasepler/main.go
```
**raseper windows**
```shell
GOOS=windows GOARCH=amd64 go build -o rasepler.exe rasepler/main.go
```

**示例**
```shell
go build -o your_app_name.exe main.go # windows
go build -o your_app_name main.go # windows
go build main.go # windows
go build app/raselper/main.go

# 为 Linux 系统打包
GOOS=linux GOARCH=amd64 go build -o your_app_linux main.go
# 为 Windows 系统打包
GOOS=windows GOARCH=amd64 go build -o your_app_windows.exe main.go
# 为 macOS 打包
GOOS=darwin GOARCH=amd64 go build -o your_app_mac main.go
```