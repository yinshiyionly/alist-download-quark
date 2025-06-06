# Alist Download Quark

这是一个用于递归获取Alist文件列表并下载的工具。它会遍历指定目录下的所有文件，并将文件信息保存到MySQL数据库中，然后支持批量下载文件。

## 功能特点

- 递归获取目录下所有文件信息
- 支持分页获取
- 使用MySQL数据库存储文件信息
- 通过文件签名避免重复处理
- 异步处理提高效率
- 支持断点续传下载
- 自动清理磁盘空间
- 循环执行支持中断恢复

## 安装

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 创建.env文件并配置以下内容：
```
# API配置
API_BASE_URL=http://your-alist-server:5678
DOWNLOAD_HOST=http://your-alist-server:5678

# MySQL配置
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=alist

# 下载配置
OUT_PATH=/path/to/your/output/directory
BATCH_SIZE=5
SLEEP_TIME=60
```

3. 更新数据库表结构：
```bash
mysql -u your_user -p your_database < update_table.sql
```

## 使用方法

### 1. 文件列表获取

```bash
python main.py
```

### 2. 文件下载

```bash
python download-to-local.py
```

## 脚本说明

### main.py
- 递归获取Alist中的文件列表
- 将文件信息保存到MySQL数据库

### download-to-local.py
- 从数据库中获取未下载的文件
- 支持断点续传下载
- 下载成功后自动删除文件释放空间
- 循环执行，支持中断恢复

## 配置参数说明

### 数据库配置
- `MYSQL_HOST`: MySQL主机地址
- `MYSQL_PORT`: MySQL端口
- `MYSQL_USER`: MySQL用户名
- `MYSQL_PASSWORD`: MySQL密码
- `MYSQL_DATABASE`: MySQL数据库名

### 下载配置
- `OUT_PATH`: 下载文件的输出目录
- `BATCH_SIZE`: 每次处理的文件数量
- `SLEEP_TIME`: 每次循环后的休眠时间(秒)
- `DOWNLOAD_HOST`: 下载文件的主机地址

## 注意事项

- 程序会自动创建MySQL数据库表
- 文件路径作为唯一键，避免重复记录
- 下载成功后会自动删除文件释放空间
- 程序支持断点续传，可以多次运行而不会重复处理已记录的文件 