# gocache

gocache 是一个 Go 内存缓存库。它用预分配的环形缓冲区保存键和值，通过 256 个独立加锁的分段支持并发访问，并提供过期、淘汰、批量读取和迭代功能。

## 安装

需要 Go 1.24 或更新版本。

```sh
go get github.com/Kirep-06/gocache
```

## 使用

```go
package main

import (
	"fmt"

	"github.com/Kirep-06/gocache"
)

func main() {
	cache := gocache.NewCache(100 * 1024 * 1024)
	key := []byte("hello")
	if err := cache.Set(key, []byte("world"), 60); err != nil {
		panic(err)
	}
	value, err := cache.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))
}
```

`Set` 的第三个参数是过期秒数；小于或等于零表示不过期。创建缓存时会预分配内存，最小容量为 512 KiB。条目即使未过期，也可能在空间不足时被淘汰。

## 示例服务器

`server` 目录包含一个使用 Redis 风格协议的简单 TCP 服务器，支持 `PING`、`DBSIZE`、`GET`、`SET`、`SETEX` 和 `DEL`，并支持连续发送多条命令。

```sh
go run ./server
```

默认监听 `:7788`，性能分析端口为 `localhost:6060`。
