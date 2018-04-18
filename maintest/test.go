package main

import (
	"strings"
	"net/http"
	"fmt"
	"os"
	"flag"
	"bufio"
	"io"
	"github.com/leo0o/groupcache"
)

const defaultHost = "127.0.0.1:9001"
const groupAddr = ":8081"

func main() {
	if len(os.Args) <= 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s peer1 [peer2...]", os.Args[0])
		os.Exit(1)
	}

	//本地peer地址
	self := flag.String("self", defaultHost, "self node")
	flag.Parse()

	//cache集群所有节点
	cluster := os.Args[1:]

	//初始化本地groupcache, 并监听groupcache相应的端口
	setUpGroup("test_cache")
	//本地peer
	peers := groupcache.NewHTTPPool(addrsToUrl(*self)[0])
	peers.Set(addrsToUrl(cluster...)...) //设置集群信息 用以本机缓存没命中的时候，一致性哈希查找key的存储节点, 并通过http请求访问

	selfPort := strings.Split(*self, ":")[1]
	http.ListenAndServe(":"+selfPort, peers) //监听本机集群内部通信的端口

}

//启动groupcache
func setUpGroup(name string) {
	//缓存池,
	stringGroup := groupcache.NewGroup(name, 1<<20, groupcache.GetterFunc(func(_ groupcache.Context, key string, dest groupcache.Sink) error {
		//当cache miss之后，用来执行的load data方法
		fp, err := os.Open("groupcache.conf")
		if err != nil {
			return err
		}
		defer fp.Close()

		fmt.Printf("look up for %s from config_file\n", key)
		//按行读取配置文件
		buf := bufio.NewReader(fp)
		for {
			line, err := buf.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					dest.SetBytes([]byte{})
					return nil
				} else {
					return err
				}
			}

			line = strings.TrimSpace(line)
			parts := strings.Split(line, "=")
			if len(parts) > 2 {
				continue
			} else if parts[0] == key {
				dest.SetBytes([]byte(parts[1]))
				return nil
			} else {
				continue
			}
		}
	}))

	http.HandleFunc("/config", func(rw http.ResponseWriter, r *http.Request) {
		k := r.URL.Query().Get("key")
		var dest []byte
		fmt.Printf("look up for %s from groupcache\n", k)
		if err := stringGroup.Get(nil, k, groupcache.AllocatingByteSliceSink(&dest)); err != nil {
			rw.WriteHeader(http.StatusNotFound)
			rw.Write([]byte("this key doesn't exists"))
		} else {
			rw.Write([]byte(dest))
		}

	})

	//能够直接访问cache的端口, 启动http服务
	//http://ip:group_addr/config?key=xxx
	go http.ListenAndServe(groupAddr, nil)

}

//将ip:port转换成url的格式
func addrsToUrl(nodeList ...string) []string {
	urls := make([]string, len(nodeList))
	for k, addr := range nodeList {
		urls[k] = "http://" + addr
	}

	return urls
}