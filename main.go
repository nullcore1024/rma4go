package main

import (
	"github.com/nullcore1024/rma4go/analyzer"
	"github.com/nullcore1024/rma4go/client"
	"github.com/nullcore1024/rma4go/cmder"
	"github.com/winjeg/redis"

	"flag"
	// _ "net/http/pprof"
	"strings"
)

type Client = redis.UniversalClient

func main() {
	//go func() {
	//	http.ListenAndServe("0.0.0.0:8899", nil)
	//}()

	flag.Parse()
	if cmder.ShowHelp() {
		flag.Usage()
		return
	}
	printKeyStat()
}

func printKeyStat() {
	var cli Client
	cluster := cmder.GetCluster()
	if len(cluster) > 0 {
		urls := strings.Split(cluster, ",")
		cli = client.BuildClusterClient(urls, cmder.GetAuth())
	} else {
		h := cmder.GetHost()
		a := cmder.GetAuth()
		p := cmder.GetPort()
		cli = client.BuildRedisClient(client.ConnInfo{
			Host: h,
			Auth: a,
			Port: p,
		}, cmder.GetDb())
	}

	stat := analyzer.ScanAllKeys(cli, cmder.GetSeparator(), cmder.GetTree(), cmder.GetPrefix(), cmder.GetCompact())
	stat.Print()
}
