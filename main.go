package main

import (
	"github.com/winjeg/redis"
	"github.com/winjeg/rma4go/analyzer"
	"github.com/winjeg/rma4go/client"
	"github.com/winjeg/rma4go/cmder"

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

	stat := analyzer.ScanAllKeys(cli, cmder.GetSeparator())
	stat.Print()
}
