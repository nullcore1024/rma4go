// scanner to iterate all keys in the redis
package analyzer

import (
	"fmt"
	"github.com/go-redis/redis"
)

const (
	scanCount = 64
	compactSize = 40960
)

func ScanAllKeys(cli redis.UniversalClient) RedisStat {
	var stat RedisStat
	scmd := cli.Scan(0, "*", scanCount)
	count := 0
	if scmd != nil {
		ks, cursor, err := scmd.Result()
		if cursor == 0 && len(ks) > 0 {
			count += len(ks)
			MergeKeyMeta(cli, ks, &stat)
		}
		// MergeKeyMeta(cli, ks, &stat)
		for cursor > 0 && err == nil {
			MergeKeyMeta(cli, ks, &stat)
			count += len(ks)
			scmd = cli.Scan(cursor, "*", scanCount)
			ks, cursor, err = scmd.Result()
			if cursor == 0 {
				if len(ks) > 0 {
					count += len(ks)
					MergeKeyMeta(cli, ks, &stat)
				}
			}
		}
		// compact for every 40k keys
		if len(stat.All.Distribution) > compactSize {
			stat.Compact()
		}

	}
	fmt.Println("total count", count)
	stat.Compact()
	return stat
}

func MergeKeyMeta(cli redis.UniversalClient, ks []string, stat *RedisStat) {
	for i := range ks {
		var meta KeyMeta
		meta.Key = ks[i]
		meta.KeySize = int64(len(ks[i]))
		ttl, err := cli.PTTL(ks[i]).Result()
		if err != nil {
			ttl = -1000000
		}
		meta.Ttl = int64(ttl)
		t, e := cli.Type(ks[i]).Result()
		if e != nil {
			continue
		}
		switch t {
		case typeString:
			meta.Type = typeString
			sl, err := cli.StrLen(ks[i]).Result()
			if err != nil {
				sl = 0
			}
			meta.DataSize = sl
		case typeList:
			meta.Type = typeList
			meta.DataSize = getListLen(ks[i], cli)
		case typeHash:
			meta.Type = typeHash
			meta.DataSize = getLen(ks[i], cli, typeHash)
		case typeSet:
			meta.Type = typeSet
			meta.DataSize = getLen(ks[i], cli, typeSet)
		case typeZSet:
			meta.Type = typeZSet
			meta.DataSize = getLen(ks[i], cli, typeZSet)

		default:
			meta.Type = typeOther
			s, err := cli.Dump(ks[i]).Result()
			if err != nil {
				meta.DataSize = 0
			}
			meta.DataSize = int64(len(s))
		}
		stat.Merge(meta)
	}
}

func getListLen(key string, cli redis.UniversalClient) int64 {
	l, err := cli.LLen(key).Result()
	if l == 0 || err != nil {
		return 0
	}
	var totalLen int64
	for i := int64(0); i < l; i++ {
		d, err := cli.LIndex(key, int64(i)).Result()
		if err != nil {
			continue
		}
		totalLen += int64(len(d))
	}
	return totalLen
}

func getLen(key string, cli redis.UniversalClient, t string) int64 {
	var cursor uint64 = 0
	var ks []string
	var totalLen int64
	var scan func(key string, cursor uint64, match string, count int64) *redis.ScanCmd
	switch t {
	case typeHash:
		scan = cli.HScan
	case typeSet:
		scan = cli.SScan
	case typeZSet:
		scan = cli.ZScan
	}
	cmd := scan(key, cursor, "*", 300)
	ks, cursor, _ = cmd.Result()
	for cursor != 0 {
		for _, v := range ks {
			var l int64
			switch t {
			case typeHash:
				f, e := cli.HGet(key, v).Result()
				if e != nil {
					continue
				}
				// field  and value
				l = int64(len(f)) + int64(len(v))
			case typeSet:
				// element len
				l = int64(len(v))
			case typeZSet:
				l = int64(len(v) + 2)
			}
			totalLen += l
		}
		cmd = scan(key, cursor, "*", 300)
		ks, cursor, _ = cmd.Result()
	}
	return totalLen
}
