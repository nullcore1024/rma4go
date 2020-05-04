// scanner to iterate all keys in the redis
package analyzer

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v7"
	"github.com/nullcore1024/rma4go/cmder"
	log "github.com/sirupsen/logrus"
	_ "gopkg.in/cheggaaa/pb.v1"
	"sync"
	"time"
)

const (
	scanCount   = 1024
	compactSize = 10240

	// cause the real memory used by redis is a little bigger than the content
	// here we plus some extra space for different key types, it's not accurate but the result will be better
	baseSize    = 50
	elementSize = 4
)

func getTotalKeys(client *redis.Client) int {
	dbsize, _ := client.DBSize().Result()
	return int(dbsize)
}

func Run(clis []*redis.Client, sep string, tree, pre, compact bool) RedisStat {
	flagSeparator = sep
	buildPrefix = pre
	buildTree = tree

	wg := &sync.WaitGroup{}

	poolSize := len(clis)

	var stat RedisStat

	cli := clis[poolSize-1]
	dbsize := getTotalKeys(cli)

	keyChan := make(chan string, 4096)
	emit := make(chan *KeyMeta, 4096)
	ctx, cancel := context.WithCancel(context.Background())

	go PipeDo(clis, poolSize-1, dbsize, keyChan, emit, ctx, wg)
	wg.Add(1)
	go ConsumerChanMeta(emit, dbsize, ctx, cancel, &stat, wg)
	wg.Add(1)
	go ProducerKey(cli, dbsize, keyChan, wg)
	log.Infof("dbsize:%d", dbsize)

	wg.Wait()
	return stat
}

func PipeDo(clis []*redis.Client, poolSize, dbsize int, recv chan string, sender chan *KeyMeta, ctx context.Context, wg *sync.WaitGroup) {
	supportMemUsage := checkSupportMemUsage(clis[0])
	limiter := make(chan bool, poolSize)
	defer close(recv)

	i := 0
	for {
		select {
		case key := <-recv:
			{
				if i == dbsize {
					log.Infof("finish key:%d", i)
					break
				}

				wg.Add(1)
				limiter <- true

				go BatchScanKeys(clis[uint32(i)%uint32(poolSize)], supportMemUsage, key, limiter, wg, sender)
				i++
			}
		case <-ctx.Done():
			log.Infof("done finish consumer key:%d", i)
			return
		}
	}
	log.Infof("finish pipe key:%d", i)
}

func ProducerKey(cli *redis.Client, dbsize int, keyChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		ks     []string
		err    error
		count  int
		prev   int
		cursor uint64 = 0
	)
	var batch int
	if (dbsize >> 10) > (1 << 15) {
		batch = dbsize >> 10
	} else {
		batch = dbsize / 10
	}

	log.Infof("%v, start total count:%d, dbsize:%d", time.Now().Local(), count, dbsize)
	//bar := pb.StartNew(dbsize)

	for {
		scmd := cli.Scan(cursor, cmder.GetMatch(), scanCount)
		ks, cursor, err = scmd.Result()
		if len(ks) > 0 {
			count += len(ks)
			for i := range ks {
				keyChan <- ks[i]
			}
			if batch != 0 && count-prev > batch {
				prev = count
				log.Infof("%v, scan key count:%d", time.Now().Local(), count)
			}
		}
		if cursor == 0 || err != nil {
			log.Infof("current size:%d, err:%s", count, err)
			break
		}
	}
	log.Infof("finish scan current size:%d", count)
}

func ConsumerChanMeta(ch chan *KeyMeta, dbsize int, ctx context.Context, cancel func(), stat *RedisStat, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(ch)

	var batch int
	if (dbsize >> 10) > (1 << 15) {
		batch = dbsize >> 10
	} else {
		batch = dbsize / 10
	}
	prev := 0
	count := 0
	for {
		select {
		case meta := <-ch:
			{
				count++
				stat.Merge(*meta)
				if batch != 0 && count-prev > batch {
					prev = count
					log.Infof("%v, current consumer:%d, key=%s", time.Now().Local(), count, meta.Key)
				}
				log.Debugf("type=%s, key=%s, keysz=%d, dataSz=%d", meta.Type, meta.Key, meta.KeySize, meta.DataSize)
				FreeKeyMeta(meta)

				if count >= dbsize {
					log.Infof("finish consumer:%d", count)
					cancel()
					continue
				}
			}
		case <-ctx.Done():
			{
				fmt.Println("notify close", count)
				return
			}
		}
	}
	log.Infof("finish consumer size:%d", count)
}

func BatchScanKeys(cli *redis.Client, supportMemUsage bool, key string, limiter chan bool, wg *sync.WaitGroup, emit chan *KeyMeta) {
	defer wg.Done()
	GetKeysMeta(cli, supportMemUsage, key, emit)
	<-limiter
	return
}

func GetKeysMeta(cli *redis.Client, supportMemUsage bool, key string, emit chan *KeyMeta) int {
	//for i := range keys {
	meta := GetKeyMeta(cli, supportMemUsage, key)
	emit <- meta
	return 0
}

func GetKeyMeta(cli *redis.Client, supportMemUsage bool, key string) *KeyMeta {
	meta := NewKeyMeta()
	meta.Key = key
	meta.KeySize = int64(len(key))
	ttl, err := cli.PTTL(key).Result()
	if err != nil {
		ttl = -1000000
	}
	meta.Ttl = int64(ttl)
	t, e := cli.Type(key).Result()
	if e != nil {
		return meta
	}
	if supportMemUsage {
		meta.DataSize = getLenByMemUsage(cli, key)
	}
	switch t {
	case typeString:
		meta.Type = typeString
		if !supportMemUsage {
			sl, err := cli.StrLen(key).Result()
			if err != nil {
				sl = 0
			}
			meta.DataSize = sl + baseSize
		}
	case typeList:
		meta.Type = typeList
		if !supportMemUsage {
			meta.DataSize = getListLen(key, cli)
		}
	case typeHash:
		meta.Type = typeHash
		if !supportMemUsage {
			meta.DataSize = getLen(key, cli, typeHash)
		}
	case typeSet:
		meta.Type = typeSet
		if !supportMemUsage {
			meta.DataSize = getLen(key, cli, typeSet)
		}
	case typeZSet:
		meta.Type = typeZSet
		if !supportMemUsage {
			meta.DataSize = getLen(key, cli, typeZSet)
		}
	default:
		meta.Type = typeOther
		s, err := cli.Dump(key).Result()
		if err != nil {
			meta.DataSize = 0
		}
		meta.DataSize = int64(len(s))
	}
	return meta
}

func ScanAllKeys(cli *redis.Client, sep string, tree, pre, compact bool) RedisStat {
	flagSeparator = sep
	buildPrefix = pre
	buildTree = tree

	supportMemUsage := checkSupportMemUsage(cli)
	var stat RedisStat
	scmd := cli.Scan(0, cmder.GetMatch(), scanCount)
	count := 0

	dbsize := getTotalKeys(cli)
	var batch int
	if (dbsize >> 10) > (1 << 15) {
		batch = dbsize >> 10
	} else {
		batch = dbsize / 10
	}

	log.Infof("%v, start total count:%d, dbsize:%d", time.Now().Local(), count, dbsize)
	//bar := pb.StartNew(dbsize)

	if scmd != nil {
		ks, cursor, err := scmd.Result()
		if cursor == 0 && len(ks) > 0 {
			count += len(ks)
			MergeKeyMeta(cli, supportMemUsage, ks, &stat)
		}
		for cursor > 0 && err == nil {
			MergeKeyMeta(cli, supportMemUsage, ks, &stat)
			count += len(ks)
			scmd = cli.Scan(cursor, cmder.GetMatch(), scanCount)
			ks, cursor, err = scmd.Result()
			if cursor == 0 {
				if len(ks) > 0 {
					count += len(ks)
					//bar.Add(len(ks))
					MergeKeyMeta(cli, supportMemUsage, ks, &stat)
				}
			}
			if count%batch == 0 {
				log.Infof("%v, current count:%d, dbsize:%d", time.Now().Local(), count, dbsize)
			}
			if err != nil {
				log.Infof("cursor:%d current size:%d, err:%s", count, err)
				break
			}
			// compact for every 40k keys
			if compact && (len(stat.All.Distribution) > compactSize) {
				log.Infof("compacting...   current size:%d", count)
				stat.Compact()
			}
		}
	}
	log.Infof("scan count:%d, dbsize:%d", count, dbsize)
	if compact {
		stat.Compact()
	}
	return stat
}

func MergeKeyMeta(cli *redis.Client, supportMemUsage bool, ks []string, stat *RedisStat) {
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
		if supportMemUsage {
			meta.DataSize = getLenByMemUsage(cli, ks[i])
		}
		switch t {
		case typeString:
			meta.Type = typeString
			if !supportMemUsage {
				sl, err := cli.StrLen(ks[i]).Result()
				if err != nil {
					sl = 0
				}
				meta.DataSize = sl + baseSize
			}
		case typeList:
			meta.Type = typeList
			if !supportMemUsage {
				meta.DataSize = getListLen(ks[i], cli)
			}
		case typeHash:
			meta.Type = typeHash
			if !supportMemUsage {
				meta.DataSize = getLen(ks[i], cli, typeHash)
			}
		case typeSet:
			meta.Type = typeSet
			if !supportMemUsage {
				meta.DataSize = getLen(ks[i], cli, typeSet)
			}
		case typeZSet:
			meta.Type = typeZSet
			if !supportMemUsage {
				meta.DataSize = getLen(ks[i], cli, typeZSet)
			}
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

func getListLen(key string, cli *redis.Client) int64 {
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
		totalLen += int64(len(d)) + elementSize
	}
	return totalLen + baseSize
}

func getLen(key string, cli *redis.Client, t string) int64 {
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
			totalLen += l + elementSize
		}
		cmd = scan(key, cursor, "*", 300)
		ks, cursor, _ = cmd.Result()
	}
	return totalLen + baseSize
}

func getLenByMemUsage(cli *redis.Client, key string) int64 {
	len, err := cli.MemoryUsage(key).Result()
	if err != nil {
		return 0
	}
	return len
}
