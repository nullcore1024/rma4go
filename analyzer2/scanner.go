// scanner to iterate all keys in the redis
package analyzer

import (
	"context"
	"fmt"
	"github.com/nullcore1024/rma4go/cmder"
	"github.com/winjeg/redis"
	_ "gopkg.in/cheggaaa/pb.v1"
	"sync"
	"time"
)

const (
	scanCount   = 512
	compactSize = 10240

	// cause the real memory used by redis is a little bigger than the content
	// here we plus some extra space for different key types, it's not accurate but the result will be better
	baseSize    = 50
	elementSize = 4
)

func getTotalKeys(client redis.UniversalClient) int {
	dbsize, _ := client.DBSize().Result()
	return int(dbsize)
}

func Run(clis []redis.UniversalClient, sep string, tree, pre, compact bool) RedisStat {
	flagSeparator = sep
	buildPrefix = pre
	buildTree = tree

	poolSize := len(clis)

	var stat RedisStat

	cli := clis[poolSize-1]
	dbsize := getTotalKeys(cli)

	ctx, cancel := context.WithCancel(context.Background())

	metaCh, errcConsumer := ConsumerChanMeta(dbsize, ctx, cancel, &stat)
	keyChan, errcKey := PipeDo(clis, poolSize-1, metaCh, ctx)

	errcScan := ProducerKey(cli, dbsize, keyChan)

	if err := Wait(ctx, cancel, errcScan, errcConsumer, errcKey); err != nil {
		fmt.Printf("%v", err)
	}
	fmt.Printf("dbsize:%d\n", dbsize)

	return stat
}

func Wait(ctx context.Context, cancel context.CancelFunc, errcs ...<-chan error) error {
	errs := make([]error, len(errcs))
	var wg sync.WaitGroup
	wg.Add(len(errcs))
	for index, errc := range errcs {
		go func(index int, errc <-chan error) {
			defer wg.Done()
			err := <-errc
			if err != nil && err != ctx.Err() {
				cancel() // notify all to stop
			}
			errs[index] = err
		}(index, errc)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil && err != ctx.Err() {
			return err
		}
	}
	return errs[0]
}

func PipeDo(clis []redis.UniversalClient, poolSize int, sender chan<- KeyMeta, ctx context.Context) (chan<- string, <-chan error) {
	supportMemUsage := checkSupportMemUsage(clis[0])
	limiter := make(chan bool, poolSize)

	ch := make(chan string, 4096)
	errc := make(chan error, 1)

	wg := &sync.WaitGroup{}

	i := 0
	go func() {
		defer close(ch)
		errc <- func() error {
			for {
				select {
				case key := <-ch:
					wg.Add(1)
					limiter <- true

					go BatchScanKeys(clis[i%poolSize], supportMemUsage, key, limiter, sender, wg)
					i++

				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		}()
		wg.Wait()
	}()
	return ch, errc
}

func ProducerKey(cli redis.UniversalClient, dbsize int, keyChan chan<- string) <-chan error {
	var (
		ks     []string
		err    error
		count  int
		prev   int
		cursor uint64 = 0
	)
	errc := make(chan error, 1)

	var batch int
	if (dbsize >> 10) > (1 << 15) {
		batch = dbsize >> 10
	} else {
		batch = dbsize / 10
	}

	fmt.Printf("%v, start total count:%d, dbsize:%d\n", time.Now(), count, dbsize)
	//bar := pb.StartNew(dbsize)

	go func() {
		errc <- func() error {
			for {
				scmd := cli.Scan(cursor, cmder.GetMatch(), scanCount)
				ks, cursor, err = scmd.Result()
				if len(ks) > 0 {
					count += len(ks)
					for i := range ks {
						fmt.Printf("produce key:%s\n", ks[i])
						keyChan <- ks[i]
					}
					if batch != 0 && count-prev > batch {
						prev = count
						fmt.Printf("%v, scan key count:%d\n", time.Now(), count)
					}
				}
				if cursor == 0 || err != nil {
					fmt.Printf("current size:%d, err:%s\n", count, err)
					break
				}
			}
			return nil
		}()
		fmt.Printf("finish current size:%d, err:%s\n", count, err)
	}()
	return errc
}

func ConsumerChanMeta(dbsize int, ctx context.Context, cancel func(), stat *RedisStat) (chan<- KeyMeta, <-chan error) {
	ch := make(chan KeyMeta, 4096)
	errc := make(chan error, 1)

	var batch int
	if (dbsize >> 10) > (1 << 15) {
		batch = dbsize >> 10
	} else {
		batch = dbsize / 10
	}
	prev := 0
	count := 0
	go func() {
		defer close(ch)
		errc <- func() error {
			for meta := range ch {
				count++
				stat.Merge(meta)

				if batch != 0 && count-prev > batch {
					prev = count
					fmt.Printf("%v, current consumer:%d, key:%s\n", time.Now(), count, meta.Key)
				}
				if count == dbsize {
					cancel()
					fmt.Printf("finish current keyMeta size:%d\n", count)
				}

				select {
				case <-ctx.Done():
					fmt.Println("notify close", count)
					return ctx.Err()
				default:
				}
			}
			return nil
		}()
	}()
	return ch, errc
}

func BatchScanKeys(cli redis.UniversalClient, supportMemUsage bool, key string, limiter chan bool, emit chan<- KeyMeta, wg *sync.WaitGroup) {
	defer wg.Done()
	GetKeysMeta(cli, supportMemUsage, key, emit)
	<-limiter
	return
}

func GetKeysMeta(cli redis.UniversalClient, supportMemUsage bool, key string, emit chan<- KeyMeta) int {
	//for i := range keys {
	meta := GetKeyMeta(cli, supportMemUsage, key)
	emit <- meta
	return 0
}

func GetKeyMeta(cli redis.UniversalClient, supportMemUsage bool, key string) KeyMeta {
	var meta KeyMeta
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

func ScanAllKeys(cli redis.UniversalClient, sep string, tree, pre, compact bool) RedisStat {
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

	fmt.Printf("%v, start total count:%d, dbsize:%d\n", time.Now(), count, dbsize)
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
				fmt.Printf("%v, current count:%d, dbsize:%d\n", time.Now(), count, dbsize)
			}
			if err != nil {
				fmt.Printf("cursor:%d current size:%d, err:%s\n", count, err)
				break
			}
			// compact for every 40k keys
			if compact && (len(stat.All.Distribution) > compactSize) {
				fmt.Printf("compacting...   current size:%d\n", count)
				stat.Compact()
			}
		}
	}
	fmt.Printf("scan count:%d, dbsize:%d", count, dbsize)
	if compact {
		stat.Compact()
	}
	return stat
}

func MergeKeyMeta(cli redis.UniversalClient, supportMemUsage bool, ks []string, stat *RedisStat) {
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
		totalLen += int64(len(d)) + elementSize
	}
	return totalLen + baseSize
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
			totalLen += l + elementSize
		}
		cmd = scan(key, cursor, "*", 300)
		ks, cursor, _ = cmd.Result()
	}
	return totalLen + baseSize
}

func getLenByMemUsage(cli redis.UniversalClient, key string) int64 {
	len, err := cli.MemoryUsage(key).Result()
	if err != nil {
		return 0
	}
	return len
}
