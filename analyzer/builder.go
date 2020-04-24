// data structures
package analyzer

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/nullcore1024/rma4go/treeprint"
	"github.com/olekukonko/tablewriter"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

const (
	defaultSize    = 128
	compactNum     = 30
	maxLeftNum     = 150
	minKeyLenLower = 2
	minKeyLen      = 5
	// expire section
	Second = 1000000000
	Minute = Second * 60
	Hour   = Minute * 60
	Day    = Hour * 24
	Week   = Day * 7

	// data size
	B  = 1
	KB = B * 1024
	MB = KB * 1024

	typeString = "string"
	typeHash   = "hash"
	typeList   = "list"
	typeSet    = "set"
	typeZSet   = "zset"
	typeOther  = "other"

	metricSize = 8
)

var (
	flagSeparator string = ":"
	buildPrefix   bool   = false
	buildTree     bool   = false
)

func getPrefix(key string, sub int) string {
	parts := strings.Split(key, flagSeparator)
	if len(parts) <= 0 {
		return key
	}
	if len(parts) <= sub {
		return getPrefix(key, sub-1)
	}

	token := []string{"*", "*", "*", "*", "*"}
	return strings.Join(parts[:len(parts)-sub], flagSeparator) + flagSeparator + strings.Join(token[:sub], flagSeparator)
}

type PrefixItems map[string]*PrefixItem

func (items PrefixItems) sortedSlice() []*PrefixItem {
	// Pull all items out of the map
	slice := make([]*PrefixItem, len(items))
	i := 0
	for _, item := range items {
		slice[i] = item
		i += 1
	}

	// Sort by size
	sort.Slice(slice, func(i, j int) bool {
		// Sort by "size desc, count desc"
		if slice[i].estimatedSize() == slice[j].estimatedSize() {
			return slice[i].count > slice[j].count
		}

		return slice[i].estimatedSize() > slice[j].estimatedSize()
	})

	return slice
}

type PrefixItem struct {
	count         int
	totalBytes    int
	numberOfDumps int
	prefix        string
}

func (item PrefixItem) averageBytesPerKey() float64 {
	if item.numberOfDumps == 0 {
		return 0
	}

	return float64(item.totalBytes) / float64(item.numberOfDumps)
}

func (item PrefixItem) estimatedSize() int64 {
	return int64(item.averageBytesPerKey() * float64(item.count))
}

func formatSize(size int64) string {
	switch {
	case size < 1024:
		return fmt.Sprintf("%d B", size)
	case size < 1024*1024:
		return fmt.Sprintf("%.3g KB", float64(size)/1024)
	case size < 1024*1024*1024:
		return fmt.Sprintf("%.3g MB", float64(size)/(1024*1024))
	default:
		return fmt.Sprintf("%.3g GB", float64(size)/(1024*1024*1024))
	}
}

type KeyMeta struct {
	Key      string
	KeySize  int64
	DataSize int64
	Ttl      int64
	Type     string
}

type RedisStat struct {
	All     KeyStat `json:"all"`
	String  KeyStat `json:"string"`
	Hash    KeyStat `json:"hash"`
	Set     KeyStat `json:"set"`
	List    KeyStat `json:"list"`
	ZSet    KeyStat `json:"zset"`
	Other   KeyStat `json:"other"`
	BigKeys KeyStat `json:"bigKeys"`
}

func sortedSlice(Prefixes map[string]Distribution) []Distribution {
	// Pull all items out of the map
	slice := make([]Distribution, len(Prefixes))
	i := 0
	for _, item := range Prefixes {
		slice[i] = item
		i += 1
	}

	// Sort by size
	sort.Slice(slice, func(i, j int) bool {
		// Sort by "size desc, count desc"
		if slice[i].estimatedSize() == slice[j].estimatedSize() {
			return slice[i].KeyCount > slice[j].KeyCount
		}

		return slice[i].estimatedSize() > slice[j].estimatedSize()
	})

	return slice
}

// total stat and distributions
type KeyStat struct {
	Distribution map[string]Distribution `json:"distribution"`
	Prefixes     map[string]Distribution `json:"prefix"`
	FPrefixes    map[string]Distribution `json:"prefix"`
	GFPrefixes   map[string]Distribution `json:"prefix"`
	Tree         treeprint.Tree
	Metrics
}

// distributions of keys of all prefixes
type Distribution struct {
	KeyPattern string `json:"pattern"`
	Metrics
}

// basic metrics of a group of key
type Metrics struct {
	KeyCount       int64 `json:"keyCount" tree:"keyCount"`
	KeySize        int64 `json:"keySize" tree:"keySz""`
	DataSize       int64 `json:"dataSize" tree:"dataSz"`
	KeyNeverExpire int64 `json:"NExpire"`
	ExpireInHour   int64 `json:"-"` // >= 0h < 1h
	ExpireInDay    int64 `json:"-"` // >= 1h < 24h
	ExpireInWeek   int64 `json:"-"` // >= 1d < 7d
	ExpireOutWeek  int64 `json:"-"` // >= 7d
}

func (meta Metrics) estimatedSize() int64 {
	return meta.KeySize + meta.DataSize
}

func (m *Metrics) MergeMeta(meta KeyMeta) {
	m.DataSize += meta.DataSize
	m.KeySize += meta.KeySize
	m.KeyCount++
	switch {
	case meta.Ttl < 0:
		m.KeyNeverExpire++
	case meta.Ttl >= 0 && meta.Ttl < Hour:
		m.ExpireInHour++
	case meta.Ttl >= Hour && meta.Ttl < Day:
		m.ExpireInDay++
	case meta.Ttl >= Day && meta.Ttl < Week:
		m.ExpireInWeek++
	case meta.Ttl >= Week:
		m.ExpireOutWeek++
	}
}

func (m *Metrics) data() []string {
	result := make([]string, 0, metricSize)
	result = append(result, fmt.Sprintf("%d", m.KeyCount))
	//result = append(result, fmt.Sprintf("%d", m.KeySize))
	//result = append(result, fmt.Sprintf("%d", m.DataSize))
	result = append(result, formatSize(m.KeySize))
	result = append(result, formatSize(m.DataSize))
	result = append(result, fmt.Sprintf("%d", m.ExpireInHour))
	result = append(result, fmt.Sprintf("%d", m.ExpireInDay))
	result = append(result, fmt.Sprintf("%d", m.ExpireInWeek))
	result = append(result, fmt.Sprintf("%d", m.ExpireOutWeek))
	result = append(result, fmt.Sprintf("%d", m.KeyNeverExpire))
	return result
}

func (stat *RedisStat) Compact() {
	stat.All.compact()
	stat.String.compact()
	stat.BigKeys.compact()
	stat.Other.compact()
	stat.Hash.compact()
	stat.ZSet.compact()
	stat.Set.compact()
	stat.List.compact()
}

func (stat *RedisStat) Merge(meta KeyMeta) {
	stat.All.Merge(meta)
	// big keys
	if meta.DataSize >= 1*MB {
		stat.BigKeys.Merge(meta)
	}
	switch meta.Type {
	case typeString:
		stat.String.Merge(meta)
	case typeList:
		stat.List.Merge(meta)
	case typeHash:
		stat.Hash.Merge(meta)
	case typeSet:
		stat.Set.Merge(meta)
	case typeZSet:
		stat.ZSet.Merge(meta)
	default:
		stat.Other.Merge(meta)
	}
}

func (stat *RedisStat) PrintTree() {
	stat.All.printTree("all.json")
	stat.String.printTree("string.json")
	stat.List.printTree("list.json")
	stat.Hash.printTree("hash.json")
	stat.Set.printTree("set.json")
	stat.ZSet.printTree("zset.json")
	stat.Other.printTree("other.json")
	stat.BigKeys.printTree("bigokey.json")
}

func (stat *RedisStat) PrintPrefix() {
	color.Green("\n\nall keys statistics\n\n")
	stat.All.printPrefixTable()
	stat.All.printPrefixParentTable()
	stat.All.printPrefixGParentTable()
	color.Green("\n\nstring keys statistics\n\n")
	stat.String.printPrefixTable()
	stat.String.printPrefixParentTable()
	stat.String.printPrefixGParentTable()
	color.Green("\n\nlist keys statistics\n\n")
	stat.List.printPrefixTable()
	stat.List.printPrefixParentTable()
	stat.List.printPrefixGParentTable()
	color.Green("\n\nhash keys statistics\n\n")
	stat.Hash.printPrefixTable()
	stat.Hash.printPrefixParentTable()
	stat.Hash.printPrefixGParentTable()
	color.Green("\n\nset keys statistics\n\n")
	stat.Set.printPrefixTable()
	stat.Set.printPrefixParentTable()
	stat.Set.printPrefixGParentTable()
	color.Green("\n\nzset keys statistics\n\n")
	stat.ZSet.printPrefixTable()
	stat.ZSet.printPrefixParentTable()
	stat.ZSet.printPrefixGParentTable()
	color.Green("\n\nother keys statistics\n\n")
	stat.Other.printPrefixTable()
	stat.Other.printPrefixParentTable()
	stat.Other.printPrefixGParentTable()
	color.Green("\n\nbig keys statistics\n\n")
	stat.BigKeys.printPrefixTable()
}

func (stat *RedisStat) Print() {

	color.Green("\n\nall keys statistics\n\n")
	stat.All.printTable()
	color.Green("\n\nstring keys statistics\n\n")
	stat.String.printTable()
	color.Green("\n\nlist keys statistics\n\n")
	stat.List.printTable()
	color.Green("\n\nhash keys statistics\n\n")
	stat.Hash.printTable()
	color.Green("\n\nset keys statistics\n\n")
	stat.Set.printTable()
	color.Green("\n\nzset keys statistics\n\n")
	stat.ZSet.printTable()
	color.Green("\n\nother keys statistics\n\n")
	stat.Other.printTable()
	color.Green("\n\nbig keys statistics\n\n")
	stat.BigKeys.printTable()

	color.Green("\n\n==================abcpreifx===================\n\n")
	if buildPrefix {
		stat.PrintPrefix()
	}
	stat.PrintTree()
}

type treeNode struct {
	Meta  Metrics    `json:"Meta"`
	Value string     `json:"N"`
	Nodes []treeNode `json:"SN"`
}

func (items treeNode) sortedSlice() []treeNode {
	// Pull all items out of the map
	slice := make([]treeNode, len(items.Nodes))
	i := 0
	for _, item := range items.Nodes {
		slice[i] = item
		i += 1
	}
	// Sort by size
	sort.Slice(slice, func(i, j int) bool {
		// Sort by "size desc, count desc"
		if slice[i].Meta.estimatedSize() == slice[j].Meta.estimatedSize() {
			return slice[i].Meta.KeyCount > slice[j].Meta.KeyCount
		}

		return slice[i].Meta.estimatedSize() > slice[j].Meta.estimatedSize()
	})

	return slice
}

func rebuildTreeName(tree *treeNode) {
	if tree.Nodes == nil {
		return
	}
	tree.Nodes = tree.sortedSlice()
	tree.Value = fmt.Sprintf("%s_%s", tree.Value, formatSize(tree.Meta.estimatedSize()))
	for i, _ := range tree.Nodes {
		if tree.Nodes[i].Nodes != nil {
			tree.Nodes[i].Nodes = tree.Nodes[i].sortedSlice()
			rebuildTreeName(&tree.Nodes[i])
		}
	}
	return
}

func (ks *KeyStat) printTree(file string) {
	if ks.Tree != nil {
		tree := treeNode{}
		json.Unmarshal([]byte(ks.Tree.JsonString()), &tree)
		rebuildTreeName(&tree)
		if dump, err := json.Marshal(tree); err == nil {
			ioutil.WriteFile(file, []byte(dump), 0755)
		}
	}
}

func (ks *KeyStat) printPrefixTable() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"pattern", "key num", "key size", "data size", "expire in hour", "expire in day",
		"expire in week", "expire out week", "never expire"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")

	for _, v := range sortedSlice(ks.Prefixes) {
		table.Append(v.tableData())
	}
	footer := make([]string, 0, metricSize+1)
	footer = append(footer, "total")
	footer = append(footer, ks.data()...)
	table.Append(footer)
	table.Render()
}

func (ks *KeyStat) printPrefixParentTable() {
	prefix := sortedSlice(ks.FPrefixes)
	if len(prefix) == 0 {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"fpattern", "key num", "key size", "data size", "expire in hour", "expire in day",
		"expire in week", "expire out week", "never expire"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")

	for _, v := range prefix {
		table.Append(v.tableData())
	}
	footer := make([]string, 0, metricSize+1)
	footer = append(footer, "total")
	footer = append(footer, ks.data()...)
	table.Append(footer)
	table.Render()
}

func (ks *KeyStat) printPrefixGParentTable() {
	prefix := sortedSlice(ks.GFPrefixes)
	if len(prefix) == 0 {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"gfpattern", "key num", "key size", "data size", "expire in hour", "expire in day",
		"expire in week", "expire out week", "never expire"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")

	for _, v := range prefix {
		table.Append(v.tableData())
	}
	footer := make([]string, 0, metricSize+1)
	footer = append(footer, "total")
	footer = append(footer, ks.data()...)
	table.Append(footer)
	table.Render()
}

func (ks *KeyStat) printTable() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"pattern", "key num", "key size", "data size", "expire in hour", "expire in day",
		"expire in week", "expire out week", "never expire"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")

	for _, v := range ks.Distribution {
		table.Append(v.tableData())
	}
	footer := make([]string, 0, metricSize+1)
	footer = append(footer, "total")
	footer = append(footer, ks.data()...)
	table.Append(footer)
	table.Render()
}

func (dist *Distribution) tableData() []string {
	result := make([]string, 0, metricSize+1)
	result = append(result, dist.KeyPattern)
	result = append(result, dist.data()...)
	return result
}

func (stat *KeyStat) MergeTree(meta KeyMeta) {
	prefix := getPrefix(meta.Key, 1)
	token := strings.Split(prefix, flagSeparator)

	if stat.Tree == nil {
		stat.Tree = treeprint.New()
	}
	var root treeprint.Tree = stat.Tree

	for i, v := range token {
		find := root.FindByValue(v)
		if find == nil {
			m := &Metrics{}
			m.MergeMeta(meta)
			if i == len(token)-1 {
				root.AddMetaNode(m, v)
			} else {
				root = root.AddMetaBranch(m, v)
			}
		} else {
			root = find
			val := find.GetMetaValue()
			if m, ok := val.(*Metrics); ok {
				m.MergeMeta(meta)
				find.SetMetaValue(m)
			}
		}
	}
}

func (stat *KeyStat) MergePrefix(meta KeyMeta) {
	prefix := getPrefix(meta.Key, 1)
	stat.FillPrefix(&stat.Prefixes, prefix, meta)

	fprefix := getPrefix(meta.Key, 2)
	if strings.Compare(prefix, fprefix) != 0 {
		stat.FillPrefix(&stat.FPrefixes, fprefix, meta)

		gfprefix := getPrefix(meta.Key, 4)
		find := false

		for k, _ := range stat.GFPrefixes {
			if strings.HasPrefix(gfprefix, k) {
				find = true
				gfprefix = k
			}
		}

		if !find {
			gfprefix = getPrefix(meta.Key, 3)
		}

		if strings.Compare(gfprefix, fprefix) != 0 {
			stat.FillPrefix(&stat.GFPrefixes, gfprefix, meta)
		}
	}
	/*
		prefix := getPrefix(meta.Key)

		if stat.Prefixes == nil {
			stat.Prefixes = make(map[string]Distribution, defaultSize)
		}

		if _, ok := stat.Prefixes[prefix]; !ok {
			stat.Prefixes[prefix] = Distribution{}
		}
		c := stat.Prefixes[prefix]
		c.KeyPattern = prefix
		c.KeyCount += 1
		c.KeySize += meta.KeySize
		c.DataSize += meta.DataSize
		stat.Prefixes[prefix] = c
	*/
}

func (stat *KeyStat) FillPrefix(Prefixes *map[string]Distribution, prefix string, meta KeyMeta) {
	if *Prefixes == nil {
		*Prefixes = make(map[string]Distribution, defaultSize)
	}

	if _, ok := (*Prefixes)[prefix]; !ok {
		(*Prefixes)[prefix] = Distribution{}
	}
	c := (*Prefixes)[prefix]
	c.KeyPattern = prefix
	c.KeyCount += 1
	c.KeySize += meta.KeySize
	c.DataSize += meta.DataSize
	switch {
	case meta.Ttl < 0:
		c.KeyNeverExpire++
	case meta.Ttl >= 0 && meta.Ttl < Hour:
		c.ExpireInHour++
	case meta.Ttl >= Hour && meta.Ttl < Day:
		c.ExpireInDay++
	case meta.Ttl >= Day && meta.Ttl < Week:
		c.ExpireInWeek++
	case meta.Ttl >= Week:
		c.ExpireOutWeek++
	}

	(*Prefixes)[prefix] = c
}

func (stat *KeyStat) Merge(meta KeyMeta) {
	stat.MergeMeta(meta)
	if buildPrefix {
		stat.MergePrefix(meta)
	}
	if buildTree {
		stat.MergeTree(meta)
	}

	dists := stat.Distribution
	if dists == nil {
		dists = make(map[string]Distribution, defaultSize)
	}

	if token := strings.Split(meta.Key, flagSeparator); len(token) > 1 {
		prefix := getPrefix(meta.Key, 1)
		if v, ok := dists[prefix]; ok {
			d := Distribution(v)
			d.MergeMeta(meta)
			dists[prefix] = d
		} else {
			var d Distribution
			d.MergeMeta(meta)
			d.KeyPattern = prefix
			dists[prefix] = d
		}
	} else {
		keyLen := len(meta.Key)

		// check for if there are already some key in the collection
		inMap := false
		for i := 0; i < keyLen; i++ {
			x := meta.Key[0 : i+1]
			if v, ok := dists[x]; ok {
				d := Distribution(v)
				d.MergeMeta(meta)
				dists[x] = d
				inMap = true
			}
		}
		//
		if !inMap {
			var d Distribution
			d.MergeMeta(meta)
			dists[meta.Key] = d
		}
	}

	stat.Distribution = dists
}

func (stat *KeyStat) compact() {
	distMap := stat.Distribution
	tmpMap := make(map[string][]string, defaultSize)
	shrinkTo := compactNum
	for k := range distMap {
		compactedKey := k
		if orgks, ok := tmpMap[compactedKey]; ok {
			orgks = append(orgks, k)
			tmpMap[compactedKey] = orgks
		} else {
			ks := make([]string, 0, defaultSize)
			ks = append(ks, k)
			tmpMap[compactedKey] = ks
		}
	}
	shrinkTo--
	for (len(tmpMap) > compactNum && shrinkTo >= minKeyLen) || (len(tmpMap) > maxLeftNum && shrinkTo >= minKeyLenLower) {
		tnMap := make(map[string][]string, defaultSize)
		for k := range tmpMap {
			// shrink
			if len(k) > shrinkTo {
				compactedKey := k[0:shrinkTo]
				if oik, ok := tnMap[compactedKey]; ok {
					oik = append(oik, tmpMap[k]...)
					tnMap[compactedKey] = oik

				} else {
					tnMap[compactedKey] = tmpMap[k]
				}
			} else {
				tnMap[k] = tmpMap[k]
			}
		}

		// 如果此次shrink 没有使得这个集合的元素数量增加， 就使用原来的key
		for k := range tmpMap {
			if len(k) > shrinkTo {
				ck := k[0:shrinkTo]
				if len(tnMap[ck]) == len(tmpMap[k]) && len(tnMap[ck]) > 1 {
					tnMap[k] = tnMap[ck]
					delete(tnMap, ck)
				}
			}
		}
		tmpMap = tnMap
		// memory need to be released after use
		// tnMap = nil
		shrinkTo--
	}

	dists := make(map[string]Distribution, defaultSize)
	for k, v := range tmpMap {
		if len(v) > 1 {
			var nd Distribution
			for _, dk := range v {
				d := distMap[dk]
				if !strings.HasSuffix(k, "*") {
					nd.KeyPattern = k + "*"
				} else {
					nd.KeyPattern = k
				}
				nd.KeyCount += d.KeyCount
				nd.KeySize += d.KeySize
				nd.DataSize += d.DataSize
				nd.ExpireInHour += d.ExpireInHour
				nd.ExpireInWeek += d.ExpireInWeek
				nd.ExpireInDay += d.ExpireInDay
				nd.ExpireOutWeek += d.ExpireOutWeek
				nd.KeyNeverExpire += d.KeyNeverExpire
			}
			dists[k] = nd
		} else {
			for _, dk := range v {
				nd := distMap[dk]
				nd.KeyPattern = dk + "*"
				dists[dk] = nd
			}
		}
	}
	stat.Distribution = dists
	// memory need to be released after use
	// tmpMap = nil
}
