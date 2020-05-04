// data structures
package analyzer

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"regexp"
	"sync"
	//"github.com/nullcore1024/rma4go/treeprint"
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
	if len(parts) <= 1 {
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
		return fmt.Sprintf("%dB", size)
	case size < 1024*1024:
		return fmt.Sprintf("%.3gKB", float64(size)/1024)
	case size < 1024*1024*1024:
		return fmt.Sprintf("%.3gMB", float64(size)/(1024*1024))
	default:
		return fmt.Sprintf("%.3gGB", float64(size)/(1024*1024*1024))
	}
}

type KeyMeta struct {
	Key      string
	KeySize  int64
	DataSize int64
	Ttl      int64
	Type     string
}

var KeyMetaPool = sync.Pool{
	New: func() interface{} {
		return new(KeyMeta)
	},
}

func NewKeyMeta() *KeyMeta {
	c := KeyMetaPool.Get().(*KeyMeta)
	return c
}

func FreeKeyMeta(key *KeyMeta) {
	KeyMetaPool.Put(key)
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
	Tree         *DictTree
	Metrics
}

// distributions of keys of all prefixes
type Distribution struct {
	KeyPattern string `json:"pattern"`
	Metrics
}

// basic metrics of a group of key
type Metrics struct {
	KeySize        int64 `json:"keySz" tree:"keySz""`
	DataSize       int64 `json:"dataSz" tree:"dataSz"`
	KeyCount       int32 `json:"keyCount" tree:"keyCount"`
	KeyNeverExpire int32 `json:"-"`
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
	default:
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
	if buildTree {
		stat.PrintTree()
	}
}

type treeNode struct {
	Meta  Metrics    `json:"Meta"`
	Value string     `json:"N"`
	Nodes []treeNode `json:"SN,omitempty"`
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

func buildRoot(root TreePrint, nn *treeNode) {
	if root == nil {
		return
	}
	tree := root.(*DictTree)
	if tree.Nodes == nil {
		return
	}

	nn.Nodes = make([]treeNode, len(tree.Nodes))

	Meta := &Metrics{}
	if m, ok := tree.GetMetaValue().(*Metrics); ok {
		Meta = m
	}
	i := 0
	for k, node := range tree.Nodes {
		an := &treeNode{
			Value: k,
		}
		buildRoot(node, an)
		nn.Nodes[i] = *an
		i++
	}

	nn.Nodes = nn.sortedSlice()
	nn.Value = fmt.Sprintf("%s_%s", nn.Value, formatSize(Meta.estimatedSize()))
	nn.Meta = *Meta
	//fmt.Printf("dir:%s, key:%d, keycount:%d\n", tree.Value, tree.Meta.KeySize, tree.Meta.KeyCount)
	return
}

func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func mkdir(path string) error {
	if IsDir(path) {
		return nil
	}
	err := os.Mkdir(path, os.ModePerm)
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func buildDir(parent string, nn *treeNode) {
	if nn == nil {
		return
	}
	var content []byte
	for i, _ := range nn.Nodes {
		content = append(content, byte('a'+i))
	}

	for i, node := range nn.Nodes {
		rootdir := fmt.Sprintf("%s/%s", parent, node.Value)
		mkdir(rootdir)

		tmpfile := fmt.Sprintf("%s/%d", rootdir, i)
		ioutil.WriteFile(tmpfile, []byte(content[0:len(nn.Nodes)-i]), 0755)

		buildDir(rootdir, &nn.Nodes[i])
	}
}

func (ks *KeyStat) printTree(file string) {
	if ks.Tree != nil {
		Meta := &Metrics{}
		if m, ok := ks.Tree.GetMetaValue().(*Metrics); ok {
			Meta = m
		}
		for _, node := range ks.Tree.Nodes {
			val := node.GetMetaValue()
			if m, ok := val.(*Metrics); ok {
				Meta.KeyCount += m.KeyCount
				Meta.KeySize += m.KeySize
				Meta.DataSize += m.DataSize
			}
		}
		if dump, err := json.Marshal(ks.Tree); err == nil {
			ioutil.WriteFile("tree."+file, []byte(dump), 0755)
		}

		node := treeNode{}
		buildRoot(ks.Tree, &node)

		fmt.Println("build dir", file)

		mkdir("./data/")
		mkdir("./data/" + file)
		buildDir("./data/"+file, &node)

		if dump, err := json.Marshal(node); err == nil {
			ioutil.WriteFile(file, []byte(dump), 0755)
		}

		/*
			tree := treeNode{}
			json.Unmarshal([]byte(ks.Tree.JsonString()), &tree)
			rebuildTreeName(&tree)
			buildRoot(&tree)
			if dump, err := json.Marshal(tree); err == nil {
				ioutil.WriteFile(file, []byte(dump), 0755)
			}
		*/
	}
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

type TreePrint interface {
	AddMetaBranch(v interface{}, name string) TreePrint
	AddMetaNode(v interface{}, name string) TreePrint
	FindByValue(name string) TreePrint
	JsonString() string

	GetMetaValue() interface{}
	SetMetaValue(v interface{})
}

type DictTree struct {
	Name  string               `json:"name"`
	Meta  interface{}          `json:"meta"`
	Nodes map[string]TreePrint `json:"node"`
}

func NEWDict(v interface{}) *DictTree {
	return &DictTree{
		Meta:  v,
		Nodes: make(map[string]TreePrint),
	}
}

func NewDict(name string, v interface{}) TreePrint {
	return &DictTree{
		Name:  name,
		Meta:  v,
		Nodes: make(map[string]TreePrint),
	}
}

func (thiz *DictTree) AddMetaNode(v interface{}, name string) TreePrint {
	node := NewDict(name, v)
	thiz.Nodes[name] = node
	return node
}

func (thiz *DictTree) JsonString() string {
	if dump, err := json.Marshal(thiz); err == nil {
		return string(dump)
	}
	return ""
}

func (thiz *DictTree) AddMetaBranch(v interface{}, name string) TreePrint {
	if thiz.Nodes == nil {
		thiz.Nodes = make(map[string]TreePrint)
	}
	node := NewDict(name, v)
	thiz.Nodes[name] = node
	return node
}

func (thiz *DictTree) FindByValue(name string) TreePrint {
	if val, ok := thiz.Nodes[name]; ok {
		return val
	}
	return nil
}

func (thiz *DictTree) GetMetaValue() interface{} {
	return thiz.Meta
}

func (thiz *DictTree) SetMetaValue(v interface{}) {
	thiz.Meta = v
}

func skipNum(token []string) []string {
	if len(token) <= 2 {
		return token
	}
	del := 0
	sz := len(token) - 1
	pattern := "\\d+" //反斜杠要转义
	result, _ := regexp.MatchString(pattern, token[sz])
	if result {
		del++
	}
	result, _ = regexp.MatchString(pattern, token[sz-1])
	if result {
		del++
	}
	return token[0 : sz-del]
}

func (stat *KeyStat) MergeTree(meta KeyMeta) {
	prefix := getPrefix(meta.Key, 1)
	sp := strings.Split(prefix, flagSeparator)

	token := skipNum(sp)

	if stat.Tree == nil {
		stat.Tree = NEWDict(&Metrics{})
	}
	var root TreePrint = stat.Tree

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

func (stat *KeyStat) Merge(meta KeyMeta) {
	stat.MergeMeta(meta)
	if buildTree {
		stat.MergeTree(meta)
	}
	if !buildPrefix {
		return
	}

	dists := stat.Distribution
	if dists == nil {
		dists = make(map[string]Distribution, defaultSize)
	}

	del := 1
	if sp := strings.Split(meta.Key, flagSeparator); len(sp) >= 4 {
		if len(sp) > 5 {
			del = 2
		} else {
			sz := len(sp) - 1
			pattern := "\\d+" //反斜杠要转义
			result, _ := regexp.MatchString(pattern, sp[sz-1])
			if result {
				del = 2
			}
		}

	}
	prefix := getPrefix(meta.Key, del)

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
	//fmt.Printf("prefix key:%s, raw key:%s\n", prefix, meta.Key)

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
