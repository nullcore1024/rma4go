// mainly deal with the args that is passed to the command line
package cmder

import (
	"flag"
	"github.com/fatih/color"
)

var (
	host          string
	port          int
	auth          string
	thread        int
	db            int
	compact       bool
	help          bool
	prefix        bool
	tree          bool
	cluster       string
	match         string
	flagSeparator string
)

func init() {
	flag.BoolVar(&help, "h", false, "help content")
	flag.BoolVar(&prefix, "pp", false, "build prefix stat")
	flag.BoolVar(&compact, "z", false, "compact key")
	flag.BoolVar(&tree, "t", false, "build tree stat")
	flag.StringVar(&host, "H", "localhost", "address of a redis")
	flag.StringVar(&host, "r", "localhost", "address of a redis")
	flag.IntVar(&port, "p", 6379, "port of the redis")
	flag.StringVar(&auth, "a", "", "password/auth of the redis")
	flag.StringVar(&flagSeparator, "s", ":", "separator semi, default :")
	flag.StringVar(&match, "m", "*", "match the pattern to scan, like 'a*'")
	flag.IntVar(&db, "d", 0, "db of the redis to analyze")
	flag.IntVar(&thread, "w", 8, "worker thread num")
	flag.StringVar(&cluster, "c", "", "cluster info separated by comma, like localhost:123,localhost:456")

	flag.Usage = usage
}

func usage() {
	color.Cyan("rma4go usage:")
	color.Green("rma4go -r some_host -p 6379 -a password -d 0")
	color.Yellow("======================================================")
	flag.PrintDefaults()
}

func GetHost() string {
	return host
}

func GetPort() int {
	return port
}
func GetAuth() string {
	return auth
}

func GetDb() int {
	return db
}

func GetCluster() string {
	return cluster
}

func GetMatch() string {
	return match
}

func GetSeparator() string {
	return flagSeparator
}

func GetPrefix() bool {
	return prefix
}

func GetTree() bool {
	return tree
}

func GetCompact() bool {
	return compact
}

func GetThread() int {
	return thread
}
