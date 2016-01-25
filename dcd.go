package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/op/go-logging"
	"github.com/sevlyar/go-daemon"
)

var log = logging.MustGetLogger("dcd")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

var (
	daemonMode = flag.Bool("d", false, "daemon mode")
	debug      = flag.Bool("v", false, "verbose output")
	cassandra  = flag.String("db", "localhost", "cassandra endpoint")
	socket     = flag.String("a", "/run/dcd.socket", "communication socket")
	repoCfg    = flag.String("f", "", "repo configuration: -f /file.tgz:/workspace:/cache,...")
	//ws          = flag.String("w", "/cfg", "workspace root")
	force       = flag.Bool("o", false, "overwrite repo contents")
	consistency = flag.String("c", "quorum", "cassandra consistency level (r/w)")
	//cacheDir    = flag.String("s", "/.dcdcache", "cache directory")
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s (edit|commit|get|update)\n", os.Args[0])
	flag.PrintDefaults()
}

func run() {
	var consistencyLevel gocql.Consistency
	if *consistency == "quorum" {
		consistencyLevel = gocql.Quorum
	} else if *consistency == "one" {
		consistencyLevel = gocql.One
	} else if *consistency == "all" {
		consistencyLevel = gocql.All
	} else {
		log.Fatal("Unsupported consistency level: %s", *consistency)
	}

	cluster := gocql.NewCluster(*cassandra)
	cluster.DiscoverHosts = true
	cluster.Timeout = 2 * time.Second
	cluster.Consistency = consistencyLevel

	session, _ := cluster.CreateSession()
	defer session.Close()

	systems := make(map[string]*System)

	repos := strings.Split(*repoCfg, ",")
	if repos != nil {
		for _, repo := range repos {
			rc := strings.SplitN(repo, ":", 3)
			if len(rc) != 3 {
				log.Fatal("Invalid repo configuration: %s", repo)
			}

			s := &Storage{
				Session: session,
				File:    rc[0],
			}

			if err := s.initStorage(); err != nil {
				log.Fatal(err)
			}

			w := &Workspace{
				Root: rc[1],
			}

			c := &Cache{
				CacheDir:  rc[2],
				ChunkSize: 65536,
			}

			if err := c.initCache(); err != nil {
				log.Fatal(err)
			}

			system := NewSystem(s, c, w)

			system.runUpdate()

			systems[rc[0]] = system
		}
	}

	server := NewHttpServerUnixSocket(*socket, systems)
	defer server.Close()

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	logging.SetFormatter(format)
	if *debug {
		logging.SetLevel(logging.DEBUG, "dcd")
	} else {
		logging.SetLevel(logging.ERROR, "dcd")
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc)
	go func() {
		s := <-sc
		ssig := s.(syscall.Signal)
		log.Error("Signal received: %s", ssig.String())
		os.Exit(128 + int(ssig))
	}()

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() == 0 {
		if *daemonMode {
			ctx := daemon.Context{}
			child, err := ctx.Reborn()
			if err != nil {
				log.Error("Cannot start child process: %s", err.Error())
				os.Exit(1)
			}
			if child != nil {
				log.Info("Daemon started")
			} else {
				defer ctx.Release()
				run()
			}
		} else {
			run()
		}
	} else {
		if flag.NArg() < 2 {
			Usage()
			os.Exit(2)
		}
		command := flag.Arg(0)
		file := flag.Arg(1)
		client := NewClientUnixSocket(*socket, file)
		switch command {
		case "get":
			err := client.Get(os.Stdout)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}
		case "edit":
			err := client.Edit()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}
		case "commit":
			err := client.Commit(*force)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}
		case "update":
			err := client.Update(*force)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(1)
			}
		}
	}
}
