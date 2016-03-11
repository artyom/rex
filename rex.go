package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/artyom/autoflags"
	"github.com/ttacon/chalk"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/net/proxy"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] ip1 ip2 ...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

type Config struct {
	Concurrency int    `flag:"n,concurrent ssh sessions"`
	Command     string `flag:"cmd,command to run"`
	Login       string `flag:"l,login to use"`
	Port        int    `flag:"p,default port"`
	StdinFile   string `flag:"stdin,REGULAR (no piping!) file to pass to stdin of remote command"`
	DumpFiles   bool   `flag:"logs,save stdout/stderr to separate per-host logs"`
	StdoutFmt   string `flag:"logs.stdout,format of stdout per-host log name"`
	StderrFmt   string `flag:"logs.stderr,format of stderr per-host log name"`
}

func main() {
	log.SetFlags(0)
	conf := Config{
		Concurrency: 100,
		Login:       "root",
		Port:        22,
		StdoutFmt:   "/tmp/%s.stdout",
		StderrFmt:   "/tmp/%s.stderr",
	}
	autoflags.Define(&conf)
	flag.Parse()
	if conf.Concurrency < 1 {
		conf.Concurrency = 1
	}
	hosts := flag.Args()
	if len(hosts) == 0 || len(conf.Command) == 0 {
		flag.Usage()
		return
	}
	if conf.DumpFiles {
		if conf.StdoutFmt == conf.StderrFmt {
			log.Fatal("file format for stdout and stderr should differ")
		}
		if !strings.Contains(conf.StdoutFmt, `%s`) {
			log.Fatal("invalid format for stdout log name")
		}
		if !strings.Contains(conf.StderrFmt, `%s`) {
			log.Fatal("invalid format for stderr log name")
		}
	}
	var sshAgent agent.Agent
	agentConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		log.Fatal(err)
	}
	sshAgent = agent.NewClient(agentConn)
	defer agentConn.Close()

	signers, err := sshAgent.Signers()
	if err != nil {
		log.Fatal(err)
	}

	config := &ssh.ClientConfig{
		User: conf.Login,
		Auth: []ssh.AuthMethod{ssh.PublicKeys(signers...)},
	}

	var wg sync.WaitGroup

	stdout := make(chan string, 10)
	stderr := make(chan string, 10)
	go func() {
		for {
			stdoutPrefix, stderrPrefix := ".", "E"
			if isTerminal(os.Stdout) {
				stdoutPrefix = chalk.Green.Color(stdoutPrefix)
			}
			if isTerminal(os.Stderr) {
				stderrPrefix = chalk.Red.Color(stderrPrefix)
			}
			select {
			case item := <-stdout:
				fmt.Fprintln(os.Stdout, stdoutPrefix, item)
			case item := <-stderr:
				fmt.Fprintln(os.Stderr, stderrPrefix, item)
			}
		}
	}()

	limit := make(chan struct{}, conf.Concurrency)

	uniqueHosts := make(map[string]struct{})
	for _, host := range hosts {
		uniqueHosts[host] = struct{}{}
	}

	for host := range uniqueHosts {
		limit <- struct{}{}
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			defer func() { <-limit }()
			switch err := RemoteCommand(host, conf, config, stdout, stderr); {
			case err == nil && conf.DumpFiles:
				fmt.Println(host, "processed")
			case err == nil:
			default:
				log.Println(host, err)
			}
		}(host)
	}
	wg.Wait()
}

func RemoteCommand(host string, conf Config, config *ssh.ClientConfig, stdout, stderr chan<- string) error {
	addr := host
	if !strings.ContainsRune(addr, ':') {
		addr = fmt.Sprintf("%s:%d", addr, conf.Port)
	}
	client, err := sshDial("tcp", addr, config)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if conf.StdinFile != "" {
		f, err := os.Open(conf.StdinFile)
		if err != nil {
			return err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() {
			return errors.New("file passed to stdin is not a regular file")
		}
		session.Stdin = f
	}

	switch {
	case conf.DumpFiles:
		stdoutLog, err := os.Create(filepath.Clean(fmt.Sprintf(conf.StdoutFmt, host)))
		if err != nil {
			return err
		}
		defer closeAndRemoveIfAt0(stdoutLog)
		session.Stdout = stdoutLog
		stderrLog, err := os.Create(filepath.Clean(fmt.Sprintf(conf.StderrFmt, host)))
		if err != nil {
			return err
		}
		defer closeAndRemoveIfAt0(stderrLog)
		session.Stderr = stderrLog
	default:
		stdoutPipe, err := session.StdoutPipe()
		if err != nil {
			return err
		}
		stderrPipe, err := session.StderrPipe()
		if err != nil {
			return err
		}

		go pipeFeeder(host, stdoutPipe, stdout)
		go pipeFeeder(host, stderrPipe, stderr)
	}

	return session.Run(conf.Command)
}

func pipeFeeder(prefix string, pipe io.Reader, sink chan<- string) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		sink <- prefix + "\t" + scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		log.Print("string scanner error", err)
	}
}

func closeAndRemoveIfAt0(f *os.File) {
	if n, err := f.Seek(0, os.SEEK_CUR); err == nil && n == 0 {
		os.Remove(f.Name())
	}
	f.Close()
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeDevice != 0
}

func sshDial(network, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
	conn, err := proxyDialer.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		return nil, err
	}
	return ssh.NewClient(c, chans, reqs), nil
}

var proxyDialer = proxy.FromEnvironment()
