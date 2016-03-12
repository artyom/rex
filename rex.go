package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

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

	stdoutPrefix, stderrPrefix string
}

func main() {
	log.SetFlags(0)
	conf := Config{
		Concurrency: 100,
		Login:       "root",
		Port:        22,
		StdoutFmt:   "/tmp/${" + remoteHostVarname + "}.stdout",
		StderrFmt:   "/tmp/${" + remoteHostVarname + "}.stderr",

		stdoutPrefix: ".",
		stderrPrefix: "E",
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
		if err := checkFilenameTemplate(conf.StdoutFmt); err != nil {
			log.Fatal("stdout filename format:", err)
		}
		if err := checkFilenameTemplate(conf.StderrFmt); err != nil {
			log.Fatal("stderr filename format:", err)
		}
	}
	if isTerminal(os.Stdout) {
		conf.stdoutPrefix = chalk.Green.Color(conf.stdoutPrefix)
	}
	if isTerminal(os.Stderr) {
		conf.stderrPrefix = chalk.Red.Color(conf.stderrPrefix)
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

	authMethods := []ssh.AuthMethod{ssh.PublicKeys(signers...)}

	var wg sync.WaitGroup

	limit := make(chan struct{}, conf.Concurrency)

	uniqueHosts := make(map[string]struct{})
	for _, host := range hosts {
		uniqueHosts[host] = struct{}{}
	}

	var errCnt int32
	for host := range uniqueHosts {
		limit <- struct{}{}
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			defer func() { <-limit }()
			switch err := RemoteCommand(host, conf, authMethods); {
			case err == nil && conf.DumpFiles:
				fmt.Println(host, "processed")
			case err == nil:
			default:
				atomic.AddInt32(&errCnt, 1)
				log.Println(host, err)
			}
		}(host)
	}
	wg.Wait()
	if errCnt > 0 {
		os.Exit(123)
	}
}

func RemoteCommand(addr string, conf Config, authMethods []ssh.AuthMethod) error {
	login, addr := loginAndAddr(conf.Login, addr, conf.Port)
	sshConfig := &ssh.ClientConfig{
		User: login,
		Auth: authMethods,
	}
	client, err := sshDial("tcp", addr, sshConfig)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}

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
			return fmt.Errorf("file passed to stdin is not a regular file")
		}
		session.Stdin = f
	}

	switch {
	case conf.DumpFiles:
		stdoutLog, err := os.Create(filepath.Clean(expandHostname(conf.StdoutFmt, host)))
		if err != nil {
			return err
		}
		defer closeAndRemoveIfAt0(stdoutLog)
		session.Stdout = stdoutLog
		stderrLog, err := os.Create(filepath.Clean(expandHostname(conf.StderrFmt, host)))
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

		go byLineCopy(fmt.Sprintf("%s %s\t", conf.stdoutPrefix, host), os.Stdout, stdoutPipe)
		go byLineCopy(fmt.Sprintf("%s %s\t", conf.stderrPrefix, host), os.Stderr, stderrPipe)
	}

	return session.Run(conf.Command)
}

func byLineCopy(prefix string, sink io.Writer, pipe io.Reader) {
	buf := []byte(prefix)
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		buf := buf[:len(prefix)]
		buf = append(buf, scanner.Bytes()...)
		buf = append(buf, '\n')
		// this is safe to write a single line to stdout/stderr without
		// additional locking from multiple goroutines as os guarantees
		// those writes are atomic (for stdout/stderr only)
		sink.Write(buf)
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

func loginAndAddr(defaultLogin, addr string, defaultPort int) (login, hostPort string) {
	login = defaultLogin
	u, err := url.Parse("//" + addr) // add slashes so that it properly parsed as url
	if err != nil {
		return defaultLogin, addr
	}
	if u.User != nil {
		login = u.User.Username()
	}
	if _, _, err := net.SplitHostPort(u.Host); err == nil {
		return login, u.Host
	}
	return login, net.JoinHostPort(u.Host, strconv.Itoa(defaultPort))
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

const remoteHostVarname = `REX_REMOTE_HOST`

func expandHostname(s, hostname string) string {
	return os.Expand(s, func(x string) string {
		if x == remoteHostVarname {
			return hostname
		}
		return os.Getenv(s)
	})
}

func checkFilenameTemplate(s string) error {
	varSet := os.Expand(s, func(x string) string {
		if x == remoteHostVarname {
			return "value"
		}
		return ""
	})
	varUnset := os.Expand(s, func(string) string { return "" })
	if varSet == varUnset {
		return fmt.Errorf("no ${%s} in pattern", remoteHostVarname)
	}
	return nil
}
