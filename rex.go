// Command rex executes given command on multiple remote hosts, connecting to
// them via ssh in parallel.
//
// You're expected to have passwordless acces to hosts, rex authenticates itself
// speaking to ssh-agent that is expected to be running.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/artyom/autoflags"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/net/proxy"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] host1 host2:port user@host3:port...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

type Config struct {
	Concurrency int    `flag:"n,concurrent ssh sessions"`
	Command     string `flag:"cmd,command to run"`
	Login       string `flag:"l,default login"`
	Port        int    `flag:"p,default port"`
	GroupFile   string `flag:"g,yaml file with host groups"`
	KnownHosts  string `flag:"knownHosts,path to known_hosts file to verify host keys against, empty to disable"`
	StdinFile   string `flag:"stdin,REGULAR (no piping!) file to pass to stdin of remote command"`
	DumpFiles   bool   `flag:"logs,save stdout/stderr to separate per-host logs"`
	StdoutFmt   string `flag:"logs.stdout,format of stdout per-host log name"`
	StderrFmt   string `flag:"logs.stderr,format of stderr per-host log name"`
	WithSuffix  bool   `flag:"fullnames,do not strip common suffix in hostname output"`

	DialTimeout  time.Duration `flag:"dt,dial timeout"`
	ForwardAgent bool          `flag:"a,forward ssh-agent connection"`

	stdoutPrefix, stderrPrefix string
	stdoutIsTerm, stderrIsTerm bool

	commonSuffix string
	maxHostWidth int
}

var (
	// overridable at build time like
	// go build -ldflags="-X=main.stdoutPrefix=☺︎"
	stdoutPrefix = "."
	stderrPrefix = "E"
)

func main() {
	log.SetFlags(0)
	conf := Config{
		Concurrency: 100,
		Login:       os.Getenv("REX_USER"),
		Port:        22,
		GroupFile:   filepath.FromSlash(path.Join(os.Getenv("HOME"), ".rex-groups.yaml")),
		KnownHosts:  filepath.FromSlash(path.Join(os.Getenv("HOME"), ".ssh/known_hosts")),
		StdoutFmt:   filepath.Join(os.TempDir(), "${"+remoteHostVarname+"}.stdout"),
		StderrFmt:   filepath.Join(os.TempDir(), "${"+remoteHostVarname+"}.stderr"),
		DialTimeout: 10 * time.Second,

		stdoutPrefix: stdoutPrefix,
		stderrPrefix: stderrPrefix,
	}
	if conf.Login == "" {
		conf.Login = os.Getenv("USER")
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
		conf.stdoutIsTerm = true
		conf.stdoutPrefix = string(escape.Green) + conf.stdoutPrefix + string(escape.Reset)
	}
	if isTerminal(os.Stderr) {
		conf.stderrIsTerm = true
		conf.stderrPrefix = string(escape.Yellow) + conf.stderrPrefix + string(escape.Reset)
	}
	switch err := run(conf, hosts); err {
	case nil:
	case errSomeJobFailed:
		os.Exit(123)
	default:
		log.Fatal(err)
	}
}

func run(conf Config, hosts []string) error {
	var sshAgent agent.Agent
	agentConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		return err
	}
	sshAgent = agent.NewClient(agentConn)
	defer agentConn.Close()

	signers, err := sshAgent.Signers()
	if err != nil {
		return err
	}
	hostKeyCallback := ssh.InsecureIgnoreHostKey()
	if conf.KnownHosts != "" {
		fn, err := knownhosts.New(conf.KnownHosts)
		if err != nil {
			return fmt.Errorf("failed to parse known_hosts: %v", err)
		}
		hostKeyCallback = fn
	}

	authMethods := []ssh.AuthMethod{ssh.PublicKeys(signers...), ssh.KeyboardInteractive(keyboardChallenge)}

	var wg sync.WaitGroup

	limit := make(chan struct{}, conf.Concurrency)

	hosts, err = expandGroups(hosts, conf.GroupFile)
	if err != nil {
		return err
	}
	hosts = uniqueHosts(hosts)
	if !conf.WithSuffix {
		conf.commonSuffix = commonSuffix(hosts)
	}
	for _, host := range hosts {
		if l := len(host); l > conf.maxHostWidth {
			conf.maxHostWidth = l
		}
	}

	var errCnt int32
	for _, host := range hosts {
		limit <- struct{}{}
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			defer func() { <-limit }()
			switch err := RemoteCommand(host, conf, sshAgent, hostKeyCallback, authMethods); {
			case err == nil && conf.DumpFiles:
				fmt.Println(host, "processed")
			case err == nil:
			default:
				atomic.AddInt32(&errCnt, 1)
				if conf.stderrIsTerm {
					host = string(escape.Red) + host + string(escape.Reset)
				}
				log.Println(host, err)
			}
		}(host)
	}
	wg.Wait()
	if errCnt > 0 {
		return errSomeJobFailed
	}
	return nil
}

func RemoteCommand(addr string, conf Config, sshAgent agent.Agent, cb ssh.HostKeyCallback, authMethods []ssh.AuthMethod) error {
	login, addr := loginAndAddr(conf.Login, addr, conf.Port)
	sshConfig := &ssh.ClientConfig{
		User:            login,
		Auth:            authMethods,
		HostKeyCallback: cb,
	}
	client, err := sshDial("tcp", addr, conf.DialTimeout, sshConfig)
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if conf.ForwardAgent {
		if err := agent.ForwardToAgent(client, sshAgent); err != nil {
			return err
		}
		if err := agent.RequestAgentForwarding(session); err != nil {
			return err
		}
	}

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

	var copyDone sync.WaitGroup // used to guard completion of stdout/stderr dumps
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

		host := host // shadow var
		width := conf.maxHostWidth
		if !conf.WithSuffix {
			h_ := strings.TrimSuffix(host, conf.commonSuffix)
			if h_ != host {
				host = h_ + "…"
				width -= (len(conf.commonSuffix) - 1)
			}
		}
		copyDone.Add(2)
		go byLineCopy(fmt.Sprintf("%[1]s %-[3]*[2]s ", conf.stdoutPrefix, host, width), os.Stdout, stdoutPipe, &copyDone)
		go byLineCopy(fmt.Sprintf("%[1]s %-[3]*[2]s ", conf.stderrPrefix, host, width), os.Stderr, stderrPipe, &copyDone)
	}

	err = session.Run(conf.Command)
	copyDone.Wait()
	return err
}

func byLineCopy(prefix string, sink io.Writer, pipe io.Reader, wg *sync.WaitGroup) {
	defer wg.Done()
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

func sshDial(network, addr string, dialTimeout time.Duration, config *ssh.ClientConfig) (*ssh.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	conn, err := proxy.Dial(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		return nil, err
	}
	return ssh.NewClient(c, chans, reqs), nil
}

var errSomeJobFailed = fmt.Errorf("some job(s) failed")

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

func uniqueHosts(hosts []string) []string {
	seen := make(map[string]struct{})
	out := hosts[:0]
	for _, v := range hosts {
		if _, ok := seen[v]; !ok {
			out = append(out, v)
			seen[v] = struct{}{}
		}
	}
	return out
}

func expandGroups(hosts []string, groupFile string) ([]string, error) {
	var groups map[string][]string
	for _, v := range hosts {
		if strings.HasPrefix(v, "@") {
			data, err := ioutil.ReadFile(groupFile)
			if err != nil {
				return nil, err
			}
			groups = make(map[string][]string)
			if err := yaml.Unmarshal(data, groups); err != nil {
				return nil, err
			}
			goto expand
		}
	}
	return hosts, nil // no need to read/parse file if groups are not used
expand:
	var out []string
	for _, v := range hosts {
		if strings.HasPrefix(v, "@") {
			out = append(out, groups[v[1:]]...)
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

func commonSuffix(l []string) string {
	if len(l) < 2 {
		return ""
	}
	min := reverse(l[0])
	max := min
	for _, s := range l[1:] {
		switch rs := reverse(s); {
		case rs < min:
			min = rs
		case rs > max:
			max = rs
		}
	}
	for i := 0; i < len(min) && i < len(max); i++ {
		if min[i] != max[i] {
			return reverse(min[:i])
		}
	}
	return reverse(min)
}

func reverse(s string) string {
	rs := []rune(s)
	if len(rs) < 2 {
		return s
	}
	for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
		rs[i], rs[j] = rs[j], rs[i]
	}
	return string(rs)
}

func keyboardChallenge(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
	if len(questions) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("keyboard interactive challenge is not supported")
}

const keyEscape = 27

// copy of v100EscapeCodes from golang.org/x/crypto/ssh/terminal/terminal.go
var escape = struct {
	Black, Red, Green, Yellow, Blue, Magenta, Cyan, White, Reset []byte
}{
	Black:   []byte{keyEscape, '[', '3', '0', 'm'},
	Red:     []byte{keyEscape, '[', '3', '1', 'm'},
	Green:   []byte{keyEscape, '[', '3', '2', 'm'},
	Yellow:  []byte{keyEscape, '[', '3', '3', 'm'},
	Blue:    []byte{keyEscape, '[', '3', '4', 'm'},
	Magenta: []byte{keyEscape, '[', '3', '5', 'm'},
	Cyan:    []byte{keyEscape, '[', '3', '6', 'm'},
	White:   []byte{keyEscape, '[', '3', '7', 'm'},

	Reset: []byte{keyEscape, '[', '0', 'm'},
}
