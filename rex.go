package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/artyom/autoflags"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
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
	Port        int    `flag:"p,port"`
}

func main() {
	log.SetFlags(log.Lshortfile)
	conf := Config{
		Concurrency: 100,
		Login:       "root",
		Port:        22,
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
			select {
			case item := <-stdout:
				fmt.Println(item)
			case item := <-stderr:
				fmt.Println(item)
			}
		}
	}()

	limit := make(chan struct{}, conf.Concurrency)

	for _, host := range hosts {
		limit <- struct{}{}
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			defer func() { <-limit }()
			if err := RemoteCommand(host, conf, config, stdout, stderr); err != nil {
				log.Println(err)
			}
		}(host)
	}
	wg.Wait()
}

func RemoteCommand(host string, conf Config, config *ssh.ClientConfig, stdout, stderr chan<- string) error {
	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", host, conf.Port), config)
	if err != nil {
		return err
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	stdoutPipe, err := session.StdoutPipe()
	if err != nil {
		return err
	}
	stderrPipe, err := session.StderrPipe()
	if err != nil {
		return err
	}

	go pipeFeeder("OUT\t"+host, stdoutPipe, stdout)
	go pipeFeeder("ERR\t"+host, stderrPipe, stderr)

	return session.Run(conf.Command)
}

func pipeFeeder(prefix string, pipe io.Reader, sink chan<- string) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		sink <- prefix + "\t" + scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		return // TODO: report error to separate channel
	}
}
