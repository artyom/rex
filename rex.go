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

	"code.google.com/p/go.crypto/ssh"
	"code.google.com/p/go.crypto/ssh/agent"
)

var concurrency uint
var command string

func init() {
	flag.UintVar(&concurrency, "n", 10, "concurrent ssh sessions")
	flag.StringVar(&command, "c", "uptime", "command to execute")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] ip1 ip2 ...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	hosts := flag.Args()
	if len(hosts) == 0 {
		flag.Usage()
		return
	}
	log.SetFlags(log.Lshortfile)
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
		User: "root",
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

	limit := make(chan struct{}, concurrency)

	for _, host := range hosts {
		limit <- struct{}{}
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			defer func() { <-limit }()
			if err := RemoteCommand(host, command, config, stdout, stderr); err != nil {
				log.Println(err)
			}
		}(host)
	}
	wg.Wait()
}

func RemoteCommand(host, command string, config *ssh.ClientConfig, stdout, stderr chan<- string) error {
	client, err := ssh.Dial("tcp", host+":22", config)
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

	if err := session.Start(command); err != nil {
		return err
	}
	return session.Wait()
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
