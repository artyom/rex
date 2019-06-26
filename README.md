# rex (remote execute)

rex executes given command(s) on multiple remote hosts, ssh-connecting to them
in parallel.

You're expected to have passwordless access to hosts, rex authenticates itself
with the help of ssh-agent that is expected to be running.

[![asciicast](https://asciinema.org/a/960vkpm5z1ktwm3jhuh5kvgxo.png)](https://asciinema.org/a/960vkpm5z1ktwm3jhuh5kvgxo)

## Setup

	cd $(mktemp -d) && go mod init tmp && go get github.com/artyom/rex

Binary will be placed either to directory set with `${GOBIN}` environment or to `${GOPATH}/bin` which defaults to `${HOME}/go/bin`.

## Usage examples

Execute `uptime` command on multiple hosts:

	rex -l root -cmd 'uptime' host1.example.com host2.example.com

Default username may be overridden on a per-host basis:

	rex -l root -cmd 'uptime' host1.example.com admin@host2.example.com

Default username can be set as environment variable:

	export REX_USER=admin

If output should be saved to files, not printed to console, use `-logs` flag:

	rex -logs -l root -cmd '/etc/init.d/mysql restart' db1.example.com db2.example.com

By default output would be saved to `/tmp/db1.example.com.stdout`,
`/tmp/db1.example.com.stderr`, etc. Empty files would be automatically removed.
Override filename templates with `-logs.stderr` and `-logs.stdout` flags.

When complex processing is required on remote hosts, it can be easier to write
separate script and pass it via `-stdin` flag:

	rex -stdin my-script.sh -cmd '/bin/sh' host1.example.com host2.example.com

Note, `-stdin` argument should be a **regular file**, since it is read multiple
times — one time per remote host, so giving something pipe-like here won't
work.

The same mechanism can be used to copy arbitrary data:

	rex -stdin archive.tar.gz -cmd 'tar xz -C /tmp && cp -v /tmp/file-from-archive /dst' host1 host2

Since in most workflows multiple hosts are used together over and over again,
it's easier to group them together. rex treats hostname starting with `@`
symbol as group name; groups are read from yaml file specified by `-g` flag
which defaults to `$HOME/.rex-groups.yaml`. Consider the file with following
content:

	alpha:
	  - alpha1.example.com
	  - alpha2.example.com
	  - alpha3.example.com
	beta:
	  - beta1.example.com
	  - beta2.example.com
	  - beta3.example.com
	  - beta4.example.com

Now rex can use groups:

	rex -cmd 'uptime' @alpha @beta

Both groups and arbitrary hosts can be used together.

## License

MIT
