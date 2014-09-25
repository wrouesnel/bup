% bup-init(1) Bup %BUP_VERSION%
% Avery Pennarun <apenwarr@gmail.com>
% %BUP_DATE%

# NAME

bup-init - initialize a bup repository

# SYNOPSIS

[BUP_DIR=*localpath*] bup init [-r [*user*@]*host*:*path*]

# DESCRIPTION

`bup init` initializes your local bup repository.  By default, BUP_DIR
is `~/.bup`.

# OPTIONS

-r, \--remote=[*user*@]*host*:*path*
:   Initialize not only the local repository, but also the
    remote repository given by *user*, *host* and *path*. *path* may be
    omitted if you intend to backup to the default path on the remote 
    server. The connection to the remote server is made with SSH.
    SSH settings can be handled with an appropriate Host entry in 
    ~/.ssh/config.

# EXAMPLES
    bup init
    

# SEE ALSO

`bup-fsck`(1), `ssh_config`(5)

# BUP

Part of the `bup`(1) suite.