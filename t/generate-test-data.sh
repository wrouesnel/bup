#!/bin/sh
# Shell script to generate test data for make test, since git checkouts of
# symlinks are unreliable across multiple platforms (and in general). This
# script should work on every POSIX like platform that can support the Bourne
# shell (pretty much all of them).
#
# This script should be run from the  subdirectory of the bup source tree.
#
# Usage: generate-test-data.sh

# Enforce execution in the directory we're located in
cd $( cd "$( dirname "$0" )" && pwd )

# An stderr echo function
echoerr ()
{
    echo $@ 1>&2
}

# Helper to generate a largish ASCII-like test files in a sensible way
# Reads from /dev/urandom and squashes non-ascii to spaces in 72 byte lines.
make_testfile ()
{
    dd if=/dev/urandom bs=72 count=3000 2> /dev/null | \
    tr -cs 'a-zA-Z0-9' '[ *]' | \
    sed -e "s/.\{72\}/&\n/g" > "$1"
}

#testfile1 and testfile2
make_t_testfiles ()
{
    if [ ! -e testfile1 ]; then
        make_testfile testfile1
    fi
    
    if [ ! -e testfile2 ]; then
        make_testfile testfile2
    fi
}

# sampledata generator
make_t_sampledata ()
{
    # Don't needlessly regenerate sample data.
    if [ -d sampledata ]; then
        return
    fi
    
    if [ -e sampledata ]; then
        echoerr "File named sampledata already exists. Please remove before \
                attempting to generate sampledata."
        return
    fi

    echoerr "Generating $(pwd)/sampledata"
    
    # Top level directory
    mkdir sampledata
    ln -s /etc sampledata/etc
    ln -s sampledata/a sampledata/b
    ln -s sampledata/b sampledata/c
    cat > sampledata/x << EOF
$(date)
EOF
    cat > sampledata/y-2000 << EOF
this file should come *before* y/ in the sort order, because of that
trailing slash.
EOF
    
    # b2
    mkdir sampledata/b2
    touch sampledata/b2/foozy
    touch sampledata/b2/foozy2
    
    # y
    mkdir sampledata/y
    cat > sampledata/y/text << EOF
this is a text file.

watch me be texty!
EOF

    # Generate testfile1 from some bits of source files with ROT-13 applied
    # FIXME: maybe this should be /dev/urandom instead? bup does do a lot of
    # work with binaries.
    make_testfile sampledata/y/testfile1
}

make_t_testfiles
make_t_sampledata

exit 0
