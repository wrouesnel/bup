#!/usr/bin/env bash
. ./wvtest-bup.sh || exit $?

set -o pipefail

if [ $(t/root-status) != root ]; then
    WVSTART 'not root: skipping tests'
    exit 0 # FIXME: add WVSKIP.
fi

if ! modprobe loop; then
    WVSTART 'unable to load loopback module; skipping tests' 1>&2
    exit 0
fi

# These tests are only likely to work under Linux for now
# (patches welcome).
if ! [[ $(uname) =~ Linux ]]; then
    WVSTART 'not Linux: skipping tests'
    exit 0 # FIXME: add WVSKIP.
fi

top="$(WVPASS pwd)" || exit $?
tmpdir="$(WVPASS wvmktempdir)" || exit $?

export BUP_DIR="$tmpdir/bup"
export GIT_DIR="$tmpdir/bup"

bup() { "$top/bup" "$@"; }

WVPASS bup init
WVPASS pushd "$tmpdir"

WVSTART 'drecurse'

WVPASS dd if=/dev/zero of=testfs.img bs=1M count=32
WVPASS mkfs -F testfs.img # Don't care what type.
WVPASS mkdir -p src/mnt/{a,b,c}
WVPASS mount -o loop testfs.img src/mnt
WVPASS mkdir -p src/mnt/x
WVPASS touch src/1 src/mnt/2 src/mnt/x/3

WVPASSEQ "$(bup drecurse src | grep -vF lost+found)" "src/mnt/x/3
src/mnt/x/
src/mnt/2
src/mnt/
src/1
src/"

WVPASSEQ "$(bup drecurse -x src)" "src/mnt/
src/1
src/"

WVSTART 'index/save/restore'

WVPASS bup index src
WVPASS bup save -n src src
WVPASS mkdir src-restore
WVPASS bup restore -C src-restore "/src/latest$(pwd)/"
WVPASS test -d src-restore/src
WVPASS "$top/t/compare-trees" -c src/ src-restore/src/

WVPASS rm -r "$BUP_DIR" src-restore
WVPASS bup init
WVPASS bup index -x src
WVPASS bup save -n src src
WVPASS mkdir src-restore
WVPASS bup restore -C src-restore "/src/latest$(pwd)/"
WVPASS test -d src-restore/src
WVPASSEQ "$(cd src-restore/src && find . -not -name lost+found | sort)" ".
./1
./mnt"

# Test that --xdev shadowing detection works correctly
WVSTART 'index -x [shadow detection]'
WVPASS rm -r "$BUP_DIR" src-restore
WVPASS bup init
WVPASS bup index -x src src/mnt
WVPASS bup save -n src src
WVPASS mkdir src-restore
WVPASS bup restore -C src-restore "/src/latest$(pwd)/"
WVPASSEQ "$(bup drecurse src-restore | grep -vF lost+found)" "src-restore/src/mnt/x/3
src-restore/src/mnt/x/
src-restore/src/mnt/2
src-restore/src/mnt/
src-restore/src/1
src-restore/src/
src-restore/"

WVPASS popd
WVPASS umount "$tmpdir/src/mnt"
WVPASS rm -r "$tmpdir"
