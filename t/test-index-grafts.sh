#!/usr/bin/env bash
. wvtest.sh
. wvtest-bup.sh
. t/lib.sh

set -o pipefail

top="$(WVPASS /bin/pwd)" || exit $?
tmpdir="$(WVPASS wvmktempdir)" || exit $?
export BUP_DIR="$tmpdir/bup"

bup() { "$top/bup" "$@"; }

WVPASS bup init
WVPASS pushd "$tmpdir"

WVSTART "index --graft"
WVPASS bup index --graft $top/t=/ $top/t/sampledata
WVPASSEQ "$(bup index -p / | sed s:/\$:: | sed /^\$/d | sort)" \
	"$(find $top/t/sampledata -printf %p\\n | sed s:$top/t:: | sort)"

WVPASS bup save -n graft-test /
WVPASS bup restore -C "$tmpdir/graft-test" graft-test/latest/
WVPASS "$top/t/compare-trees" "$top/t/sampledata/" "$tmpdir/graft-test/sampledata"

WVSTART "index --regraft"
WVPASS bup index --regraft --fake-invalid --graft $tmpdir/graft-test/=/
WVPASSEQ "$(bup index -p / | sed s:/\$:: | sed /^\$/d | sort)" \
	"$(find $top/t/sampledata -printf %p\\n | sed s:$top/t:: | sort)"

WVPASS bup save -n regraft-test /
WVPASS bup restore -C "$tmpdir/regraft-test" regraft-test/latest/
WVPASS "$top/t/compare-trees" "$top/t/sampledata/" "$tmpdir/regraft-test/sampledata"

WVPASS popd
WVPASS force-delete "$tmpdir"
