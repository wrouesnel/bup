import math, tempfile, subprocess
from wvtest import *
import bup._helpers as _helpers
from bup import xstat

bup_tmp = os.path.realpath('../../../t/tmp')

@wvtest
def test_fstime():
    WVPASSEQ(xstat.timespec_to_nsecs((0, 0)), 0)
    WVPASSEQ(xstat.timespec_to_nsecs((1, 0)), 10**9)
    WVPASSEQ(xstat.timespec_to_nsecs((0, 10**9 / 2)), 500000000)
    WVPASSEQ(xstat.timespec_to_nsecs((1, 10**9 / 2)), 1500000000)
    WVPASSEQ(xstat.timespec_to_nsecs((-1, 0)), -10**9)
    WVPASSEQ(xstat.timespec_to_nsecs((-1, 10**9 / 2)), -500000000)
    WVPASSEQ(xstat.timespec_to_nsecs((-2, 10**9 / 2)), -1500000000)
    WVPASSEQ(xstat.timespec_to_nsecs((0, -1)), -1)
    WVPASSEQ(type(xstat.timespec_to_nsecs((2, 22222222))), type(0))
    WVPASSEQ(type(xstat.timespec_to_nsecs((-2, 22222222))), type(0))

    WVPASSEQ(xstat.nsecs_to_timespec(0), (0, 0))
    WVPASSEQ(xstat.nsecs_to_timespec(10**9), (1, 0))
    WVPASSEQ(xstat.nsecs_to_timespec(500000000), (0, 10**9 / 2))
    WVPASSEQ(xstat.nsecs_to_timespec(1500000000), (1, 10**9 / 2))
    WVPASSEQ(xstat.nsecs_to_timespec(-10**9), (-1, 0))
    WVPASSEQ(xstat.nsecs_to_timespec(-500000000), (-1, 10**9 / 2))
    WVPASSEQ(xstat.nsecs_to_timespec(-1500000000), (-2, 10**9 / 2))
    x = xstat.nsecs_to_timespec(1977777778)
    WVPASSEQ(type(x[0]), type(0))
    WVPASSEQ(type(x[1]), type(0))
    x = xstat.nsecs_to_timespec(-1977777778)
    WVPASSEQ(type(x[0]), type(0))
    WVPASSEQ(type(x[1]), type(0))

    WVPASSEQ(xstat.nsecs_to_timeval(0), (0, 0))
    WVPASSEQ(xstat.nsecs_to_timeval(10**9), (1, 0))
    WVPASSEQ(xstat.nsecs_to_timeval(500000000), (0, (10**9 / 2) / 1000))
    WVPASSEQ(xstat.nsecs_to_timeval(1500000000), (1, (10**9 / 2) / 1000))
    WVPASSEQ(xstat.nsecs_to_timeval(-10**9), (-1, 0))
    WVPASSEQ(xstat.nsecs_to_timeval(-500000000), (-1, (10**9 / 2) / 1000))
    WVPASSEQ(xstat.nsecs_to_timeval(-1500000000), (-2, (10**9 / 2) / 1000))
    x = xstat.nsecs_to_timeval(1977777778)
    WVPASSEQ(type(x[0]), type(0))
    WVPASSEQ(type(x[1]), type(0))
    x = xstat.nsecs_to_timeval(-1977777778)
    WVPASSEQ(type(x[0]), type(0))
    WVPASSEQ(type(x[1]), type(0))

    WVPASSEQ(xstat.fstime_floor_secs(0), 0)
    WVPASSEQ(xstat.fstime_floor_secs(10**9 / 2), 0)
    WVPASSEQ(xstat.fstime_floor_secs(10**9), 1)
    WVPASSEQ(xstat.fstime_floor_secs(-10**9 / 2), -1)
    WVPASSEQ(xstat.fstime_floor_secs(-10**9), -1)
    WVPASSEQ(type(xstat.fstime_floor_secs(10**9 / 2)), type(0))
    WVPASSEQ(type(xstat.fstime_floor_secs(-10**9 / 2)), type(0))


@wvtest
def test_bup_utimensat():
    initial_failures = wvfailure_count()
    if not xstat._bup_utimensat:
        return
    tmpdir = tempfile.mkdtemp(dir=bup_tmp, prefix='bup-txstat-')
    path = tmpdir + '/foo'
    open(path, 'w').close()
    frac_ts = (0, 10**9 / 2)
    xstat._bup_utimensat(_helpers.AT_FDCWD, path, (frac_ts, frac_ts), 0)
    st = _helpers.stat(path)
    atime_ts = st[8]
    mtime_ts = st[9]
    WVPASSEQ(atime_ts[0], 0)
    WVPASS(atime_ts[1] == 0 or atime_ts[1] == frac_ts[1])
    WVPASSEQ(mtime_ts[0], 0)
    WVPASS(mtime_ts[1] == 0 or mtime_ts[1] == frac_ts[1])
    if wvfailure_count() == initial_failures:
        subprocess.call(['rm', '-rf', tmpdir])


@wvtest
def test_bup_utimes():
    initial_failures = wvfailure_count()
    if not xstat._bup_utimes:
        return
    tmpdir = tempfile.mkdtemp(dir=bup_tmp, prefix='bup-txstat-')
    path = tmpdir + '/foo'
    open(path, 'w').close()
    frac_ts = (0, 10**6 / 2)
    xstat._bup_utimes(path, (frac_ts, frac_ts))
    st = _helpers.stat(path)
    atime_ts = st[8]
    mtime_ts = st[9]
    WVPASSEQ(atime_ts[0], 0)
    WVPASS(atime_ts[1] == 0 or atime_ts[1] == frac_ts[1] * 1000)
    WVPASSEQ(mtime_ts[0], 0)
    WVPASS(mtime_ts[1] == 0 or mtime_ts[1] == frac_ts[1] * 1000)
    if wvfailure_count() == initial_failures:
        subprocess.call(['rm', '-rf', tmpdir])


@wvtest
def test_bup_lutimes():
    initial_failures = wvfailure_count()
    if not xstat._bup_lutimes:
        return
    tmpdir = tempfile.mkdtemp(dir=bup_tmp, prefix='bup-txstat-')
    path = tmpdir + '/foo'
    open(path, 'w').close()
    frac_ts = (0, 10**6 / 2)
    xstat._bup_lutimes(path, (frac_ts, frac_ts))
    st = _helpers.stat(path)
    atime_ts = st[8]
    mtime_ts = st[9]
    WVPASSEQ(atime_ts[0], 0)
    WVPASS(atime_ts[1] == 0 or atime_ts[1] == frac_ts[1] * 1000)
    WVPASSEQ(mtime_ts[0], 0)
    WVPASS(mtime_ts[1] == 0 or mtime_ts[1] == frac_ts[1] * 1000)
    if wvfailure_count() == initial_failures:
        subprocess.call(['rm', '-rf', tmpdir])
