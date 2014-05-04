% bup-index(1) Bup %BUP_VERSION%
% Avery Pennarun <apenwarr@gmail.com>
% %BUP_DATE%

# NAME

bup-index - print and/or update the bup filesystem index

# SYNOPSIS

bup index \<-p|-m|-s|-u\> [-H] [-l] [-x] [\--fake-valid] [\--no-check-device]
[\--fake-invalid] [\--check] [\--clear] [\--regraft] [-f *indexfile*] 
[\--exclude *path*] [\--exclude-from *filename*] [\--exclude-rx *pattern*]
[\--exclude-rx-from *filename*] [-v] \<filenames...\>

# DESCRIPTION

`bup index` prints and/or updates the bup filesystem index,
which is a cache of the filenames, attributes, and sha-1
hashes of each file and directory in the filesystem.  The
bup index is similar in function to the `git`(1) index, and
can be found in `$BUP_DIR/bupindex`.

Creating a backup in bup consists of two steps: updating
the index with `bup index`, then actually backing up the
files (or a subset of the files) with `bup save`.  The
separation exists for these reasons:

1. There is more than one way to generate a list of files
that need to be backed up.  For example, you might want to
use `inotify`(7) or `dnotify`(7).

2. Even if you back up files to multiple destinations (for
added redundancy), the file names, attributes, and hashes
will be the same each time.  Thus, you can save the trouble
of repeatedly re-generating the list of files for each
backup set.

3. You may want to use the data tracked by bup index for
other purposes (such as speeding up other programs that
need the same information).

# NOTES

bup makes accommodations for the expected "worst-case" filesystem
timestamp resolution -- currently one second; examples include VFAT,
ext2, ext3, small ext4, etc.  Since bup cannot know the filesystem
timestamp resolution, and could be traversing multiple filesystems
during any given run, it always assumes that the resolution may be no
better than one second.

As a practical matter, this means that index updates are a bit
imprecise, and so `bup save` may occasionally record filesystem
changes that you didn't expect.  That's because, during an index
update, if bup encounters a path whose actual timestamps are more
recent than one second before the update started, bup will set the
index timestamps for that path (mtime and ctime) to exactly one second
before the run, -- effectively capping those values.

This ensures that no subsequent changes to those paths can result in
timestamps that are identical to those in the index.  If that were
possible, bup could overlook the modifications.

You can see the effect of this behavior in this example (assume that
less than one second elapses between the initial file creation and
first index run):

    $ touch src/1 src/2
    # A "sleep 1" here would avoid the unexpected save.
    $ bup index src
    $ bup save -n src src  # Saves 1 and 2.
    $ date > src/1
    $ bup index src
    $ date > src/2         # Not indexed.
    $ bup save -n src src  # But src/2 is saved anyway.

Strictly speaking, bup should not notice the change to src/2, but it
does, due to the accommodations described above.

# MODES

-u, \--update
:   recursively update the index for the given filenames and
    their descendants.  One or more filenames must be
    given.  If no mode option is given, this is the
    default.

-p, \--print
:   print the contents of the index.  If filenames are
    given, shows the given entries and their descendants. 
    If no filenames are given, shows the entries starting
    at the current working directory (.).
    
-m, \--modified
:   prints only files which are marked as modified (ie.
    changed since the most recent backup) in the index. 
    Implies `-p`.

-s, \--status
:   prepend a status code (A, M, D, or space) before each
    filename.  Implies `-p`.  The codes mean, respectively,
    that a file is marked in the index as added, modified,
    deleted, or unchanged since the last backup.

\--regraft
:	change the real path of files marked as modified to match
	options supplied by the --graft parameter. This option can
	be used to quickly update index locations when files may be
	marked as modified from one filesystem, but uploaded from
	another (i.e. a snapshotted volume).
	
	Example:
	
	Index a home directory:
		
		$ bup index -um --graft /home/user=/ /home/user
		
	Regraft the home directory to the real location it will be 
	saved from:
		
		$ bup index --regraft --graft /home/user/snapshot/today=/
		
	Then save:
		$ bup save -N home /  

# OPTIONS

\--graft *oldpath*=*newpath*
:   treat files indexed under *oldpath* as though they were
    really under *newpath* in the index. This is useful for
    snapshot backups where the path-prefix of a directory
    tree may change from backup to backup to prevent bup
    rereading file contents everytime. If *oldpath* is given
    as a relative path, it will be evaluated against the
    current working directory. *newpath* is always treated
    as an absolute path.
    
    Note: --graft will cause `bup index` to treat any file
    under *oldpath* which maps to an existing entry in the
    index under *newpath* as being the same file, and will
    only compare metadata to determine if it should be backed
    up again. In the unlikely event that two files have
    identical metadata but dissimilar content, using --graft
    will cause `bup index` to miss the change. 
    
    Usually this option should be used with --no-check-device to 
    avoid spurious update detection (since a mounted snapshot
    will frequently have a different device id to the normal
    filesystem).
    
    Example:
    
    Backup a home directory:
    
        $ bup index -um --graft /home/user=/ /home/user
        $ bup save -n homedirectory /

    Relocate the home directory:
    
        $ mv /home/user /home/user2

    Only backup files which have actually changed since the last
    backup:
    
        $ bup index -um --graft /home/different_user=/ /home/user2
        $ bup save -n homedirectory /

-H, \--hash
:   for each file printed, prepend the most recently
    recorded hash code.  The hash code is normally
    generated by `bup save`.  For objects which have not yet
    been backed up, the hash code will be
    0000000000000000000000000000000000000000.  Note that
    the hash code is printed even if the file is known to
    be modified or deleted in the index (ie. the file on
    the filesystem no longer matches the recorded hash). 
    If this is a problem for you, use `--status`.
    
-l, \--long
:   print more information about each file, in a similar
    format to the `-l` option to `ls`(1).

-x, \--xdev, \--one-file-system
:   don't cross filesystem boundaries when recursing through the
    filesystem -- though as with tar and rsync, the mount points
    themselves will still be indexed.  Only applicable if you're using
    `-u`.
    
\--fake-valid
:   mark specified filenames as up-to-date even if they
    aren't.  This can be useful for testing, or to avoid
    unnecessarily backing up files that you know are
    boring.
    
\--fake-invalid
:   mark specified filenames as not up-to-date, forcing the
    next "bup save" run to re-check their contents.
    
\--check
:   carefully check index file integrity before and after
    updating.  Mostly useful for automated tests.

\--clear
:   clear the default index.

-f, \--indexfile=*indexfile*
:   use a different index filename instead of
    `$BUP_DIR/bupindex`.

\--exclude=*path*
:   exclude *path* from the backup (may be repeated).

\--exclude-from=*filename*
:   read --exclude paths from *filename*, one path per-line (may be
    repeated).

\--exclude-rx=*pattern*
:   exclude any path matching *pattern*, which must be a Python regular
    expression (http://docs.python.org/library/re.html).  The pattern
    will be compared against the full path, without anchoring, so
    "x/y" will match "ox/yard" or "box/yards".  To exclude the
    contents of /tmp, but not the directory itself, use
    "^/tmp/.". (may be repeated)

    Examples:

      * '/foo$' - exclude any file named foo
      * '/foo/$' - exclude any directory named foo
      * '/foo/.' - exclude the content of any directory named foo
      * '^/tmp/.' - exclude root-level /tmp's content, but not /tmp itself

\--exclude-rx-from=*filename*
:   read --exclude-rx patterns from *filename*, one pattern per-line
    (may be repeated).

\--no-check-device
:   don't mark a an entry invalid if the device number (stat(2)
    st_dev) changes.  This can be useful when indexing remote,
    automounted, or (LVM) snapshot filesystems.

-v, \--verbose
:   increase log output during update (can be used more
    than once).  With one `-v`, print each directory as it
    is updated; with two `-v`, print each file too.


# EXAMPLES
    bup index -vux /etc /var /usr
    

# SEE ALSO

`bup-save`(1), `bup-drecurse`(1), `bup-on`(1)

# BUP

Part of the `bup`(1) suite.
