bup: It backs things up
=======================

This is my forked development repo for work on bup.

The main bup archive can be found at https://github.com/bup/bup

To submit code to bup, a mailing list workflow is used - the mailing
list is bup-list@googlegroups.com

#Current development branches:
##index-grafts
Support for highly graftable indexes to enable easy use of bup
with relocatable roots, or directory trees with subtle differences
(i.e. when backing up home directories across Windows and Linux)
	
The principle effect of this branch is to repurpose the metadata cache objects `path` field during indexing to store the real path of any given entry in the bupindex. The bupindex represents what the final bup archive will look like.

A grafted bup index can be produced as follows:
```bash
$ bup index --graft /var/www=/ /var/www
```

You could then use `bup save` like so:
```bash
$ bup save -N www /
```

This leads to the contents of `/var/www` being stored under a backup named `www` in the bup-archive.

The important thing about this feature is that it means bup will treat a relocated directory tree that is grafted to the same point as though it is the same tree, meaning it only rehashes files which have non-matching metadata.

So suppose you move `/var/www` to `/srv/mysite.com`. While bup will only upload changes between files, it will rehash the content of _all_ the files in the directory. With many thousands of files, or many gigabytes of files, this is very slow - slower then rsync in my experience.

But, with index-grafts we can just do the following:
```bash
$ bup index --graft /srv/mysite.com=/ /srv/mysite.com
$ bup save -N www /
```

bup iterates over the index as it is in the archive, and updates the the real file location of all objects. Only files which have changed according to metadata are rehashed.

The expense of this operation is a larger metadata cache since its now being used as key-value store. But this object is stored locally and never needs to be sent over the network, and stored metadata in bup is recalculated during file saves.

**IMPORTANT NOTE**: it is important to use the `--no-check-device` flag when the files are on a different filesystem or device (i.e. a snapshot drive) to the originals. Otherwise bup will mark all the files as invalid.

### --regraft
index-grafts also adds the `--regraft` option to bup-index. `--regraft` does not iterate over any directories on disk, and instead iterates through the bup index remapping the realpath of any entry which will be read by the next `bup save` run.

This is useful when `bup index` is accepting input from inotify, but the actual files are going to be copied from a snapshotted volume. The real path of files which will not be updated is left untouched since `bup save` will not need to read these files.

Example (accomplishing the same as the two runs with --graft above):
```bash
$ bup index --graft /var/www=/ /var/www
$ mv /var/www/* /srv/mysite.com/*
$ bup index --regraft --graft /srv/mysite.com=/
$ bup save -N www /
```

**NOTE**: --regraft respects the --no-check-device flag but generally shouldn't need it since the files it scans will be marked for upload anyway.