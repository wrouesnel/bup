bup: It backs things up
=======================

This is my forked development repo for work on bup.

The main bup archive can be found at https://github.com/bup/bup

To submit code to bup, a mailing list workflow is used - the mailing
list is bup-list@googlegroups.com

Current development branches:
	* index-grafts
	Support for highly graftable indexes to enable easy use of bup
	with relocatable roots, or directory trees with subtle differences
	(i.e. when backing up home directories across Windows and Linux)

	* improved-ssh-operation
	Adding support for passing arbitrary commands to the SSH commandline
	in bup and improving the documentation. This should be feature-complete.

	* cygwin-compatibility
	This is an ongoing effort to make bup compatible with cygwin and thus
	support Windows.

Most branches get periodically rolled into cutting-edge, which is the branch I
use for development buildings.

