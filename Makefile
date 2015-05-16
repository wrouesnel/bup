
sampledata_rev := $(shell t/configure-sampledata --revision)
current_sampledata := t/sampledata/var/rev/v$(sampledata_rev)

OS:=$(shell uname | sed 's/[-_].*//')
CFLAGS := -Wall -O2 -Werror -Wno-unknown-pragmas $(PYINCLUDE) $(CFLAGS)
CFLAGS := -D_FILE_OFFSET_BITS=64 $(CFLAGS)
SOEXT:=.so

ifeq ($(OS),CYGWIN)
  SOEXT:=.dll
endif

ifdef TMPDIR
  test_tmp := $(TMPDIR)
else
  test_tmp := $(CURDIR)/t/tmp
endif

initial_setup := $(shell ./configure-version --update)
bup_deps := bup lib/bup/_version.py lib/bup/_helpers$(SOEXT) cmds

all: $(bup_deps) Documentation/all $(current_sampledata)

bup:
	ln -s main.py bup

Documentation/all: $(bup_deps)

$(current_sampledata):
	t/configure-sampledata --setup

INSTALL=install
PYTHON=python
PREFIX=/usr
MANDIR=$(DESTDIR)$(PREFIX)/share/man
DOCDIR=$(DESTDIR)$(PREFIX)/share/doc/bup
BINDIR=$(DESTDIR)$(PREFIX)/bin
LIBDIR=$(DESTDIR)$(PREFIX)/lib/bup
install: all
	$(INSTALL) -d $(MANDIR)/man1 $(DOCDIR) $(BINDIR) \
		$(LIBDIR)/bup $(LIBDIR)/cmd \
		$(LIBDIR)/web $(LIBDIR)/web/static
	[ ! -e Documentation/.docs-available ] || \
	  $(INSTALL) -m 0644 \
		Documentation/*.1 \
		$(MANDIR)/man1
	[ ! -e Documentation/.docs-available ] || \
	  $(INSTALL) -m 0644 \
		Documentation/*.html \
		$(DOCDIR)
	$(INSTALL) -pm 0755 bup $(BINDIR)
	$(INSTALL) -pm 0755 \
		cmd/bup-* \
		$(LIBDIR)/cmd
	$(INSTALL) -pm 0644 \
		lib/bup/*.py \
		$(LIBDIR)/bup
	$(INSTALL) -pm 0755 \
		lib/bup/*$(SOEXT) \
		$(LIBDIR)/bup
	$(INSTALL) -pm 0644 \
		lib/web/static/* \
		$(LIBDIR)/web/static/
	$(INSTALL) -pm 0644 \
		lib/web/*.html \
		$(LIBDIR)/web/
%/all:
	$(MAKE) -C $* all

%/clean:
	$(MAKE) -C $* clean

config/config.h: config/Makefile config/configure config/configure.inc \
		$(wildcard config/*.in)
	cd config && $(MAKE) config.h

lib/bup/_helpers$(SOEXT): \
		config/config.h \
		lib/bup/bupsplit.c lib/bup/_helpers.c lib/bup/csetup.py
	@rm -f $@
	cd lib/bup && \
	LDFLAGS="$(LDFLAGS)" CFLAGS="$(CFLAGS)" $(PYTHON) csetup.py build
	cp lib/bup/build/*/_helpers$(SOEXT) lib/bup/

lib/bup/_version.py:
	@echo "Something has gone wrong; $@ should already exist."
	@echo 'Check "./configure-version --update"'
	@false

t/tmp:
	mkdir t/tmp

runtests: runtests-python runtests-cmdline

runtests-python: all t/tmp
	TMPDIR="$(test_tmp)" $(PYTHON) wvtest.py t/t*.py lib/*/t/t*.py

<<<<<<< HEAD
cmdline_tests := \
  t/test-fuse.sh \
  t/test-drecurse.sh \
  t/test-cat-file.sh \
  t/test-compression.sh \
  t/test-fsck.sh \
  t/test-index-clear.sh \
  t/test-index-check-device.sh \
  t/test-ls.sh \
  t/test-meta.sh \
  t/test-on.sh \
  t/test-restore-map-owner.sh \
  t/test-restore-single-file.sh \
  t/test-rm-between-index-and-save.sh \
  t/test-sparse-files.sh \
  t/test-command-without-init-fails.sh \
  t/test-redundant-saves.sh \
  t/test-save-creates-no-unrefs.sh \
  t/test-save-restore-excludes.sh \
  t/test-save-strip-graft.sh \
  t/test-import-duplicity.sh \
  t/test-import-rdiff-backup.sh \
  t/test-xdev.sh \
  t/test.sh

# For parallel runs.
tmp-target-run-test%: all t/tmp
	TMPDIR="$(test_tmp)" t/test$*

runtests-cmdline: $(subst t/test,tmp-target-run-test,$(cmdline_tests))
=======
runtests-cmdline: all
	test -e t/tmp || mkdir t/tmp
	TMPDIR="$(test_tmp)" t/test-fuse.sh
	TMPDIR="$(test_tmp)" t/test-drecurse.sh
	TMPDIR="$(test_tmp)" t/test-cat-file.sh
	TMPDIR="$(test_tmp)" t/test-compression.sh
	TMPDIR="$(test_tmp)" t/test-fsck.sh
	TMPDIR="$(test_tmp)" t/test-index-clear.sh
	TMPDIR="$(test_tmp)" t/test-index-check-device.sh
	TMPDIR="$(test_tmp)" t/test-index-grafts.sh
	TMPDIR="$(test_tmp)" t/test-ls.sh
	TMPDIR="$(test_tmp)" t/test-meta.sh
	TMPDIR="$(test_tmp)" t/test-on.sh
	TMPDIR="$(test_tmp)" t/test-restore-map-owner.sh
	TMPDIR="$(test_tmp)" t/test-restore-single-file.sh
	TMPDIR="$(test_tmp)" t/test-rm-between-index-and-save.sh
	TMPDIR="$(test_tmp)" t/test-sparse-files.sh
	TMPDIR="$(test_tmp)" t/test-command-without-init-fails.sh
	TMPDIR="$(test_tmp)" t/test-redundant-saves.sh
	TMPDIR="$(test_tmp)" t/test-save-creates-no-unrefs.sh
	TMPDIR="$(test_tmp)" t/test-save-restore-excludes.sh
	TMPDIR="$(test_tmp)" t/test-save-strip-graft.sh
	TMPDIR="$(test_tmp)" t/test-import-rdiff-backup.sh
	TMPDIR="$(test_tmp)" t/test-xdev.sh
	TMPDIR="$(test_tmp)" t/test.sh
>>>>>>> Add --graft and --regraft support to bup-index and bup-save

stupid:
	PATH=/bin:/usr/bin $(MAKE) test

test: all
	./wvtestrun $(MAKE) PYTHON=$(PYTHON) runtests-python runtests-cmdline

check: test

cmds: \
    $(patsubst cmd/%-cmd.py,cmd/bup-%,$(wildcard cmd/*-cmd.py)) \
    $(patsubst cmd/%-cmd.sh,cmd/bup-%,$(wildcard cmd/*-cmd.sh))

cmd/bup-%: cmd/%-cmd.py
	rm -f $@
	ln -s $*-cmd.py $@

cmd/bup-%: cmd/%-cmd.sh
	rm -f $@
	ln -s $*-cmd.sh $@

# update the local 'man' and 'html' branches with pregenerated output files, for
# people who don't have pandoc (and maybe to aid in google searches or something)
export-docs: Documentation/all
	git update-ref refs/heads/man origin/man '' 2>/dev/null || true
	git update-ref refs/heads/html origin/html '' 2>/dev/null || true
	GIT_INDEX_FILE=gitindex.tmp; export GIT_INDEX_FILE; \
	rm -f $${GIT_INDEX_FILE} && \
	git add -f Documentation/*.1 && \
	git update-ref refs/heads/man \
		$$(echo "Autogenerated man pages for $$(git describe --always)" \
		    | git commit-tree $$(git write-tree --prefix=Documentation) \
				-p refs/heads/man) && \
	rm -f $${GIT_INDEX_FILE} && \
	git add -f Documentation/*.html && \
	git update-ref refs/heads/html \
		$$(echo "Autogenerated html pages for $$(git describe --always)" \
		    | git commit-tree $$(git write-tree --prefix=Documentation) \
				-p refs/heads/html)

# push the pregenerated doc files to origin/man and origin/html
push-docs: export-docs
	git push origin man html

# import pregenerated doc files from origin/man and origin/html, in case you
# don't have pandoc but still want to be able to install the docs.
import-docs: Documentation/clean
	git archive origin/html | (cd Documentation; tar -xvf -)
	git archive origin/man | (cd Documentation; tar -xvf -)

clean: Documentation/clean config/clean
	rm -f *.o lib/*/*.o *.so lib/*/*.so *.dll lib/*/*.dll *.exe \
		.*~ *~ */*~ lib/*/*~ lib/*/*/*~ \
		*.pyc */*.pyc lib/*/*.pyc lib/*/*/*.pyc \
		bup bup-* cmd/bup-* \
		randomgen memtest \
		testfs.img lib/bup/t/testfs.img
	if test -e t/mnt; then t/cleanup-mounts-under t/mnt; fi
	if test -e t/mnt; then rm -r t/mnt; fi
	if test -e t/tmp; then t/cleanup-mounts-under t/tmp; fi
        # FIXME: migrate these to t/mnt/
	if test -e lib/bup/t/testfs; \
	  then umount lib/bup/t/testfs || true; fi
	rm -rf *.tmp *.tmp.meta t/*.tmp lib/*/*/*.tmp build lib/bup/build lib/bup/t/testfs
	if test -e t/tmp; then t/force-delete t/tmp; fi
	./configure-version --clean
	t/configure-sampledata --clean
