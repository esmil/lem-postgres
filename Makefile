CC         = gcc -std=gnu99
CFLAGS    ?= -O2 -pipe -Wall -Wextra
PKG_CONFIG = pkg-config
PG_CONFIG  = pg_config
STRIP      = strip
INSTALL    = install
UNAME      = uname

OS         = $(shell $(UNAME))
CFLAGS    += $(shell $(PKG_CONFIG) --cflags lem)
CFLAGS    += -I$(shell $(PG_CONFIG) --includedir)
LDFLAGS   += -L$(shell $(PG_CONFIG) --libdir)
LIBS       = -lpq
lmoddir    = $(shell $(PKG_CONFIG) --variable=INSTALL_LMOD lem)
cmoddir    = $(shell $(PKG_CONFIG) --variable=INSTALL_CMOD lem)

ifeq ($(OS),Darwin)
SHARED     = -dynamiclib -Wl,-undefined,dynamic_lookup
STRIP     += -x
else
SHARED     = -shared
endif

llibs = lem/postgres/queued.lua
clibs = lem/postgres.so

ifdef V
E=@\#
Q=
else
E=@echo
Q=@
endif

.PHONY: all debug strip install clean

all: CFLAGS += -DNDEBUG
all: $(clibs)

debug: $(clibs)

lem/postgres.so: lem/postgres.c
	$E '  CCLD  $@'
	$Q$(CC) $(CFLAGS) -fPIC -nostartfiles $(SHARED) $^ -o $@ $(LDFLAGS) $(LIBS)

%-strip: %
	$E '  STRIP $<'
	$Q$(STRIP) $<

strip: $(clibs:%=%-strip)

$(DESTDIR)$(lmoddir)/% $(DESTDIR)$(cmoddir)/%: %
	$E '  INSTALL $@'
	$Q$(INSTALL) -d $(dir $@)
	$Q$(INSTALL) -m 644 $< $@

install: \
	$(llibs:%=$(DESTDIR)$(lmoddir)/%) \
	$(clibs:%=$(DESTDIR)$(cmoddir)/%)

clean:
	rm -f $(clibs)
