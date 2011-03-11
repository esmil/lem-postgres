CC         = gcc
CFLAGS    ?= -O2 -pipe -Wall -Wextra -Wno-variadic-macros -Wno-strict-aliasing
PKGCONFIG  = pkg-config
STRIP      = strip
INSTALL    = install

CFLAGS    += $(shell $(PKGCONFIG) --cflags lem)
LUA_PATH   = $(shell $(PKGCONFIG) --variable=path lem)
LUA_CPATH  = $(shell $(PKGCONFIG) --variable=cpath lem)

programs = postgres.so
scripts  = queued.lua

ifdef NDEBUG
CFLAGS += -DNDEBUG
endif

.PHONY: all strip install clean
.PRECIOUS: %.o

all: $(programs)

%.o: %.c
	@echo '  CC $@'
	@$(CC) $(CFLAGS) -fPIC -nostartfiles -c $< -o $@

postgres.so: CFLAGS += -I$(shell pg_config --includedir)
postgres.so: postgres.o
	@echo '  LD $@'
	@$(CC) -shared -L$(shell pg_config --libdir) -lpq $(LDFLAGS) $^ -o $@

%-strip: %
	@echo '  STRIP $<'
	@$(STRIP) $<

strip: $(programs:%=%-strip)

path-install:
	@echo "  INSTALL -d $(LUA_PATH)/lem/postgres"
	@$(INSTALL) -d $(DESTDIR)$(LUA_PATH)/lem/postgres

%.lua-install: %.lua path-install
	@echo "  INSTALL $<"
	@$(INSTALL) -m644 $< $(DESTDIR)$(LUA_PATH)/lem/postgres/$<

cpath-install:
	@echo "  INSTALL -d $(LUA_CPATH)/lem"
	@$(INSTALL) -d $(DESTDIR)$(LUA_CPATH)/lem

%.so-install: %.so cpath-install
	@echo "  INSTALL $<"
	@$(INSTALL) $< $(DESTDIR)$(LUA_CPATH)/lem/$<

install: $(programs:%=%-install) $(scripts:%=%-install)

clean:
	rm -f $(programs) *.o *.c~ *.h~
