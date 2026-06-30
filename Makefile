EXTENSION    = amqp
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
PG_CONFIG    = pg_config
# Require PostgreSQL 9.1+ (extension system). Rejects 8.x and 9.0.
PG_GE91      = $(shell $(PG_CONFIG) --version | grep -qE " 8\.| 9\.0" && echo no || echo yes)

ifeq ($(PG_GE91),yes)
DOCS         = $(wildcard doc/*.*)
#TESTS        = $(wildcard test/sql/*.sql)
#REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
#REGRESS_OPTS = --inputdir=test
MODULE_big   = $(patsubst src/%.c,%,$(wildcard src/*.c))
OBJS         = src/pg_amqp.o

# Link against the maintained rabbitmq-c client library (librabbitmq).
# On Ubuntu/Debian (the primary target) install "librabbitmq-dev"; on macOS
# "brew install rabbitmq-c".  TLS/SSL is handled inside librabbitmq, so we no
# longer link OpenSSL directly.  Use pkg-config when available and fall back to
# a plain -lrabbitmq otherwise.
PKG_CONFIG     ?= pkg-config
RABBITMQ_CFLAGS := $(shell $(PKG_CONFIG) --cflags librabbitmq 2>/dev/null)
RABBITMQ_LIBS   := $(shell $(PKG_CONFIG) --libs librabbitmq 2>/dev/null || echo -lrabbitmq)

PG_CPPFLAGS += $(RABBITMQ_CFLAGS)
SHLIB_LINK  += $(RABBITMQ_LIBS)


all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/tables/*.sql sql/functions/*.sql
	cat $^ > $@

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql
else
$(error Minimum version of PostgreSQL required is 9.1.0)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
