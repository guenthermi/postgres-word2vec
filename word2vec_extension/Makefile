EXTENSION = word2vec
DATA = word2vec--0.0.1.sql  # script files to install
OBJS = $(patsubst %.c,%.o,$(wildcard *.c))
MODULE_big = $(EXTENSION)
PG_CPPFLAGS = -lpq -I/usr/include/postgresql -L$(pg_config --libdir) -lpq

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
