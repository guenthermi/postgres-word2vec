EXTENSION = freddy
DATA = freddy--0.0.1.sql  # script files to install
OBJS = $(patsubst %.c,%.o,$(wildcard dep/c_hashmap/*.c)) $(patsubst %.c,%.o,$(wildcard *.c))
MODULE_big = $(EXTENSION)
PG_CPPFLAGS = -lpq -I/usr/include/postgresql -Idep/c_hashmap -L$(pg_config --libdir) -lpq -fPIC

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

verbose:
	@echo $(OBJS)
