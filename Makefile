EXTENSION = pg_dtw      # the extensions name
DATA = pg_dtw--1.0.sql  # script files to install
MODULES = pg_dtw        # our c module file to build

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
