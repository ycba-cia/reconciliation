from identifiers import DB
from lmdb_utils import LMDB

NAMEDB = LMDB('ycba_name_db', open=True)

for x in DB:
    print(x)

for x in NAMEDB:
    print(x)


