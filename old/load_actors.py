
from lmdb_utils import LMDB 
import json
import os


fh = open('../actors/uuid_ulan_wd_lc_viaf_map.json')
mapping = json.load(fh)
fh.close()

db = LMDB('identifier_db', open=True)

for (k, v) in mapping.items():
	val = {'ulan': v[0], 'wikidata': v[1], 'lcnaf': v[2], 'viaf': v[3]}
	db[f"uuid:{k}"] = val
	for (k2, v2) in val.items():
		if v2:
			db[f"{k2}:{v2}"] = f"uuid:{k}"

db.commit()

