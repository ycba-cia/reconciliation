
from lmdb_utils import LMDB 
import json
import os


fh = open('../places/uuid_tgn_wd_geo_wof_map.json')
mapping = json.load(fh)
fh.close()

db = LMDB('identifier_db', open=True)

for (k, v) in mapping.items():
	val = {'tgn': v[0], 'wikidata': v[1], 'geonames': v[2], 'wof': v[3]}
	db[f"uuid:{k}"] = val
	for (k2, v2) in val.items():
		if v2:
			db[f"{k2}:{v2}"] = f"uuid:{k}"

db.commit()

