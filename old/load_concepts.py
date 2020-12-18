
from lmdb_utils import LMDB 
import json
import os


fh = open('../concepts/aat_uuid_mapping.json')
mapping = json.load(fh)
fh.close()

db = LMDB('identifier_db', open=True)

for (k, v) in mapping.items():
	val = {'aat': k, 'wikidata': None, 'lcsh': None, 'fast': None}
	db[f"uuid:{v}"] = val
	db[f"aat:{k}"] = f"uuid:{v}"

db.commit()

