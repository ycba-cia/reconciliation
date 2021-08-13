
from lmdb_utils import LMDB 
from data_utils import fetch_templates, entity_templates, validate_fns, fetch, recurse_fetch, prefix_by_uri
import uuid
from cromulent import model


class IdentifierDB(LMDB):

	def get(self, key):
		# urn:uuid:foo --> uuid:foo
		if key.startswith('urn:uuid:'):
			key = key.replace('urn:', '')
		val = LMDB.get(self, key)
		if type(val) == str and val.startswith('uuid:'):
			val = f"urn:{val}"
		return val

	def set(self, key, value):
		if key.startswith('urn:uuid:'):
			key = key.replace('urn:' , '')
		return LMDB.set(self, key, value)

DB = IdentifierDB('identifier_db', open=True)

def map_uuid_uri(uri, automap=True):
	if not uri:
		return None
	prefix = ""
	for (k,v) in prefix_by_uri.items():
		base = "/".join(k.split("/")[0:3])
		if uri.startswith(base):
			prefix = v
			if "aat" in uri:
				prefix = "aat"
			if "tgn" in uri:
				prefix = "tgn"
			if "ulan" in uri:
				prefix = "ulan"
			if "/names/" in uri:
				prefix = "lcnaf"
			if "subjects" in uri:
				prefix = "lcsh"
			# uri = uri.replace(k, "")
			uri = uri.split("/")[-1]
			break
	if prefix:						
		return map_uuid(prefix, uri, automap)
	else:
		return None

def map_uuid(vocab, ident, automap=True):
	if not vocab in entity_templates:
		raise ValueError(f"Unknown vocabulary {vocab}")
	if vocab in validate_fns:
		ident = validate_fns[vocab](ident)
		if not ident:
			raise ValueError(f"Badly formed identifier {ident} for vocab {vocab}")
	uu = DB[f"{vocab}:{ident}"]
	if uu:
		return uu
	elif not automap:
		return None
	else:
		# Set it, initialize with just this, and return
		uu = f"urn:uuid:{uuid.uuid4()}"
		DB[uu] = {vocab:ident}
		DB[f"{vocab}:{ident}"] = uu
		DB.commit()
		#removing fetch 8/13/2021
		#if vocab in fetch_templates:
			#fetch(vocab, ident)
		return uu

def fetch_and_map(vocab, ident):
	ids = recurse_fetch(vocab, ident)
	for i in ids:
		try:
			map_uuid(vocab, i)
		except:
			print(f"Failed to map for {vocab} : {i}")

def get_languages():
	c = DB.cursor(prefix="lang:")
	langs = {}
	for (k,v) in c:
		# This doesn't go through get() so isn't auto-fixed
		code = k.replace('lang:', '')
		langs[code] = f"urn:{v}"
	return langs

def rewrite_crom_ids(record):
	for p in record.list_my_props():
		if p == "id" and not record.id.startswith('urn:uuid:'):
			if not record.id:
				continue
			else:
				# See if we can map to a prefix
				uu = map_uuid_uri(record.id)
				if uu:
					record.id = uu
		elif p in ['type', '_label', 'value', 'content', 'defined_by', 'begin_of_the_begin', 'end_of_the_end', 'equivalent']:
			continue
		else:
			val = getattr(record, p)
			if isinstance(val, model.BaseResource):
				rewrite_crom_ids(val)                        
			elif type(val) == list:
				for it in val:
					if isinstance(it, model.BaseResource):
						rewrite_crom_ids(it)
