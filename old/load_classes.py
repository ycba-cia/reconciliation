from lmdb_utils import LMDB
from identifiers import DB, map_uuid
import os, json

# Trust ULAN, then Wikidata ... if can even figure it out, then local guess
# None of which are great


# Load NamesDB from authority

NAMEDB = LMDB('ycba_name_db', open=True)
#place:{name} = uuid
##actor:{name} = uuid


# Load TGN Names for alignment
tgn = "data/tgn"
files = os.listdir(tgn)
for f in files:
	fn = os.path.join(tgn, f)
	uu = map_uuid("tgn", f[:-5])
	info = DB[uu]
	fh = open(fn)
	js = json.load(fh)
	fh.close()
	if type(js) == list:
		continue
	# Get our names
	names = js['identified_by']
	good_names = []
	for n in names:
		if 'content' in n and n['content'] and n['type'] == 'Name':		
			good_names.append(n)

	for n in good_names:
			name = n['content']
			nn = f"place:{name}"
			if nn in NAMEDB and NAMEDB[nn] != uu:
				print(f"Found collision: {nn} is {NAMEDB[nn]} but want to set to {uu} from {f}")
				NAMEDB[nn] = uu
				NAMEDB[uu] = nn
			elif nn in NAMEDB:
				# Already set to this place
				continue
			else:
				NAMEDB[nn] = uu
				NAMEDB[uu] = nn
NAMEDB.commit()


if False:
	ulan = "data/ulan"
	files = os.listdir(ulan)
	for f in files:
		fn = os.path.join(ulan, f)
		uu = map_uuid("ulan", f[:-5])
		info = DB[uu]

		fh = open(fn)
		js = json.load(fh)
		fh.close()
		if type(js) == list:
			continue
		typ = js['type']
		if type(typ) == list:
			# ...
			continue
		info['class'] = typ	
		DB[uu] = info
		print(f"{uu} --> {typ}")
	DB.commit()

if False:
	wiki = 'data/wikidata'
	c = DB.cursor(prefix="wikidata")
	for (k,v) in c:
		info = DB[v]
		if 'ulan' in info and not info['ulan']:
			# Try and work out from DBPedia
			wdid = info['wikidata']
			fn = f"data/wikidata/{wdid}.json"
			fh = open(fn)
			js = json.load(fh)
			fh.close()

			redirect = False
			# Find the right entry
			for e in js['@graph']:
				if '@id' in e and e['@id'] == f"wd:{wdid}" and 'label' in e:
					break
				elif '@id' in e and e['@id'] == f"wd:{wdid}" and 'sameAs' in e:
					# Redirected
					new_id = e['sameAs']
					print(f"Redirected to {new_id}")
					redirect = True


			if 'P31' in e and (e['P31'] == "wd:Q5" or "wd:Q5" in e['P31']):
				typ = "Person"
			elif 'P31' in e and (e['P31'] in ["wd:Q43229", "wd:Q8436", "Q4830453"] or "wd:Q43229" in e['P31'] or "wd:Q8436" in e['P31'] or "wd:Q4830453" in e['P31']):
				# 43229 == Organization
				# 8436 == Family
				# 4830453 == Business
				typ = "Group"
			elif 'P21' in e or 'P569' in e or 'P19' in e or 'P570' in e or 'P20' in e or 'P26' in e or 'P40' in e:
				typ = "Person"
				print(f"Predict person based on attributes but, but P31 is {e.get('P31', None)}")
			elif 'P2124' in e or 'P488' in e or 'P571' in e or 'P576' in e or 'P159' in e:
				typ = "Group"
				print(f"Predict Group based on attributes")
			else:
				typ = "Group"
				print(f"Predict Group for {k} lacking any other information")

			if not redirect and 'class' in info and not typ == info['class']:
				print(f"ULAN says {info['class']} ; wikidata predicts {typ}")
			if not redirect and not 'class' in info:
				info['class'] = typ
				DB[v] = info
	DB.commit()
