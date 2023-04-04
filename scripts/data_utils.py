
import os
import json
import requests

DATA = "../data"

fetch_templates = {
	"aat": "https://data.getty.edu/vocab/{vocab}/{ident}",
	"ulan": "https://data.getty.edu/vocab/{vocab}/{ident}",
	"tgn": "https://data.getty.edu/vocab/{vocab}/{ident}",
	"_getty": "http://vocab.getty.edu/{vocab}/{ident}.jsonld",
	"wikidata": "https://wikidata.org/wiki/Special:EntityData/{ident}.jsonld?flavor=dump",
	"geonames": "https://sws.geonames.org/{ident}/about.rdf", # XML
	"wof": "https://data.whosonfirst.org/{ident[:3]}/{ident[3:6]}/{ident[6:]}/{ident}.geojson",
	"fast": "http://id.worldcat.org/fast/{ident}.rdf.xml", # XML
	"viaf": "https://viaf.org/viaf/{ident}/rdf.xml", # XML
	"lcnaf": "http://id.loc.gov/authorities/names/{ident}.skos.json",
	"lcsh": "http://id.loc.gov/authorities/subjects/{ident}.skos.json",
	"glotto": "https://glottolog.org/resource/languoid/id/{ident}.json" # not JSON-LD but rdf doesn't have links to others
}

entity_templates = {
	"aat": "http://vocab.getty.edu/aat/{ident}",	
	"tgn": "http://vocab.getty.edu/tgn/{ident}",	
	"ulan": "http://vocab.getty.edu/ulan/{ident}",	
	"wikidata": "https://www.wikidata.org/entity/{ident}",
	"lcnaf": "http://id.loc.gov/authorities/names/{ident}",
	"lcsh": "http://id.loc.gov/authorities/subjects/{ident}",
	"geonames": "https://sws.geonames.org/{ident}",
	"viaf": "https://viaf.org/viaf/{ident}",
	"fast": "http://id.worldcat.org/fast/{ident}",
	"glotto": "https://glottolog.org/resource/languoid/id/{ident}",
	"ycba": "http://collection.britishart.yale.edu/id/{ident}",
	"hponline": "https://www.historyofparliamentonline.org/{ident}",
	"gbif": "https://www.gbif.org/species/{ident}"
}

prefix_by_uri = {}
for (k,v) in entity_templates.items():
	v = v.replace('{ident}', '')
	prefix_by_uri[v] = k


def validate_getty(ident):
	if ident.startswith('http://vocab.getty.edu') or ident.startswith('https://data.getty.edu'):
		rsl = ident.rfind('/')
		ident = ident[rsl+1:]
	if ident.isdigit():
		return ident
	else:
		return None

def validate_aat(ident):
	ident = validate_getty(ident)
	if ident and len(ident) == 9 and ident[0] == "3":
		return ident
	else:
		return None

def validate_ulan(ident):
	ident = validate_getty(ident)
	if ident and len(ident) == 9 and ident[0] == "5":
		return ident
	else:
		return None

def validate_wikidata(ident):
	if "www.wikidata.org" in ident:
		rsl = ident.rfind('/')
		ident = ident[rsl+1:]
	if ident.startswith('wd:'):
		ident = ident.replace('wd:', '')
	if ident and ident[0] == "Q" and ident[1:].isdigit():
		return ident
	else:
		return None

def validate_lcnaf(ident):
	ident = ident.replace('.html', '')
	if ident and ident[0] == "n" and (ident[1].isdigit() or ident[1] in ['b', 'o', 'n','r']) and ident[2:].isdigit():
		return ident
	else:
		return None

validate_fns = {
	"aat": validate_aat,
	"tgn": validate_getty,
	"ulan": validate_ulan,
	"wikidata": validate_wikidata,
	"lcnaf": validate_lcnaf
}

def ff(templ, ident, vocab, format):
	return eval(f'f"""{templ}"""')

def fetch(vocab, ident, format='json', false_if_have=False):

	validator = validate_fns.get(vocab, None)

	if validator:
		ident = validator(ident)
		if not ident:
			# broken
			print(f"Broken {vocab} identifier {ident}")
			return None

	fn = os.path.join(DATA, vocab, f"{ident}.{format}")
	if not os.path.exists(fn):
		templ = fetch_templates.get(vocab, None)
		if not templ:
			raise ValueError(f"Unknown vocabulary {vocab}")
		try:
			uri = templ.format(vocab=vocab, ident=ident, format=format)
		except:
			uri = ff(templ, ident, vocab, format)

		resp = requests.get(uri)
		if resp.status_code == 200:
			fh = open(fn, 'w')
			fh.write(resp.text)
			fh.close()
			if format == "json":
				val = resp.json()
			else:
				val = resp.text
		else:
			# Failed to retrieve
			if vocab in ['ulan','aat','tgn']:
				# Try base vocab -- probably being redirected
				uri = fetch_templates['_getty'].format(vocab=vocab, ident=ident, format=format)
				resp = requests.get(uri)
				if resp.status_code == 200:
					fh = open(fn, 'w')
					fh.write(resp.text)
					fh.close()
					return resp.json()
			print(f"Failed to retrieve {vocab}:{ident} from {uri}")
			return None
	elif false_if_have:
		return False
	else:
		fh = open(fn)
		val = fh.read()
		fh.close()
		if format == "json":
			val = json.loads(val)
	return val

def recurse_fetch(vocab, data):
	# could be an ident or the full response if recursing
	fetched = []
	if type(data) == str:
		ident = data
		data = fetch(vocab, data, false_if_have=True)
		if not data:
			return []
		else:
			fetched.append(ident)

	parts = []
	# This assumes JSON with skos / linked art style keys
	# And that we stick within a single vocab (tgn->tgn, aat->aat)
	if 'part_of' in data:
		parts = data['part_of']
	elif 'broader' in data:
		parts = data['broader']
	else:
		return fetched
	if type(parts) != list:
		parts = [parts]
	for p in parts:
		if type(p) != dict:
			p = {'id': p}
		resp = fetch(vocab, p['id'], false_if_have=True)
		if resp:
			print(f"  recursing --> {p['id']}")
			fetched.append(p['id'])
			rfetched = recurse_fetch(vocab, resp)
			fetched.extend(rfetched)
	return fetched

def get_wikidata_entity(js, wdid):
	for e in js['@graph']:
		if '@id' in e and e['@id'] == f"wd:{wdid}" and 'label' in e:
			return e
	return {}


