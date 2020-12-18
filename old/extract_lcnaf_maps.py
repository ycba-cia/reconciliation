import os
import json
from pyld import jsonld
from data_utils import prefix_by_uri, fetch
import requests


def uri_split(uri):
	prefix = ""
	for (k,v) in prefix_by_uri.items():
		if uri.startswith(k):
			prefix = v
			uri = uri.replace(k, "")
			break
	return (prefix, uri)

ctx = {"id": "@id", "type": "@type", "skos": "http://www.w3.org/2004/02/skos/core#", 
	"skos:exactMatch": {"@container": "@set"}, "skos:closeMatch": {"@container": "@set"}}


lcnaf_viaf = {}
lcnaf_ulan = {}
lcnaf_wikidata = {}
lcnaf_fast = {}


src = "data/lcnaf"
files = os.listdir(src)
for f in files:
	fn = os.path.join(src, f)
	lcid = f[:-5]
	fh = open(fn)
	js = json.load(fh)
	fh.close()

	for e in js:
		if e['@id'].endswith("/" + lcid):
			break
	js = jsonld.compact(e, ctx)
	exact = js.get('skos:exactMatch', [])
	close = js.get('skos:closeMatch', [])

	for e in exact:
		ei = e['id']
		if ei.startswith('http://viaf.org/viaf/sourceID/'):
			# resolve viaf relative reference
			r = requests.head(ei)
			if r.status_code == 301:
				viafid = r.headers['location']
				viafid = viafid.replace('http:', 'https:')
				(pref, uri) = uri_split(viafid)
				print(pref, uri)
				# fetch(pref, uri, format='xml')
				lcnaf_viaf[lcid] = uri
		else:
			print(f"Unhandled exact: {ei}")
	for c in close:
		ci = c['id']
		if ci.startswith('http://www.wikidata'):
			ci = ci.replace('http:', 'https:')
		(pref, uri) = uri_split(ci)
		print(pref, uri)
		if pref == "fast":
			# fetch(pref, uri, format='xml')
			lcnaf_fast[lcid] = uri
		else:				
			fetch(pref, uri)
			if pref == "ulan":
				lcnaf_ulan[lcid] = uri
			elif pref == "wikidata":
				lcnaf_wikidata[lcid] = uri

fh = open('mappings/lcnaf_viaf.json', 'w')
out = json.dumps(lcnaf_viaf)
fh.write(out)
fh.close()

fh = open('mappings/lcnaf_fast.json', 'w')
out = json.dumps(lcnaf_fast)
fh.write(out)
fh.close()

fh = open('mappings/lcnaf_ulan.json', 'w')
out = json.dumps(lcnaf_ulan)
fh.write(out)
fh.close()

fh = open('mappings/lcnaf_wikidata.json', 'w')
out = json.dumps(lcnaf_wikidata)
fh.write(out)
fh.close()
