
from pyld import jsonld
from pyld.jsonld import set_document_loader
import os
import sys
import json
import time
import requests
import datetime


source = "../output_lux_minimal"
dest = "../output_lux_minimal_ml"

proc = jsonld.JsonLdProcessor()
docCache = {}
def fetch(url):
	print("Fetching context") 
	resp = requests.get(url)
	return resp.json()

def load_document_and_cache(url, *args, **kwargs):
    if url in docCache:
            return docCache[url]
    doc = {"expires": None, "contextUrl": None, "documentUrl": None, "document": ""}
    data = fetch(url)
    doc["document"] = data
    doc["expires"] = datetime.datetime.now() + datetime.timedelta(minutes=1440)
    docCache[url] = doc
    return doc

set_document_loader(load_document_and_cache)
options = {'format': 'application/n-quads'}

files = os.listdir(source)
files.sort()
fx = 0
for f in files:
	if f.endswith('.json'):
		fh = open(os.path.join(source, f))
		data = fh.read()
		fh.close()
		fx += 1
		data = data.replace('http://lux.yale.edu/supertype/Rocks and Minerals', 'http://lux.yale.edu/supertype/Rocks_and_Minerals')
		try:
			js = json.loads(data)
			src = {'@id': js['id'], '@graph':js}
			triples = proc.to_rdf(src, options)
		except:
			print(f"Bad JSON-LD in {f}")
			raise
		js['triples'] = []
		tlines = triples.split('\n')
		for tl in tlines:
			if tl:
				# first trash <graph> . from end
				tl = tl.replace(f" <{js['id']}> .", '')
				(s,p,o) = tl.split(' ', 2)
				t= {}
				s = s.strip()				
				if s[0] == "<" and s[-1] == ">":
					t['subject'] = s[1:-1]
				elif s.startswith("_:"):
					t['subject'] = f"_:r{fx}_{s[2:]}"
				else:
					print(f"unknown subject format: {s}")
				p = p.strip()
				if p[0] == "<" and p[-1] == ">":
					t['predicate'] = p[1:-1]
				else:
					print(f"unknown predicate format: {p}")
				o = o.strip()
				if o[0] == '"' and o[-1] == '"':
					t['object'] = {'value': o[1:-1], 'datatype': 'xs:string'}
				elif o[0] == "<" and o[-1] == ">":
					t['object'] = o[1:-1]					
				elif o.startswith('_:'):
					t['object'] = f"_:r{fx}_{o[2:]}"
				elif '^^' in o:
					value, datatype = o.split('^^')
					# chomp ""s and <>s
					t['object'] = {'value': value[1:-1], 'datatype': datatype[1:-1]} 
				else:
					print(f"Unknown object format: {o}")
				js['triples'].append({'triple': t})
		jstr = json.dumps(js)
		oh = open(os.path.join(dest, f), 'w')
		oh.write(jstr)
		oh.close()
