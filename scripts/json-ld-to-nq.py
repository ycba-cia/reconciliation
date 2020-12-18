
from pyld import jsonld
from pyld.jsonld import set_document_loader
import os
import requests
import os
import sys
import json
import datetime
import pathlib

sources = ['data/ycba/linked_art', 'output']
output = 'nquads'

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

count = 0
nfiles = 0
prev_ct = 0

for src in sources:
	models = os.listdir(src)
	models.sort()
	for model in models:
		mpath = os.path.join(src, model)
		chunks = os.listdir(mpath)
		chunks.sort()
		for chunk in chunks:
			cpath = os.path.join(mpath, chunk)
			if len(chunk) != 2:
				print(f"Found not a chunk: {cpath}")
				continue
			files = os.listdir(cpath)
			files.sort()
			for f in files:
				if f.endswith('.json'):
					nfiles += 1
					fn = os.path.join(cpath, f)
					outdir = os.path.join(output, f[:2])
					outfn = os.path.join(outdir, f[:-5] + '.nquads')
					if os.path.exists(outfn):
						# Read from nquads
						with open(outfn) as fh:
							lines = fh.readlines()
							count += len(lines)
					else:
						with open(fn) as fh:
							try:
								js = json.load(fh)
							except:
								print(f"No JSON in {fn}")
								continue
							try:
								input = {'@id': js['id'], '@graph':js}
								triples = proc.to_rdf(input, options)
							except:
								print(f"Bad JSON-LD in {fn}")
								continue

							pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
							ofh = open(outfn, 'w')
							ofh.write(triples)
							ofh.close()
							qs = triples.split('\n')
							count += len(qs)
					if count > prev_ct + 100000:
						print(f"files: {nfiles} --> {count} quads ({count/nfiles} / file)")
						prev_ct += 100000