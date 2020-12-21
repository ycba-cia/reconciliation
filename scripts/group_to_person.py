
import os
import json
import pathlib
from lxml import etree
from identifiers import DB
from data_utils import fetch, get_wikidata_entity

nss = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'}
src = '../data/ycba/linked_art/group'
dest = '../data/ycba/linked_art/person'

def group_to_person(js, fn):
	# easiest to do this in the JSON as otherwise it means recreating objects
	js['type'] = 'Person'
	for eq in js['equivalent']:
		eq['type'] = 'Person'
	if 'formed_by' in js:
		js['born'] = js['formed_by']
		del js['formed_by']
		js['born']['type'] = "Birth"
	if 'dissolved_by' in js:
		js['died'] = js['dissolved_by']
		del js['dissolved_by']
		js['died']['type'] = "Death"

	outstr = json.dumps(js, indent=2)

	fnd = os.path.join(dest, js['id'][9:11])
	pathlib.Path(fnd).mkdir(parents=True, exist_ok=True)
	outfn = os.path.join(fnd, f"{js['id'][9:]}.json")
	fh = open(outfn, 'w')
	fh.write(outstr)
	fh.close()
	# Update in the metametadata
	info = DB[js['id']]
	info['class'] = 'Person'
	DB.commit()
	# And remove the original
	os.remove(fn)

chunks = os.listdir(src)
chunks.sort()
for c in chunks:
	cfn = os.path.join(src, c)
	files = os.listdir(cfn)
	files.sort()
	for f in files:
		fn = os.path.join(cfn, f)
		uuid = f"urn:uuid:{f[:-5]}"
		fh = open(fn)
		js = json.load(fh)
		fh.close()

		# check equivs
		info = DB[uuid]

		if not 'ulan' in info:
			print(f"No ulan entry {info} ... {uuid} is not a person?")
			continue

		if info['ulan']:
			ujs = fetch('ulan', info['ulan'])
			if type(ujs) == dict and ujs['type'] == 'Person':
				group_to_person(js, fn)
				continue		
		if info['viaf']:
			vid = info['viaf']
			if vid.startswith('http'):
				sl = vid.rfind('/')
				vid = vid[sl+1:]
				info['viaf'] = vid
				DB[uuid] = info
				DB.commit()

			vxml = fetch('viaf', info['viaf'], format='xml')
			if vxml:
				dom = etree.XML(vxml)
				# Check if person / group
				# <rdf:RDF> <rdf:Description rdf:about="http://viaf.org/viaf/52588603">
				vurl = f"http://viaf.org/viaf/{vid}"
				vtypes = dom.xpath(f"/rdf:RDF/rdf:Description[@rdf:about='{vurl}']/rdf:type/@rdf:resource", namespaces=nss)
				if 'http://schema.org/Person' in vtypes:
					group_to_person(js, fn)
					continue
		if info['wikidata']:
			wjs = fetch('wikidata', info['wikidata'])
			# Check if person / group	
			e = get_wikidata_entity(wjs, info['wikidata'])
			if 'P31' in e and (e['P31'] == "wd:Q5" or "wd:Q5" in e['P31']):
				group_to_person(js, fn)
				continue
			elif 'P21' in e or 'P569' in e or 'P19' in e or 'P570' in e or 'P20' in e or 'P26' in e or 'P40' in e:
				group_to_person(js, fn)
				continue
		# print("Still a group")


