from data_utils import fetch, recurse_fetch, get_wikidata_entity
from identifiers import DB
import json
import os

mappings = {}

maps = os.listdir('../data/mappings')
for m in maps:
	fh = open(f'../data/mappings/{m}')
	mappings[m[:-5]] = json.load(fh)
	fh.close()

maps = {}


def align_place(uuid, info):

	wdid = info.get('wikidata', None)
	tgnid = info.get('tgn', None)
	wofid = info.get('wof', None)
	geoid = info.get('geonames', None)
	viafid = info.get('viaf', None)
	fastid = info.get('fast', None)
	lcnafid = info.get('lcsh', None)

	if tgnid and not wdid:
		wdid = mappings['tgn_wikidata'].get(tgnid, None)
		if wdid:
			wdid = wdid.replace('wd:', '')
			fetch('wikidata', wdid)
	if geoid and not wdid:
		wdid = mappings['geonames_wikidata'].get(geoid, None)
		if wdid:
			wdid = wdid.replace('wd:', '')
			fetch('wikidata', wdid)
	if geoid and not fastid:
		gl = geoid if type(geoid) == list  else [geoid]
		for g in gl:
			fastid = mappings['geonames_fast'].get(g, None)
			if fastid:
				fetch('fast', fastid, format='xml')

	if wdid:
		wdjs = fetch('wikidata', wdid)
		if wdjs:
			wdjs = get_wikidata_entity(wdjs, wdid)

		if not tgnid and 'P1667' in wdjs:
			tgnid = wdjs['P1667']
			recurse_fetch('tgn', tgnid)	
		if not geoid and 'P1566' in wdjs:
			geoid = wdjs['P1566']
			gl = geoid if type(geoid) == list  else [geoid]			
			for g in gl:
				try:
					fetch('geonames', g)
				except:
					print(f"failed to fetch: {g}")
			if not fastid:
				for g in gl:
					fastid = mappings['geonames_fast'].get(g, None)
					if fastid:
						fetch('fast', fastid, format='xml')			
		if not fastid and 'P2163' in wdjs:
			fastid = wdjs['P2163']
			fl = fastid if type(fastid) == list else [fastid]
			for f in fl:
				fetch('fast', f, format='xml')	
		if not wofid and 'P6766' in wdjs:
			wofid = wdjs['P6766']
			wl = wofid if type(wofid) == list else [wofid]
			for w in wl:
				fetch('wof', w)
		if not viafid and 'P214' in wdjs:
			viafid = wdjs['P214']
			if type(viafid) == list:
				for v in viafid:
					fetch('viaf', v, format='xml')
			else:
				fetch('viaf', viafid, format='xml')
		if not lcnafid and 'P244' in wdjs:
			lcnafid = wdjs['P244']
			if type(lcnafid) == list:
				for l in lcnafid:
					fetch('lcnaf', l)
			else:	
				fetch('lcnaf', lcnafid)

	if not 'class' in info:
		info['class'] = "Place"

	info['tgn'] = tgnid
	info['wikidata'] = wdid
	info['wof'] = wofid
	info['geonames'] = geoid
	info['viaf'] = viafid
	info['fast'] = fastid
	info['lcnaf'] = lcnafid
	# print(info)
	return info

def align_actor(uuid, info):

	ulanid = info.get('ulan', None)
	wdid = info.get('wikidata', None)
	lcnafid = info.get('lcnaf', None)
	viafid = info.get('viaf', None)
	isniid = info.get('isni', None)
	fastid = info.get('fast', None)
	typ = info.get('class', None)

	if lcnafid and not ulanid:
		ulanid = mappings['lcnaf_ulan'].get('lcnafid', None)
	if ulanid and not wdid:
		wdid = mappings['ulan_wikidata'].get(ulanid, None)
		if wdid:
			wdid = wdid.replace('wd:', '')
			fetch('wikidata', wdid)		
	if ulanid and not lcnafid:
		lcnafid = mappings['ulan_lcnaf'].get(ulanid, None)
		if lcnafid:
			fetch('lcnaf', lcnafid)
	if lcnafid and not wdid:
		wdid = mappings['lcnaf_wikidata'].get(lcnafid, None)
	if lcnafid and not viafid:
		viafid = mappings['lcnaf_viaf'].get(lcnafid, None)
	if lcnafid and not fastid:
		fastid = mappings['lcnaf_fast'].get(lcnafid, None)

	if wdid:
		wdjs = fetch('wikidata', wdid)
		if wdjs:
			wdjs = get_wikidata_entity(wdjs, wdid)

			if not typ:
				if 'P31' in wdjs and (wdjs['P31'] == "wd:Q5" or "wd:Q5" in wdjs['P31']):
					typ = "Person"
				elif 'P31' in wdjs and (wdjs['P31'] in ["wd:Q43229", "wd:Q8436", "Q4830453"] or "wd:Q43229" in wdjs['P31'] or "wd:Q8436" in wdjs['P31'] or "wd:Q4830453" in wdjs['P31']):
					# 43229 == Organization
					# 8436 == Family
					# 4830453 == Business
					typ = "Group"
				elif 'P21' in wdjs or 'P569' in wdjs or 'P19' in wdjs or 'P570' in wdjs or 'P20' in wdjs or 'P26' in wdjs or 'P40' in wdjs:
					typ = "Person"
					print(f"Predict person based on attributes but, but P31 is {wdjs.get('P31', None)}")
				elif 'P2124' in wdjs or 'P488' in wdjs or 'P571' in wdjs or 'P576' in wdjs or 'P159' in wdjs:
					typ = "Group"
					print(f"Predict Group based on attributes")
				else:
					typ = "Group"
					print(f"Predict Group for {k} lacking any other information")

			if not ulanid and 'P245' in wdjs:
				ulanid = wdjs['P245']
			if not lcnafid and 'P244' in wdjs:
				lcnafid = wdjs['P244']
			if not viafid and 'P214' in wdjs:
				viafid = wdjs['P214']
			if not isniid and 'P213' in wdjs:
				isniid = wdjs['P213']
			if not fastid and 'P2163' in wdjs:
				fastid = wdjs['P2163']

	info['ulan'] = ulanid
	info['wikidata'] = wdid
	info['viaf'] = viafid
	info['fast'] = fastid
	info['lcnaf'] = lcnafid
	info['isni'] = isniid
	return info

def align_concept(uuid, info):
	pass

def align_textual(uuid, info):
	pass

def align_event(uuid, info):
	pass

c = DB.cursor(prefix="uuid")

for (k,v) in c:
	if 'tgn' in v or 'geonames' in v:
		info = align_place(k, v)
		DB[k] = info
	elif 'ulan' in v or 'lcnaf' in v:
		info = align_actor(k, v)
		DB[k] = info
	elif 'aat' in v:
		#info = align_concept(k, v)
		#DB[k] = info
		pass
	elif 'class' in v:
		cl = v['class']
		if cl in ['Person', 'Group']:
			align_actor(k, v)
		elif cl == 'Place':
			align_place(k, v)
		elif cl in ['Type', 'MeasurementUnit', 'Material', 'Currency', 'Language', 'Concept']:
			align_concept(k, v)
		elif cl == 'LinguisticObject':
			align_textual(k, v)
		elif cl in ['HumanMadeObject', 'VisualItem', 'DigitalObject']:
			pass
		elif cl in ['Period', 'Event', 'Activity']:
			align_event(k ,v)
		else:
			print(f"Unprocessed class: {cl}")
	else:
		# print(f"Unknown type of entity: {v}")
		pass

DB.commit()
