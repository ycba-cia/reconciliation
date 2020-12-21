
import os
import json
import copy
from cromulent import model, vocab, reader
import pathlib
from shapely.geometry import Polygon
from identifiers import DB, get_languages, map_uuid, map_uuid_uri, rewrite_crom_ids
from data_utils import fetch, entity_templates, get_wikidata_entity
import shutil

model.factory.auto_assign_id = False

source = "../data/ycba/linked_art/place"
output = "../output"

languages = get_languages()

def _vec2d_dist(p1, p2):
    return (p1[0] - p2[0])**2 + (p1[1] - p2[1])**2
def _vec2d_sub(p1, p2):
    return (p1[0]-p2[0], p1[1]-p2[1])
def _vec2d_mult(p1, p2):
    return p1[0]*p2[0] + p1[1]*p2[1]

def ramerdouglas(line, dist):
    if len(line) < 3:
        return line
    (begin, end) = (line[0], line[-1]) if line[0] != line[-1] else (line[0], line[-2])
    distSq = []
    for curr in line[1:-1]:
        tmp = (
            _vec2d_dist(begin, curr) - _vec2d_mult(_vec2d_sub(end, begin), _vec2d_sub(curr, begin)) ** 2 / _vec2d_dist(begin, end))
        distSq.append(tmp)
    maxdist = max(distSq)
    if maxdist < dist ** 2:
        return [begin, end]
    pos = distSq.index(maxdist)
    return (ramerdouglas(line[:pos + 2], dist) + 
            ramerdouglas(line[pos + 1:], dist)[1:])

featureCollTempl = {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {}, 'properties': {}}]}
rdr = reader.Reader()
chunks = os.listdir(source)
chunks.sort()
for chunk in chunks:
	chunkdir = os.path.join(source, chunk)
	files = os.listdir(chunkdir)
	files.sort()
	for f in files:
		uuid = f[:-5]
		uu = f"urn:uuid:{uuid}"
		fn = os.path.join(chunkdir, f)

		# Ensure we don't inherit from previous iteration
		coords = []
			
		fnd = os.path.join(output, 'place', uuid[9:11])
		pathlib.Path(fnd).mkdir(parents=True, exist_ok=True)
		outfn = os.path.join(fnd, f"{uuid[9:]}.json")

		if uu in DB:
			fh = open(fn)
			data = json.load(fh)
			fh.close()

			info = DB[uu]
			tgnid = info.get('tgn', None)
			wdid = info.get('wikidata', None)
			geoid = info.get('geonames', None)
			wofid = info.get('wof', None)

			if tgnid or wdid or geoid or wofid:
				place = rdr.read(data)
			else:
				continue

			if hasattr(place, 'equivalent'):
				eqids = [eq.id for eq in place.equivalent]
			else:
				eqids = []

			if tgnid:
				la_js = fetch('tgn', tgnid)
				if not la_js:
					print(f"Cannot fetch {tgnid}")
					la_js = {}

				if 'identified_by' in la_js:
					for i in la_js['identified_by']:
						content = i.get('content', i.get('value', None))
						typ = i['type']
						lang = i.get('language', None)
						if lang:
							lang = lang[0]["_label"]
							didx = lang.find('-')
							if didx > -1:
								lang = lang[:didx]

						if typ == "Identifier": 
							# This is just TGN id we already have
							pass
						elif typ == "crm:E47_Spatial_Coordinates":
							# coords as a list
							content = content.replace('-.', '-0.')
							content = content.replace('[.', '[0.')
							content = content.replace(',.', ',0.')
							coords = json.loads(content)
							place.defined_by = None
							tmpl = copy.deepcopy(featureCollTempl)
							tmpl['features'][0]['geometry'] = {"type": "Point", "coordinates": coords}
							place.defined_by = json.dumps(tmpl)
						elif typ == "Name":
							# Check if we have the name already
							found = False
							for pi in place.identified_by:
								if i['content'] == pi.content:
									# This tests only if the string is present, not that the langauge matches as well
									found = True
									break
							if not found:
								n = model.Name(content=content)
								if lang and lang in languages and languages[lang]:
									n.language = model.Language(ident=languages[lang])
								place.identified_by = n

				if 'classified_as' in la_js:
					for c in la_js['classified_as']:
						i = c['id']
						if hasattr(place, 'classified_as'):
							found = False
							for pc in place.classified_as:
								if pc.id == c['id']:
									found = True
									break
							if not found:
								place.classified_as = model.Type(ident=c['id'])
						else:
							place.classified_as = model.Type(ident=c['id'])

				if 'subject_of' in la_js:
					for s in la_js['subject_of']:
						if hasattr(place, 'referred_to_by'):
							found = False
							for ps in place.referred_to_by:
								if ps.content == s['content']:
									found = True
									break
							if not found:
								place.referred_to_by = vocab.Description(content=s['content'])
						else:
							place.referred_to_by = vocab.Description(content=s['content'])

				if 'part_of' in la_js:
					parts = la_js['part_of']
					if type(parts) != list:
						parts = [parts]
					for p in parts:
						if type(p) == str:
							p = {'id': p}
						puu = map_uuid_uri(p['id'])
						found = False
						if hasattr(place, 'part_of'):
							for pp in place.part_of:
								if pp.id == puu:
									found = True
									break
						if not found:
							place.part_of = model.Place(ident=puu)

				# Ensure TGN is in equivalent
				tgn_uri = entity_templates['tgn'].format(ident=tgnid)
				if not tgn_uri in eqids:
					place.equivalent = model.Place(ident=tgn_uri)

			if wdid:
				wd_js = fetch('wikidata', wdid)
				if wd_js:
					e = get_wikidata_entity(wd_js, wdid)

					plmap = {x['@language'] : x['@value'] for x in e['prefLabel']}
					baselangs = [x.language[0].id for x in place.identified_by if hasattr(x, 'language')]
					for (l,n) in plmap.items():
						if l in languages and not languages[l] in baselangs and languages[l]:
							name = model.Name(content=n)
							name.language = model.Language(ident=languages[l])						
							place.identified_by = name

					uni = e.get('P487', None)
					# this is the flag character for countries

					if not wofid:
						# Test if it is there
						wofid = e.get('P6766', None)
					if not geoid:
						geoid = e.get('P1566', None)


					northmost = e.get('P1332', None)
					southmost = e.get('P1333', None)
					eastmost = e.get('P1334', None)
					westmost = e.get('P1335', None)

					if northmost and southmost and eastmost and westmost:
						# from here we can make a polygon

						coords = []
						for pt in [northmost, eastmost, southmost, westmost, northmost]:
							if type(pt) == list:
								pt = pt[0]
							pt = pt.strip().replace('Point(', '').replace(')', '')
							pt = [float(x) for x in pt.split(' ')]							
							coords.append(pt)

						place.defined_by = None
						tmpl = copy.deepcopy(featureCollTempl)
						tmpl['features'][0]['geometry'] = {"type": "Polygon", "coordinates": [coords]}
						place.defined_by = json.dumps(tmpl)	

					else:
						coords = e.get('P625', None)
						if coords:
							# Rob trusts WD > TGN
							if type(coords) == list:
								# Pick first one, essentially at random
								coords = coords[0]

							coords = coords.strip().replace('Point(', '').replace(')', '')
							coords = [float(x) for x in coords.split(' ')]
							place.defined_by = None
							tmpl = copy.deepcopy(featureCollTempl)
							tmpl['features'][0]['geometry'] = {"type": "Point", "coordinates": coords}
							place.defined_by = json.dumps(tmpl)	

					# ensure we're in equivalent
					wduri = f'http://www.wikidata.org/entity/{wdid}'
					if not wduri in eqids:
						place.equivalent = model.Place(ident=wduri)

			if wofid:
				if type(wofid) == list:
					wofid = wofid[0]

				wof_js = fetch('wof', wofid)
				if wof_js:
					props = wof_js['properties']
					bbox = []
					point = []
					if 'geom:bbox' in props:
						bbox = props['geom:bbox']
						# String
						if type(bbox) == str:
							bbox = json.loads(f"[{bbox}]")
					if 'bbox' in wof_js and not bbox:
						bbox = wof_js['bbox']


					if 'lbl:latitude' in props:
						point = [props['lbl:longitude'], props['lbl:latitude']]
					if 'geom:latitude' in props and not point:
						point = [props['geom:longitude'], props['geom:latitude']]
					if 'mps:latitude' in props and not point:
						point = [props['mps:longitude'], props['lbl:latitude']]

					geom = wof_js['geometry']
					t = geom['type']
					coords = geom['coordinates']
					if t == "MultiPolygon":
						# Many of these are actually just Polygons
						# No need for geojson/shapely
						while(len(coords) == 1):
							coords = coords[0]
						if len(coords[0]) != 2 or type(coords[0][0]) != float:
							# A real multipolygon
							# Just use bounding box for now
							coords = [] 
						else:
							t = "Polygon"
					elif t == "Polygon":
						while(len(coords) == 1):
							coords = coords[0]					  
						if len(coords[0]) != 2 or type(coords[0][0]) != float:
							coords = []
					else:
						# These are all Points
						# print(f"Found unknown coords type: {t}")
						coords = []

					if coords:
						if len(coords) > 350:
							factor = 500 / (len(coords) * 10)
							while True:
								ncoords = ramerdouglas(coords, factor)
								# print(f"Reduced coordinate space from {len(coords)} to {len(ncoords)}")
								if len(ncoords) < 100:
									factor /= 2
								elif len(ncoords) > 600:
									factor *= 2
								else:
									coords = ncoords
									break
							coords = [coords]
						else:
							if len(coords) != 1:
								coords = [coords]

						rounded = [ [round(x[0], 5), round(x[1], 5)] for x in coords[0] ]

						remove_idx = []
						for x in range(len(coords)-1):
							if coords[x] == coords[x+1]:
								# drat
								remove_idx.append(x)
						if remove_idx:
							remove_idx.reverse()
							for idx in remove_idx:
								coords.pop(idx)

						p = Polygon(rounded)
						if p.area * 1000 < 5:
							# Polygon is so small as to be a point
							coords = []
						else:
							coords = [rounded]							

					if not coords and bbox:
						# Make a polygon from the bounding box
						# Do rounding here, in case we round it down to a point
						coords = [[bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]]]
						rounded = [ [round(x[0], 5), round(x[1], 5)] for x in coords ]
						broken = False
						for x in range(len(coords)-1):
							if coords[x] == coords[x+1]:
								# drat
								broken = True
								break
						if broken:
							coords = []
						else:
							coords = [rounded]

					place.defined_by = None
					tmpl = copy.deepcopy(featureCollTempl)
					if coords:
						tmpl['features'][0]['geometry'] = {"type": "Polygon", "coordinates": coords}
					else:
						print(point)
						tmpl['features'][0]['geometry'] = {'type': 'Point', 'coordinates': point}

					place.defined_by = json.dumps(tmpl)			

			if geoid:
				if type(geoid) == list:
					geoid = geoid[0]

				geouri = f"https://sws.geonames.org/{geoid}"
				if not geouri in eqids:
					place.equivalent = model.Place(ident=geouri)

			rewrite_crom_ids(place)
			# Now serialize merged Place record
			model.factory.toFile(place, compact=False, filename=outfn)
		else:
			# Just copy the file into output
			shutil.copyfile(fn, outfn)





