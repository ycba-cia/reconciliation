import os
import re
import uuid
import sys
import pathlib
import json
import time
from io import BytesIO

source = "../../lux"  # Here there should be the S3 buckets for v9 per unit
dest = "../input_lux_min"

ead_uri_map = {}


outrecs = []

done_supertypes = {}
done_specific_types = {}
done_title_types = {}
done_identifier_types = {}
done_bdnote_props = {}
done_materials = {}
done_note_types = {}
done_colls = {}
done_holdings = {}
done_divisions = {}
done_hier_types = {}
done_agent_types = {}
done_agent_roles = {}
done_place_roles = {}
done_date_roles = {}
done_meas_types = {}
done_meas_units = {}
done_asset_types = {}
done_asset_flags = {}
done_languages = {}
done_place_types = {}

done_features = {}

index = {}
index['supertypes'] = done_supertypes
index['specific_types'] = done_specific_types
index['title_types'] = done_title_types
index['bdnote_types'] = done_bdnote_props
index['materials'] = done_materials
index['note_types'] = done_note_types
index['collections'] = done_colls
index['holdings'] = done_holdings
index['hierarchy_types'] = done_hier_types
index['agent_types'] = done_agent_types
index['agent_roles'] = done_agent_roles
index['place_roles'] = done_place_roles
index['date_roles'] = done_date_roles
index['date_roles'] = done_date_roles
index['measure_types'] = done_meas_types
index['measure_units'] = done_meas_units
index['asset_types'] = done_asset_types
index['asset_flags'] = done_asset_flags
index['languages'] = done_languages
index['place_types'] = done_place_types
index['features'] = done_features


bdnote_props = [
	'edition_display',
	'imprint_display',
	'materials_display',
	'inscription_display',
	'provenance_display',
	'acquisition_source_display']

agent_roles_translations = {
	"acteur": "actor",
	"auteur": "author",
	"traducteur": "translator",
	"trad": "translator", 
	"编": "editor",
	"译": "translator",
	"译述": "translator",
	"撰": "author",
	"编辑": "editor",
	"编著": "editor",
	"辑": "editor",
	"主编": "editor in chief",
	"原着": "author", # "original" ... author?
	"编译": "compiler",
	"编纂": "compiler",
	"编校": "editor",
	"编选": "compiler",
	"wen zi zuo zhe": "author",
	"main author": "author",
	"authors": "author",
	"auth": "author",
	"ed": "editor",
	"illustrations": "illustrator",	
	"illus": "illustrator",
	"ill": "illustrator",
	"trans": "translator",
	"tr": "translator",
	"printers": "printer",
	"prin": "printer",
	"ptr": "printer",
	'colourist': "colorist",
	"comp": "composer",
	"architects": "architect",
	'co-author': "author",
	"publishers": "publisher",
	"pub": "publisher",
	'binders': 'binder',
	"onscreen presenter": "on-screen presenter",
	"onscreen participant": "on-screen participant",
	"wood engraver": "wood-engraver",
	"metal engraver": "metal-engraver",
	"writer": "author",
	"collaborator" : "contributor",
	"maker": "creator",
	'binding designers': "binding designer",
	'bookjacket designer': "book jacket designer",
	"cast": "actor",
	"cast member": "actor",
	"director of photography": "cinematographer",
	"respondant": "respondent",
	"defendent": "defendant",
	"honouree": "honoree",
}

def transform_json(record, fn):

	###  record
	# The only thing we really care about is the identifier, which goes to the URI
	md_identifier = record['record'].get('metadata_identifier', fn)

	### basic_descriptors // classifications
	# Need to process basic first to find the oject type
	bd = record['basic_descriptors']
	st = bd.get('supertypes', [])
	# Get super types
	if type(st) != list:
		st = [[st]]
	nst = []
	for s in st:
		if type(s) != list:
			nst.append([s])
		else:
			nst.append(s)

	for st in nst:
		if not st[0] in done_supertypes:
			outrecs.append(record)
			done_supertypes[st[0]] = md_identifier
			return 1

	specific_type = bd.get('specific_type', [])
	for st in specific_type:
		if not st in done_specific_types:
			outrecs.append(record)
			done_specific_types[st] = md_identifier
			return 1

	# titles
	titles = record['titles']
	for title in titles:
		ttype_label = title.get('title_type', None)
		tlabel = title.get('title_label', None)
		tvalues = title.get('title_display', [])

		if not ttype_label in done_title_types:
			outrecs.append(record)
			done_title_types[ttype_label] = md_identifier
			return 1

	# identifiers
	identifiers = record.get('identifiers', [])
	for i in identifiers:
		ival = i.get('identifier_value', '')
		if ival:	
			ityp = i.get('identifier_type', None)

			if not ityp in done_identifier_types:
				outrecs.append(record)
				done_identifier_types[ityp] = md_identifier
				return 1

			idisp = i.get('identifier_display', '')
			if idisp and idisp != ival:
				if not "identifier_display" in done_features:
					outrecs.append(record)
					done_features['identifier_display'] = md_identifier
					return 1

	# basic_descriptor notes
	for prop in bdnote_props:
		if prop in bd and not prop in done_bdnote_props:
			outrecs.append(record)
			done_bdnote_props[prop] = md_identifier
			return 1

	# materials
	mats = bd.get('materials_type', [])
	mat_uris = bd.get('materials_type_URI', [])

	for mat in mats:
		if not mat in done_materials:
			outrecs.append(record)
			done_materials[mat] = md_identifier
			return 1

	# notes
	notes = record.get('notes', [])
	for n in notes:
		nt = n.get('note_type', None)
		if not nt in done_note_types:
			outrecs.append(record)
			done_note_types[nt] = md_identifier
			return 1

		nl = n.get('note_label', None)
		if nl and not 'note_label' in done_features:
			outrecs.append(record)
			done_features['note_label'] = md_identifier
			return 1

	# citations // as notes
	cites = record.get('citations', [])
	if not cites and not 'citation' in done_features:
		outrecs.append(record)
		done_features['citation'] = md_identifier
		return 1

	# locations
	locations = record.get('locations', [])

	for loc in locations:
		ards = loc.get('access_in_repository_display', [])		
		if ards and not 'a_in_repo_disp' in done_features:
			outrecs.append(record)
			done_features['a_in_repo_disp'] = md_identifier
			return 1

		refs = loc.get('access_in_repository_URI', [])
		if refs and not 'a_in_repo_uri' in done_features:
			outrecs.append(record)
			done_features['a_in_repo_uri'] = md_identifier
			return 1

		colls = loc.get('collections', [])
		for c in colls:
			if c and not c in done_colls:
				outrecs.append(record)
				done_colls[c] = md_identifier
				return 1

		yhi = loc.get('yul_holding_institution', [])
		for y in yhi:
			if not y in done_holdings:
				outrecs.append(record)				
				done_holdings[y] = md_identifier
				return 1

		cd = loc.get('campus_division', [])
		for c in cd:
			if not c in done_divisions:
				outrecs.append(record)
				done_divisions[c] = md_identifier
				return 1

		lcn = loc.get('location_call_number', "")
		if lcn and not 'call_number' in done_features:				
			outrecs.append(record)
			done_features['call_number'] = md_identifier
			return 1


	# measurements
	measurements = record.get('measurements', [])
	for mez in measurements:
		lbl = mez.get('measurement_label', '')
		forms = mez.get('measurement_form', [])
		for form in forms:
			dnames = form.get('measurement_display', [])
			for d in dnames:
				if d and not 'measurement_display' in done_features:
					outrecs.append(record)
					done_features['measurement_display'] = md_identifier
					return 1
			aspects = form.get('measurement_aspect', [])
			for a in aspects:
				at = a.get('measurement_type', None)
				au = a.get('measurement_unit', None)			
				av = a.get('measurement_value', None)
				if not at in done_meas_types:
					outrecs.append(record)
					done_meas_types[at] = md_identifier
					return 1
				if not au in done_meas_units:
					outrecs.append(record)
					done_meas_units[au] = md_identifier
					return 1

	langs = record.get('languages', [])
	for l in langs:
		llbl = l.get('language_display', "")
		if not llbl in done_languages:
			outrecs.append(record)
			done_languages[llbl] = md_identifier
			return 1

	# digital_assets
	digass = record.get('digital_assets', [])
	for da in digass:
		da_uris = da.get('asset_URI', [])
		da_type = da.get('asset_type', None) # image, soundcloud
		da_flag = da.get('asset_flag', None) # primary image,

		if da_uris:
			if not da_type in done_asset_types:
				outrecs.append(record)
				done_asset_types[da_type] = md_identifier
				return 1
			if not da_flag in done_asset_flags:
				outrecs.append(record)
				done_asset_flags[da_flag] = md_identifier
				return 1

		captions = da.get('asset_caption_display', [])
		if captions and not "asset_captions" in done_features:
			outrecs.append(record)
			done_features['asset_captions'] = md_identifier
			return 1

	# hierarchies
	hiers = record.get('hierarchies', [])
	for h in hiers:

		htype = h.get('hierarchy_type', "") # "EAD; Series" / EAD; File / common names / taxonomic names

		if not htype in done_hier_types:
			outrecs.append(record)
			done_hier_types[htype] = md_identifier
			return 1

	# agents
	agents = record.get('agents', [])
	for a in agents:
		dnames = a.get('agent_display', [])
		sname = a.get('agent_sortname', '')
		if not sname and not dnames:
			continue

		auri = a.get('agent_URI', [])
		if auri and not 'agent_uri' in done_features:
			outrecs.append(record)
			done_features['agent_uri'] = md_identifier
			return 1

		if sname and not "agent_sortname" in done_features:
			outrecs.append(record)
			done_features['agent_sortname'] = md_identifier
			return 1

		atypl = a.get('agent_type_display', None)
		if atypl and not atypl in done_agent_types:
			outrecs.append(record)
			done_agent_types[atypl] = md_identifier
			return 1

		rolel = a.get('agent_role_label', '')
		rolec = a.get('agent_role_code', '').lower()
		roleu = a.get('agent_role_URI', [])
		context = a.get('agent_context_display', [])
		if roleu:
			roleu = roleu[0] # could still be ""
		if context:
			context = context[0]

		if rolec == "voc":
			rolel = "vocalist"
		elif rolec == "ptr" and not rolel:
			rolel = "printer"
		elif rolec == "ed" and not rolel:
			rolel = "editor"
		elif rolec == "clb" and not rolel:
			rolel = "collaborator"

		if not rolel and not rolec and not roleu and not context and not 'no_agent_role' in done_features:
			# An actor with no role at all
			outrecs.append(record)
			done_features['no_agent_role'] = md_identifier
			return 1

		else:

			# Creation
			roleLabel = rolel
			rolel = rolel.lower()
			rolel = rolel.replace('jt. ', ' joint ')
			rolel = rolel.replace('ed.', 'editor')
			rolel = rolel.replace('tr.', 'translator')
			rolel = rolel.replace(' & ', ' and ')
			rolel = rolel.replace("(expression)", "")
			rolel = rolel.replace("supposed ", "attributed ")
			rolel = rolel.replace(', attributed to', "")
			rolel = rolel.replace('joint ', '')
			rolel = rolel.replace('attributed name', 'creator')
			rolel = rolel.replace('attributed ', '')
			rolel = rolel.replace(', possibly by', '')
			rolel = rolel.replace(', probably by', '')
			rolel = rolel.strip()
			if len(rolel) > 2 and rolel[0] == "(" and rolel[-1] == ")":
				rolel = rolel[1:-1].strip()

			if rolel in agent_roles_translations:
				rolel = agent_roles_translations[rolel]

			if rolel.startswith('author of ') or rolel.startswith("writer of "):
				# XXX This could be separated out as a part
				rolel = "author"
			elif rolel.startswith('editor of'):
				rolel = "editor"
			elif rolel.startswith('arranger of'):
				rolel = "arranger"
			elif rolel.startswith('artist, '):
				rolel = "artist"
			elif rolel.startswith('maker, '):
				rolel = "maker"

			while rolel.endswith('.'):
				rolel = rolel[:-1]

			if rolel and not rolel in done_agent_roles:
				outrecs.append(record)
				done_agent_roles[rolel] = md_identifier
				return 1

	# places
	places = record.get('places', [])
	for p in places:

		puris = p.get('place_URI', [])
		pnames = p.get('place_display', [])
		if not pnames or (pnames and len(pnames) == 1 and not pnames[0]['value']):
			continue

		if pnames and not 'place_name' in done_features:
			outrecs.append(record)
			done_features['place_name'] = md_identifier
			return 1
		if puris and not 'place_uri' in done_features:
			outrecs.append(record)
			done_features['place_uri'] = md_identifier
			return 1			

		# Only in YPM?
		pts = p.get('place_type_display', "")
		if pts and not pts in done_place_types:
			outrecs.append(record)
			done_place_types[pts] = md_identifier
			return 1

		# Only for maps?
		coords = p.get("place_coordinates_display", "")
		if coords and not 'place_coords' in done_features:
			outrecs.append(record)
			done_features['place_coords'] = md_identifier
			return 1

		rolel = p.get('place_role_label', '').lower()
		rolec = p.get('place_role_code', '').lower()
		roleu = p.get('place_role_URI', [])
		if roleu:
			roleu = roleu[0] # could still be ""
		if rolel.startswith('made, '):
			rolel = "made"
		elif rolel.startswith('found, '):
			rolel = "found"

		if not rolel and not rolec and not roleu and not 'no_place_role' in done_features:
			# anonymous activity?
			outrecs.append(record)
			done_features['no_place_role'] = md_identifier
			return 1
		else:
			if rolel and not rolel in done_place_roles:
				outrecs.append(record)
				done_place_roles[rolel] = md_identifier
				return 1


	# dates
	dates = record.get('dates', [])
	for dt in dates:
		rolel = dt.get('date_role_label', '').lower()
		if not rolel:
			outrecs.append(record)
			done_features['no_date_role'] = md_identifier
			return 1
		else:
			if rolel and not rolel in done_date_roles:
				outrecs.append(record)
				done_date_roles[rolel] = md_identifier
				return 1

	# subjects
	subjects = record.get('subjects', [])
	for sub in subjects:
		suri = sub.get('subject_heading_URI', [])
		if suri and not 'subject_uri' in done_features:
			outrecs.append(record)
			done_features['subject_uri'] = md_identifier
			return 1	
			
		shd = sub.get('subject_heading_display', [])
		if suri and not 'subject_disp' in done_features:
			outrecs.append(record)
			done_features['subject_disp'] = md_identifier
			return 1

		facets = sub.get('subject_facets', [])
		if suri and not 'subject_facet' in done_features:
			outrecs.append(record)
			done_features['subject_facet'] = md_identifier
			return 1

	return 0


def process_jsonl(fn):
	
	used = 0
	lx = 0
	with open(fn) as fh:
		for l in fh.readlines():
			lx += 1
			try:
				js = json.loads(l)
			except:
				print(f"Bad JSON in {fn} line {lx}")
				continue
			used += transform_json(js, f"{fn}-{lx}")
			
	return used

def process_json(fn):

	with open(fn) as fh:
		try:
			js = json.load(fh)
		except:
			print(f"Bad JSON in {fn}")
			return 0
		used = transform_json(js, fn)
	return used


total = 0
units = os.listdir(source)
units.sort()
last_report = 0
report_every = 50
start = time.time()
for unit in units[:]:
	unitfn = os.path.join(source, unit)
	if os.path.isdir(unitfn):
		filedirs = os.listdir(unitfn)
		filedirs.sort()
		for fd in filedirs[:]:
			fn = os.path.join(unitfn, fd)
			if os.path.isdir(fn):
				# descend for aspace
				sfiles = os.listdir(fn)
				sfiles.sort()
				for sf in sfiles:
					sfn = os.path.join(fn, sf)
					if sf.endswith('.json'):
						total += process_json(sfn)
					elif sf.endswith('.jsonl'):
						total += process_jsonl(sfn)
					else:
						print(f"Found extraneous file: {sfn}")
						continue
			elif fd.endswith('.json'):
				total += process_json(fn)
			elif fd.endswith('.jsonl'):
				total += process_jsonl(fn)
			else:
				# These shouldn't be here
				print(f"Found extraneous file: {fd}")
				continue
			if last_report + report_every < total:
				last_report = total
				secs = time.time() - start
				print(f"Found {total} interesting in {secs}") 
				# break

jstr = json.dumps(outrecs)
fh = open('minimal_types.json', 'w')
fh.write(jstr)
fh.close()

idxstr = json.dumps(index)
fh = open('minimal_type_index.json', 'w')
fh.write(idxstr)
fh.close()
