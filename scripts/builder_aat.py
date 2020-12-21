import os
import json
import uuid
import requests
from cromulent import model, vocab
import pathlib
from identifiers import DB, map_uuid, map_uuid_uri, get_languages, fetch_and_map
from data_utils import entity_templates

model.factory.auto_assign_id = False

source = "../data/aat"
dest = "../output"

classifications = {
	"http://vocab.getty.edu/term/type/UsedForTerm": map_uuid("aat", "300417478"),
	"http://vocab.getty.edu/term/type/AlternateDescriptor": map_uuid("aat", "300417477"),
	"http://vocab.getty.edu/term/type/Descriptor": map_uuid("aat", "300404670"),
	"http://vocab.getty.edu/aat/300435416": map_uuid("aat", "300411780"), # desc note --> desc
	"http://vocab.getty.edu/aat/300404629": map_uuid("aat", "300404621"), # uri --> local number
	"http://vocab.getty.edu/term/POS/PluralNoun": None,
	"http://vocab.getty.edu/term/POS/SingularNoun": None,
	"http://vocab.getty.edu/term/POS/Noun": None,
	"http://vocab.getty.edu/term/POS/BothSingularAndPlural": None,
	"http://vocab.getty.edu/term/POS/PastParticiple": None,
	"http://vocab.getty.edu/term/POS/VerbalNounGerund": None,
	"http://vocab.getty.edu/term/POS/Adjectival": None,
	"http://vocab.getty.edu/term/POS/MasculineSingularNoun": None,
	"http://vocab.getty.edu/term/POS/NeuterAdjectival": None,
	"http://vocab.getty.edu/term/POS/FeminineSingularNoun": None,
	"http://vocab.getty.edu/term/POS/MasculinePluralNoun": None,
	"http://vocab.getty.edu/term/POS/NeuterSingularNoun": None
}

languages = get_languages()
languages['qqq'] = None
languages['und'] = None
languages['x'] = None

def get_language_by_code(code):
	didx = code.find('-')
	if didx > -1:
		code = code[:didx]
	if code in languages:
		return languages[code]
	else:
		print(f"Unknown language code: {code}")
		return None

type_override = {}
for l in languages.values():
	type_override[l] = "Language"

for voc in vocab.identity_instances.values():
	if voc['parent'] == vocab.MeasurementUnit:
		type_override[map_uuid('aat', voc['id'])] = "MeasurementUnit"
	elif voc['parent'] == vocab.Material:
		type_override[map_uuid('aat', voc['id'])] = "Material"
	elif voc['parent'] == vocab.Currency:
		type_override[map_uuid('aat', voc['id'])] = "Currency"
		# or parent = 300411993



parent_override = {}
material_facet = map_uuid('aat', '300010358')
language_facet = map_uuid('aat', '300411913')
parent_override[language_facet] = "Language"
parent_override[map_uuid('aat', '300411993')] = "Currency"
parent_override[map_uuid('aat', '300379251')] = "MeasurementUnit"
parent_override[material_facet] = "Material" # This is the top of a deeeeeeep hierarchy



typedirs = {"Type": "concept", "MeasurementUnit": "unit", "Currency": "currency", "Language": "language", "Material": "material"}
# consider: ['see_also', 'related_from_by', 'la:related_from_by']

AAT_PREFIX = entity_templates['aat'].replace("{ident}", "")


def build_concept_from_aat(data):
	if type(data) == list:
		# It's an old reference
		data = data[0]
		if 'http://purl.org/dc/terms/isReplacedBy' in data:
			old = data['@id'].replace(AAT_PREFIX, "aat:")
			new = data['http://purl.org/dc/terms/isReplacedBy']
			if type(new) == list:
				new = new[0]
			if type(new) == dict:
				new = new['@id']
			new = new.replace(AAT_PREFIX, "aat:")
			DB[old] = new
			if not new in DB:
				fetch_and_map("aat", new[-9:])
		return

	# Map our ID and store orig
	my_aat_id = data['id'][-9:]
	myid = map_uuid('aat', my_aat_id)
	mytype = type_override.get(data['id'], "Type")

	# Might be narrower than an override, then override
	if 'broader' in data:
		part_ofs = data['broader']
	elif 'part_of' in data:
		part_ofs = data['part_of']
	else:
		part_ofs = []
	if type(part_ofs) != list:
		part_ofs = [part_ofs]

	new_part_ofs = []
	for p in part_ofs:
		if type(p) == str:
			p = {'id': p}

		puu = map_uuid_uri(p['id'])
		if puu in parent_override:
			mytype = parent_override[puu]

		pclass = type_override.get(puu, 'Type')
		if pclass == "Material" and puu != material_facet:
			mytype = "Material"
		elif pclass == "Language" and puu != language_facet:
			mytype = "Language"

		part = getattr(model, pclass)(ident=puu)
		if '_label' in p:
			if type(p['_label']) == list:
				p['_label'] = p['_label'][0]
			if type(p['_label']) == dict:
				part._label = p['_label']['@value']
			else:
				part._label = p['_label']
		new_part_ofs.append(part)

	myclass = getattr(model, mytype)

	record = myclass(ident=myid)
	record.equivalent = myclass(ident=f"{AAT_PREFIX}{my_aat_id}")
	if '_label' in data:
		if type(data['_label']) == list:
			record._label = data['_label'][0]
		else:
			record._label = data['_label']

	for n in data['identified_by']:
		if 'type' in n and not n['type'] in ['Name', 'Identifier']:
			continue
		name = getattr(model, n['type'])()
		name.content = n['content']
		if 'language' in n:
			for l in n['language']:
				lang = get_language_by_code(l['_label'])
				# We only care about one
				if lang:
					name.language = model.Language(ident=lang)
					break
		if 'classified_as' in n:
			for c in n['classified_as']:
				if c['id'] in classifications:
					nc = classifications[c['id']]
					if nc:
						name.classified_as = model.Type(ident=nc)
		record.identified_by = name

	for p in new_part_ofs:
		record.broader = p

	descs = []
	if 'subject_of' in data:
		descs = data['subject_of']
		if type(descs) != list:
			descs = [descs]
	if 'referred_to_by' in data:
		rtb = data['referred_to_by']
		if type(rtb) != list:
			rtb = [rtb]
		descs.extend(rtb)

	for ref in descs:
		if 'content' in ref:
			desc = model.LinguisticObject()
			desc.content = ref['content']
			if 'language' in ref:
				for l in ref['language']:
					lang = get_language_by_code(l['_label'])
					# We only care about one
					if lang:
						desc.language = model.Language(ident=lang)
						break
			if 'classified_as' in ref:
				for c in ref['classified_as']:
					if c['id'] in classifications:
						nc = classifications[c['id']]
						if nc:
							desc.classified_as = model.Type(ident=nc)

	# add equivalents from identifier DB
	equivs = DB[record.id]
	try:
		del equivs['aat']
	except:
		# uhhh ... add it!
		nequivs = equivs.copy()
		nequivs['aat'] = my_aat_id
		DB[record.id] = nequivs
		DB.commit() 
	if equivs:
		curr = [x.id for x in record.equivalent]
		for (k, v) in equivs.items():
			if k in entity_templates:
				if v:
					uri = entity_templates[k].format(ident=v)
					# only add if not already there (don't know how it would be there?)
					if not uri in curr:
						record.equivalent = myclass(ident=uri)

	return record


files = os.listdir(source)
for f in files:
	fh = open(os.path.join(source, f))
	data = json.load(fh)
	fh.close()

	record = build_concept_from_aat(data)
	if record:
		# If not, could be a redirect
		typedir = typedirs.get(record.type, "concept")
		fnd = os.path.join(dest, typedir, record.id[9:11])
		pathlib.Path(fnd).mkdir(parents=True, exist_ok=True)
		fn = record.id[9:] + '.json'
		model.factory.toFile(record, filename=os.path.join(fnd, fn), compact=False)	



