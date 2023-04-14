import time
start_time = time.time()
import os
import os.path
from os import path
from lxml import etree
from io import BytesIO
import re
from cromulent import model, vocab
from cromulent.extract import date_cleaner
from cromulent.model import factory
import uuid
import sys
import pathlib
import json
import pymysql
import filecmp
import datetime


if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

from ycba_prefs import *

from identifiers import map_uuid, map_uuid_uri, DB, rewrite_crom_ids
from data_utils import entity_templates, validate_wikidata, validate_ulan
from lmdb_utils import LMDB

if len(sys.argv) > 1:
	config1 = sys.argv[1]
else:
	config1 = "prod"
if len(sys.argv) > 2:
	config2 = sys.argv[2]
else:
	config2 = "do_site_everytime"
print(config1)

def _add_linked_art_boundary_check():

	boundary_classes = [x.__name__ for x in [vocab.Actor, vocab.HumanMadeObject, vocab.Person, vocab.Group, vocab.VisualItem, \
		vocab.Place, vocab.Period, vocab.LinguisticObject, vocab.Phase, vocab.Set, vocab.Event, vocab.DigitalObject, vocab.DigitalService]]
	data_embed_classes = [vocab.Name, vocab.Identifier, vocab.Dimension, vocab.TimeSpan, vocab.MonetaryAmount]
	type_embed_classes = [vocab.Type, vocab.Currency, vocab.Language, vocab.Material, vocab.MeasurementUnit]
	event_embed_classes = [vocab.Birth, vocab.Creation, vocab.Production, vocab.Formation, vocab.Payment, \
							vocab.Death, vocab.Destruction, vocab.Dissolution ]
	all_embed_classes = []
	all_embed_classes.extend(data_embed_classes)
	all_embed_classes.extend(type_embed_classes)
	all_embed_classes.extend(event_embed_classes)
	embed_classes = [x.__name__ for x in all_embed_classes]

	# Activity, AttributeAssignment, InformationObject, TransferOfCustody, Move
	# Propositional Object

	vocab.ExternalResource._embed_override = None

	def my_linked_art_boundary_check(self, top, rel, value):
		# True = Embed ; False = Split
		if value._embed_override is not None:
			return value._embed_override
		elif isinstance(value, vocab.LinguisticObject) and hasattr(value, 'classified_as'):
			for ca in value.classified_as:
				if instances['brief text'] in getattr(ca, 'classified_as', []):
					return True
		# Non Statement Linguistic objects might still be internal or external
		# so apply logic from relating properties, not return False
		elif isinstance(value, vocab.ProvenanceEntry):
			return False
		elif isinstance(value, vocab.DigitalObject) and rel in ["digitally_carried_by"]:
			return True
		elif isinstance(value, vocab.LinguisticObject) and rel in ["subject_of"]:
			return True
		elif isinstance(value, vocab.DigitalObject) and rel in ["digitally_shown_by"]:
			return True
		elif isinstance(value, vocab.VisualItem) and rel in ["representation"]:
			return True

		if rel in ["part", "member"]:
			# Downwards, internal simple partitioning
			# This catches an internal part to a LinguisticObject
			return True
		elif rel in ["part_of", 'member_of']:
			# upwards partition refs are inclusion, and always boundary crossing
			return False
		elif value.type in boundary_classes:
			# This catches the external text LinguisticObject
			return False
		elif value.type in embed_classes:
			return True
		else:
			# Default to embedding to avoid data loss
			return True

	setattr(vocab.BaseResource, "_linked_art_boundary_okay", my_linked_art_boundary_check)
	factory.linked_art_boundaries = True

# Default to blank nodes and override as needed
model.factory.auto_assign_id = False
vocab.add_linked_art_boundary_check = _add_linked_art_boundary_check
vocab.add_linked_art_boundary_check()
vocab.set_linked_art_uri_segments()
model.factory.base_url = entity_templates[inst_prefix].replace("{ident}", "")
model.factory.base_dir = output

# Helper functions and global storage
vocab.register_instance('troy ounces', {"parent": model.MeasurementUnit, "id": "300404394", "label": "troy ounces"})
vocab.register_instance('pennyweight', {"parent": model.MeasurementUnit, "id": "300XXXXXX", "label": "pennyweight (dwt)"})
vocab.register_vocab_class("AccessStatement", {"parent": model.LinguisticObject, "id": "300133046", "label": "Access Statement","metatype": "brief text"})
vocab.register_vocab_class("Citation", {"parent": model.LinguisticObject, "id": "300311705", "label": "Citation","metatype": "brief text"})
vocab.register_vocab_class("CreatorDescription", {"parent": model.LinguisticObject, "id": "300435446", "label": "Creator Description","metatype": "brief text"})
vocab.register_vocab_class("CreditLine", {"parent": model.LinguisticObject, "id": "300435418", "label": "Credit Line","metatype": "brief text"})
vocab.register_vocab_class("GalleryLabel", {"parent": model.LinguisticObject, "id": "300048722", "label": "Gallery Label","metatype": "brief text"})
vocab.register_vocab_class("PubCatEntry", {"parent": model.LinguisticObject, "id": "300111999", "label": "Published Catalog Entry","metatype": "brief text"})
vocab.register_vocab_class("CurComment", {"parent": model.LinguisticObject, "id": "300435416", "label": "Curatorial Comment","metatype": "brief text"})


#supertypes
supertypes = {
	"Brass Rubbing": "Visual Works", #9836
	"Drawing & Watercolor": "Visual Works", #16978
	"Drawing & Watercolor-Architectural": "Visual Works", #13172
	"Drawing & Watercolor-Miniature": "Visual Works", #10667
	"Drawing & Watercolor-Sketchbook": "Visual Works", #14852
	"Painting": "Visual Works", #134
	"Photograph": "Visual Works", #36011
	"Poster": "Visual Works", #62720
	"Print": "Visual Works", #17001
	"Ceramic": "Objects", #1489
	"Frame": "Objects", #65155
	"Model": "Objects", #34988
	"Paint Box": "Objects", #6819
	"Painted Object": "Objects", #41366
	"Print-printing-plate": "Objects", #55706
	"Sculpture": "Objects", #16993
	"Silver": "Objects", #38478
	"Wedgewood": "Objects", #none 8/2/2022
	"Manuscript": "Texts", #34440
	"Rare Book": "Texts", #82229
	"ALbum": "Objects" #
}

supertype_uris = {
	"Objects": "http://vocab.getty.edu/aat/300010331",
	"Texts": "http://vocab.getty.edu/aat/300263751",
	"Visual Works": "http://vocab.getty.edu/aat/300191086"
}

unknownUnit = model.MeasurementUnit(ident="urn:uuid:28DE5DAD-CA3A-4424-A3FA-25683637C622", label="Unknown Unit")
instances = vocab.instances

parser = etree.XMLParser(remove_blank_text=True)
nss = {
	"xsi": "http://www.w3.org/2001/XMLSchema-instance",
	"gml": "http://www.opengis.net/gml",
	"lido": "http://www.lido-schema.org"
}

dimtypemap = {"height": vocab.Height, "width": vocab.Width, "depth": vocab.Depth, "weight": vocab.Weight, "diameter": vocab.Diameter,
			  "length": vocab.Length, "thickness": vocab.Thickness, "[not specified]": vocab.PhysicalDimension}
dimunitmap = {"cm": instances['cm'], 'kg': instances['kilograms'], 'lb': instances['pounds'], 'in': instances['inches'], 
			   'mm': instances['mm'], 'g': instances['grams'], 'clock hours': instances['hours'], 'oz': instances['ounces'],
			   'mins': instances['minutes'], 't oz': instances['troy ounces'], 'dwt': instances['pennyweight']}

subjplace_rels = {
	"Made": "production",
	"Manufactured": "production",
	"Assembled": "production",
	"Retailed": "production", #??
	"Designed": "production",
	"or": None,
	"Possibly": None,
	"Place": None,
	"Owned": "owned",
	"Depicted": "depicted",
	"Depicted,": "depicted",
	"Found": "found",
	"Excavated": "found"
}

event_role_rels = {
	"http://vocab.getty.edu/aat/300025103": "carried_out", 
	"http://vocab.getty.edu/aat/300311675": "carried_out",	
	"http://vocab.getty.edu/aat/300025574": "carried_out", 
	"http://vocab.getty.edu/aat/300025633": "carried_out",
	"http://vocab.getty.edu/aat/300403974": "carried_out", # contributor?
	"http://vocab.getty.edu/aat/300025492": "created_by", 
	"http://vocab.getty.edu/aat/300025526": "created_by",
	"http://vocab.getty.edu/aat/300203630": "transferred_title_to",
	"http://collection.britishart.yale.edu/id/concept/63": "exhibition",  # lender / borrower?
	"http://collection.britishart.yale.edu/id/concept/66": "exhibition",
	"http://vocab.getty.edu/aat/300025427": "exhibition"
}

attrib_qual = {
	"attributed to": "http://vocab.getty.edu/aat/300404269", #107
	"formerly": "http://vocab.getty.edu/aat/300404270", #collapse to AAT for formerly attributed to - 120
	"formerly attributed to": "http://vocab.getty.edu/aat/300404270", #55898
	"possibly": "http://vocab.getty.edu/aat/300404272", #12064
	"related to": "?", #23422

}

#note: AAT terms not used in linked art pattern
attrib_qual_group = {
	"workshop of": "http://vocab.getty.edu/aat/300404274",  # not in ycba - 37054
	"studio of": "http://vocab.getty.edu/aat/300404275", #11602
	"atelier of": "http://vocab.getty.edu/aat/300404277",  # not in ycba - No TMS records using "atelier of". We use/it's the same as "studio of"
	"office of": "http://vocab.getty.edu/aat/300404276",  # not in ycba - No TMS records using "office of"
	"manufactory of": "http://vocab.getty.edu/aat/300404281",  # not in ycba - No TMS records using "manufactory of"
	"assistant to": "http://vocab.getty.edu/aat/300404278",  # not in ycba - No TMS records using "assistant to"
	"associate of": "http://vocab.getty.edu/aat/300404280",  # not in ycba - No TMS records using "associate of"
	"school of": "http://vocab.getty.edu/aat/300404284",  # not in ycba - No TMS records using "school of"
	"circle of": "http://vocab.getty.edu/aat/300404283", # 48147
	"follower of": "http://vocab.getty.edu/aat/300404282", # 11599
	"pupil of": "http://vocab.getty.edu/aat/300404279" # 12361
}

#note: AAT terms not used in linked art pattern
attrib_qual_infl = {
	"after": "http://vocab.getty.edu/aat/300404286", # 21887
	"copy after": "http://vocab.getty.edu/aat/300404287",  # AAT technically "copyist of" - 21988
	"style of": "http://vocab.getty.edu/aat/300404285", #12809
	"in the manner of": "http://vocab.getty.edu/aat/300404288", #11387
	"imitator of": "http://vocab.getty.edu/aat/300404288" #same AAT as manner of - 156
}

#those missing AAT terms could be mapped by qual_synonyms
attrib_prod_type = {
	"printed by": " http://vocab.getty.edu/aat/300053319",  # 24976
	"published by": "http://vocab.getty.edu/aat/300054686",  # 24976
	"etched by": "http://vocab.getty.edu/aat/300053241", # 26108
	"engraved by": "http://vocab.getty.edu/aat/300053225", #26108
	"commissioned by": "http://vocab.getty.edu/aat/300393199", #40556
	"text by": "http://vocab.getty.edu/aat/300054698", #21787
	"annotated by": "", #56003
	"drawn by": "", #22236
	"etched and engraved by": "", #26252
	"figure(s) by": "", #238
	"sketched by": "", #75748
	"aquatinted by": "http://vocab.getty.edu/aat/300053242", #44330
	"verso by": "http://vocab.getty.edu/aat/300010292", #11972
	"completed by": "", #29334
	"landscape by": "", #238
	"modeled by": "http://vocab.getty.edu/aat/300053130", #39100
	"produced by": "", #
	"color printing by": "", #
	"colored by": "",
	"drawing on plate by": "",
	"commenced by": "",
	"corrected by": "",
	"designed by": "",
	"alterations by": "",
	"background by": "",
	"painted by": "",
	"sold by": "",
	"flower basket and some of the draperies by": "",
	"facimile color by": "",
	"Identifying sitters in a painting by": ""
}

#these could be fixed in TMS, or remain in TMS with normalization mapping here for LUX
qual_synonyms = {
	"(?)": "possibly",
	"after ?": "possibly after",
	"previously": "formerly",
	"prints made by": "engraved by",
	"center engraved by": "engraved by",
	"perhaps": "possibly",
	"after (?)": "possibly after",
	"engraving by": "engraved by",
	"previously published by": "formerly published by",
	"copy of print by": "printed by",
	"previously attributed to": "formerly attributed to",
	"after possibly": "possibly after",
	"published:": "published by",
	"copy of": "copy after",
	"?": "possibly",
	"formerly attributed:": "formerly attributed to",
	"after?": "possibly after",
	"possibly by": "possibly",
	"formerly print made by": "formerly engraved by",
	"(formerly) after": "formerly after",
	"probably": "possibly",
	"published:": "published by",
	"print finished by": "print completed by",
	"commissioned in 1732 by": "commissioned by",
	"finished by": "completed by",
	"student of the": "pupil of",
	"cicle of (?)": "possibly circle of",
	"print made by imitator of": "printed by imitator of",
	"engravings by": "engraved by",
	"drawing by": "drawn by",
	"figures by": "figure(s) by",
	"figure by": "figure(s) by",
	"also (?)": "",
	"and": "",
	"or": "",
	"and:": "",
	"and / or": "",
	"annotations by": "annotated by",
	"and one etching by": "etched by",
	"annotated": "annotated by,",
	"print possibly made by": "possibly printed by",
	"printed by the": "printed by",
	"recto: print made by": "recto printed by",
	"upper print made by": "upper printed by",
	"student of": "pupil of",
	"[": "",
	"for": "",
	"etching by": "etched by",
	"copy of print made by": "copy of print after",
	"design": "designed by",
	"design by": "designed by",
	"in the manner": "in the manner of",
	"formerly:": "formerly",
}

#suppress list to suppress long term loans
suppress = [51893,56207]

new_rels = {}
for (k,v) in event_role_rels.items():
	new_rels[map_uuid_uri(k)] = v
event_role_rels.update(new_rels)

NAMEDB = LMDB('ycba_name_db', open=True)

missing_materials = {}
missing_techniques = {}
missing_subjects = {}

processed = []
newactivities = []
updatedactivities = []
def serialize_method(serialize_array):
	for record in serialize_array:
		if record.id[9:] in processed:
			continue
		#print(f"Record:{record.id}")
		outdir = os.path.join(model.factory.base_dir, record._uri_segment, record.id.split("/")[5])
		pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
		outfn = os.path.join(outdir, record.id.split("/")[6])
		# Process immediately before serializing

		#rewrite_crom_ids(record) #try to do w/o this method
		if not hasattr(record, 'identified_by'):
			print(f"{factory.toString(record, compact=False)}")
			print(f"No name/identifier for {outfn}")
			#raise ValueError()

		record_status = "same"

		tmsid = ""
		if "HumanMadeObject" in str(type(record)):
			if "SystemNumber" in str(record.identified_by):
				tmsid = record.identified_by[0].content
		#print (f'ID:{tmsid}')

		if not path.exists(outfn):
			model.factory.toFile(record, compact=False, filename=outfn)
			record_status = "New"
			ts = time.time()
			timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
			print(f"New    {record.id} {timestamp} {record._uri_segment}")
			newactivities.append((record.id.split("/")[6].replace(".json",""), record._uri_segment, record_status, timestamp, timestamp,tmsid))
		else:
			checkfn = os.path.join(model.factory.base_dir, "checkrecord.json")
			model.factory.toFile(record, compact=False, filename=checkfn)
			same = filecmp.cmp(outfn, checkfn)
			if not same:
				model.factory.toFile(record, compact=False, filename=outfn)
				record_status = "Update"
				ts = time.time()
				timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
				print(f"Update    {record.id} {timestamp} {record._uri_segment}")
				updatedactivities.append((record_status,timestamp,record.id.split("/")[6].replace(".json","")))
			else:
				ts = time.time()
				timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
				print(f"Same    {record.id} {timestamp} {record._uri_segment}")
		processed.append(record.id[9:])
		DB.commit()
		NAMEDB.commit()
def print_elm(elm):	
	print(etree.tostring(elm, pretty_print=True).decode('utf-8'))	

def make_concept(conc, clss=model.Type):
	# pass in the parent of lido:conceptID, lido:term and make a Type with ident and label
	cid = conc.xpath('./lido:conceptID', namespaces=nss)
	lbl = conc.xpath('./lido:term/text()', namespaces=nss)
	cidtype = conc.xpath('./lido:conceptID/@lido:type', namespaces=nss)
	if not cid and not lbl:
		# Nothing to make
		return None
	if cidtype[0] == "No ObjectWorkType for Record":
		return None
	cid_index = 0
	loop = "true"
	while (loop=="true"):
		try:
			uri = get_concept_uri(cid[cid_index]) if cid else "auto uuid"
		except:
			loop = "false"
		if not uri:
			cid_index +=1
		else:
			loop = "false"
	if not uri and ((not lbl) or lbl[0] == "not selected"):
		return None
	lbl = lbl[0] if lbl else ""
	if uri and lbl != "" and uri.startswith('http://collection.britishart.yale.edu/id'):
		#print(f'uri:{uri}')
		to_serialize.append(get_concept_from_term(lbl, True))
		return get_concept_from_term(lbl,False)
	if uri == "auto uuid" and lbl and f"concept:{lbl}" in NAMEDB:
		uri = NAMEDB[f"concept:{lbl}"]
	t = clss(ident=uri, label=lbl)
	if uri == "auto uuid" and lbl:
		# assign a UUID to the label
		NAMEDB[f"concept:{lbl}"] = t.id
		NAMEDB[t.id] = f"concept:{lbl}"
		NAMEDB.commit()
	return t

def get_concept_uri(f, clss=None, map_to_uuid=False):
	t = f.text 
	if not t or t in ["-1", '0']:
		return None
	vocab = f.attrib.get('{http://www.lido-schema.org}source', '').strip().lower()

	if vocab in ['aat', 'tgn', 'ulan', 'viaf', 'fast', 'wikidata']:
		t = t.replace('ID:', '')
		t = t.replace('ID', '')
		t = t.strip()

	if vocab == "aat" and len(t) < 9: 
		t = f"3{t.rjust(8,'0')}"
	if vocab == "local" and len(t) == 9 and t.startswith('50') and t.isdigit():
		# looks like a ulan number in the wrong vocab :P
		vocab = "ulan"
	elif vocab == "local" and len(t) == 9 and t.startswith('30') and t.isdigit():
		vocab = "aat"

	if vocab in ['local', 'ycba'] and clss:
		t = f"{clss}/{t}"

	# Now check if we're mapped, if so, return the UUID
	# ERJ 12/4 is the below right? sometimes returning uri, something returning uri
	if not map_to_uuid and vocab in entity_templates:
		return entity_templates[vocab].format(ident=t)
	elif vocab in entity_templates:
		uu = map_uuid(vocab, t)
		# XXX This could look up the class but can rely on the model to inform
		return uu
	elif t.startswith('http'):
		return t
	elif vocab in ['lc call number', 'oclc number']:
		return None
	elif vocab in ['ycba tms bibliographic module record referenceid']:
		t = f"bibid/{t}"
		uu = map_uuid('ycba', t)
		return uu
	else:
		print(f"Cannot assign a URI for {vocab}:{t}")
		return None

def make_object_concept(conc, clss=model.Type):
	#based on make_concept for subjectObjects with different LIDO scheme than subjectConcept
	#http://lido-schema.org/schema/v1.0/lido-v1.0-specification.pdf
	cid = conc.xpath('./lido:object/lido:objectID', namespaces=nss)
	lbl = conc.xpath('./lido:displayObject', namespaces=nss)
	if not cid and not lbl:
		# Nothing to make
		return None
	cid_index = 0
	loop = "true"
	while (loop=="true"):
		uri = get_concept_uri(cid[cid_index]) if cid else "auto uuid"
		if not uri:
			cid_index +=1
		else:
			loop = "false"
	if not uri and ((not lbl) or lbl[0] == "not selected"):
		return None
	lbl = lbl[0].text.rstrip() if lbl else ""
	if uri and lbl != "" and uri.startswith('http://collection.britishart.yale.edu/id'):
		#print(f'uri:{uri}')
		to_serialize.append(get_concept_from_term(lbl, True))
		return get_concept_from_term(lbl, False)
	if uri == "auto uuid" and lbl and f"concept:{lbl}" in NAMEDB:
		uri = NAMEDB[f"concept:{lbl}"]
	t = clss(ident=uri, label=lbl)
	if uri == "auto uuid" and lbl:
		# assign a UUID to the label
		NAMEDB[f"concept:{lbl}"] = t.id
		NAMEDB[t.id] = f"concept:{lbl}"
		NAMEDB.commit()
	return t

def make_datetime(txt):
	if txt in ['0', '9999']:
		# this means "ongoing" (beginning/end)
		return None
	else:
		try:
			begin, end = date_cleaner(txt)
		except:
			raise ValueError(txt)
			return None
		try:			
			return (begin.isoformat()+"Z", end.isoformat()+"Z")
		except:
			return ["{begin}-01-01T00:00:00Z", "{end}-01-01T00:00:00Z"]


def file_exists(clss, uu):
	uu = uu.replace('urn:uuid:', '')
	fn = os.path.join(model.factory.base_dir, clss, uu[:2], f"{uu}.json")
	return os.path.exists(fn)


actor_id_source = {}

def make_qualified_group_actor(a,aqa,actor):
	idents = []
	ids = a.xpath('./lido:actorID', namespaces=nss)
	for aid in ids:
		if not aid.text:
			continue
		val = aid.text.strip()
		if val == "ycba_actor_1281":
			return (None, None)
		src = aid.attrib.get('{%s}source' % nss['lido'], None)
		if src:
			src = src.lower()
			if src in ["tms", "local"]:
				src = "ycba"
		typ = aid.attrib.get('{%s}type' % nss['lido'], None)
		if not (typ, src, val) in idents:
			idents.append((typ, src, val))
	idents.sort()
	uu = map_uuid("ycba", f"actor/qualgroup_{idents[0][2]}")
	pclss = model.Group
	who = pclss(ident=urn_to_url_json(uu, "group"))
	names = a.xpath('./lido:nameActorSet/lido:appellationValue', namespaces=nss)
	for n in names:
		if not n.text:
			continue
		pref = n.attrib.get('{%s}pref' % nss['lido'], None)
		val = n.text.strip()
		if val:
			val = f"{aqa.capitalize()} {val}"
			if pref == "preferred":
				who.identified_by = vocab.PrimaryName(value=val)
				who._label = val
			else:
				who.identified_by = model.Name(value=val)
				if not hasattr(who, '_label'):
					who._label = val
	who.classified_as = model.Type(ident=attrib_qual_group.get(aqa,"http://collection.britishart.yale.edu/qualifier/outsideofvocab"), label=aqa)
	if not hasattr(who, 'formed_by'):
		b = model.Formation()
		who.formed_by = b
	who.formed_by.influenced_by = actor
	return who

def make_actor(a, source=""):
	
	# source: object, life, pub, subject
	# Use in this order of precedence
	idents = []
	ids = a.xpath('./lido:actorID', namespaces=nss)
	for aid in ids:
		if not aid.text:
			continue
		val = aid.text.strip()
		if val == "ycba_actor_1281" and source != "acq" and source != "exh":
			return (None,None)
		src = aid.attrib.get('{%s}source' % nss['lido'], None)
		if src:
			src = src.lower()
			if src in ["tms", "local"]:
				src = "ycba"		
		typ = aid.attrib.get('{%s}type' % nss['lido'], None)
		if not (typ, src, val) in idents:
			idents.append((typ, src, val))
	idents.sort()  # this will sort local to the beginning

	uu = None
	who_info = None
	done_src = None
	for (typ, src, val) in idents:
		if typ in ['local', 'subjectActor']:
			# ycba : actor/nnnn
			# print(f"{typ} / {src} / {val}")
			if src:
				if src == "aat" and val in ['1281', '1253']:
					src = 'ycba'
				if src == "wikidata" and val.startswith('http'):
					# Strip to Q
					rsl = val.rfind('/')
					val = val[rsl+1:]
				if src in ['ycba', 'local']:
					if not val in ["-1", "0"]:
						uu = map_uuid(src, f"actor/{val}", automap=False)
				elif src == "history of parliament":
					sep = "/"
					val = sep.join(val.split("/")[3:7])
					uu = map_uuid("hponline", val)
				elif val:
					try:
						uu = map_uuid(src, val, automap=False)
					except:
						print(f"Bad identifier {val} for {src}")
				if uu:
					break
		elif typ == "url" and val:
			if val.startswith('http://vocab.getty.edu/page/'):
				val = val.replace('page/', '')
			try:
				uu = map_uuid_uri(val, automap=False)
			except:
				print(f"Bad URI: {val}")
				pass
			if uu:
				break
	if uu:
		who_info = DB[uu]
		done_src = actor_id_source.get(uu, None)
	else:
		# create one
		if len(idents) != 1:
			print(f"Create for multiple {idents}")
			done = 0
			for i in idents:
				try:
					uu = map_uuid(i[1], i[2], automap=False)
					if uu:
						print(f"Found: {uu} for {i[2]}")
						done = 1
						break
				except:
					pass
			if not done and idents:
				# create with mappings to the other ids
				uu = map_uuid('ycba', f'actor/{idents[0][2]}')
				info = {'class': 'Person', 'ycba': f"actor/{idents[0][2]}", "ulan": None, "wikidata": None, "viaf": None}
				for i in idents:
					if i[1] == "wikidata":
						wid = validate_wikidata(i[2])
						if wid:
							info['wikidata'] = wid
							DB[f'wikidata:{wid}'] = uu
					elif i[1] == "ulan":
						uid = validate_ulan(i[2])
						if uid:
							info['ulan'] = uid
							DB[f'ulan:{uid}'] = uu
					else:
						print(f"Unknown id source {i[1]} in {idents}")
				DB[uu] = info
			elif not done:
				# No ident should map using name
				uu = f"urn:uuid:{uuid.uuid4()}"
		else:
			uu = map_uuid("ycba", f"actor/{idents[0][2]}")

		DB.commit()

	if done_src:
		if source == "subject":
			# No more data than we already have
			pass
		elif source == "pub" and done_src == "subject":
			# print("pub > subject")
			done_src = None
		elif source == "life" and done_src in ['pub, subject']:
			# print(f"life > {done_src}")
			done_src = None
		elif source == "object" and done_src != "object": 
			done_src = None
			# print(f"object > {done_src}")

	if done_src is None:
		atype = a.xpath('./@lido:type', namespaces=nss)
		if atype:
			atype = atype[0].lower()
		else:
			atype = None
		# person, organization, institution, corporation, (empty)

		if atype in ['person','constituent']:
			pclss = model.Person
			who = pclss(ident=urn_to_url_json(uu, "person"))
		elif atype in ['organization', 'institution', 'corporation']:
			pclss = model.Group
			who = pclss(ident=urn_to_url_json(uu, "group"))
		else:
			# print(f"no type on actor: {label}")
			# in YCBA, these seem to be mostly groups
			# XXX Make a list from the raw data
			pclss = model.Group
			who = pclss(ident=urn_to_url_json(uu, "group"))
		# Cache class

		if not who_info:
			who_info = {}
		who_info['class'] = pclss.__name__
		if uu:
			DB[uu] = who_info
	else:
		print("BADHAPPENING")
		pclss = getattr(model, who_info['class'])
		# break out and use existing, make this just a reference
		return (pclss(ident=uu), "cached")

	# Now we can just construct the object without checking new data

	for (typ, src, val) in idents:
		if typ == 'local':
			local = model.factory.base_url + 'actor/' + val
			ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281")  # Yale Center for British Art by TMS con ID
			ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"), label="Yale Center for British Art")
			att_ass = model.AttributeAssignment()
			att_ass.carried_out_by = ycbagroup
			sysnum = vocab.SystemNumber(value=val)
			sysnum.assigned_by = att_ass
			who.identified_by = sysnum
			#who.identified_by = vocab.SystemNumber(value=val)
		elif typ == 'url':
			who.equivalent = pclss(ident=val)
		elif typ in ["subjectActor","subjectActorAbout"]:
			if src in entity_templates:
				if val.startswith('http'):
					who.equivalent = pclss(ident=val)
				else:
					url = entity_templates[src].format(ident=val)
					who.equivalent = pclss(ident=url)
		else:
			print(f"no typ {typ} for actorID: {val}")

	names = a.xpath('./lido:nameActorSet/lido:appellationValue', namespaces=nss)
	for n in names:
		if not n.text:
			continue

		pref = n.attrib.get('{%s}pref' % nss['lido'], None)
		val = n.text.strip()
		if val:
			if pref == "preferred":
				who.identified_by = vocab.PrimaryName(value=val)
				who._label = val
			else:
				who.identified_by = model.Name(value=val)
				if not hasattr(who, '_label'):
					who._label = val
	nationalityConcepts = a.xpath('./lido:nationalityActor/lido:conceptID', namespaces=nss)
	for nc in nationalityConcepts:
		src = nc.xpath('./@lido:source', namespaces=nss)
		typ = nc.xpath('./@lido:type', namespaces=nss)
		label = nc.xpath('./@lido:label', namespaces=nss)
		txt = nc.xpath('./text()', namespaces=nss)
		if len(src) == 0:
			continue
		else:
			src = src[0]
		if len(typ) == 0:
			continue
		else:
			typ = typ[0]
		if len(label) == 0:
			continue
		else:
			label = label[0]
		if len(txt) == 0:
			continue
		else:
			txt = txt[0]

		#get TGN conceptIDs in nationality Actor to get places related to actor (birth place, death place, place of activity)
		if src == "TGN":
			src = src.lower()
			uu = DB[f"{src}:{txt}"]
			if uu:
				outdir = os.path.join(model.factory.base_dir, "place", uu[uu.rfind(':')+1:uu.rfind(':')+3])
				pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
				outfn = os.path.join(outdir, uu[uu.rfind(':')+1:] + ".json")
				where = model.Place(ident=urn_to_url_json(uu, "place"), label=label)
				where.equivalent = model.Place(ident=f"http://vocab.getty.edu/tgn/{txt}")
				if not path.exists(outfn):
					print(f"Warning: {outfn} does't exist on disk, need to serialize")
					to_serialize.append(where)
					#continue
				#where = model.Place(ident=urn_to_url_json(uu, "place"), label=label)
			else:
				uu = f"urn:uuid:{uuid.uuid4()}"
				DB[uu] = {src: txt}
				DB[f"{src}:{txt}"] = uu
				DB.commit()
				where = model.Place(ident=urn_to_url_json(uu, "place"), label=label)
				where.equivalent = model.Place(ident=f"http://vocab.getty.edu/tgn/{txt}")
				to_serialize.append(where)

			pltyp = typ.lower()
			if who.type == "Person":
				if pltyp == "birth place":
					if not hasattr(who, 'born'):
						who.born = model.Birth()
					who.born.took_place_at = where
				elif pltyp == "death place":
					if not hasattr(who, 'died'):
						who.died = model.Death()
					who.died.took_place_at = where
				elif pltyp == "place of baptism":
					baptismAct = model.Activity()
					who.participated_in = baptismAct
					baptismAct.took_place_at = where
					baptismAct.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300069030", label="Baptism")
				elif pltyp == "place of activity":
					activityAct = model.Activity()
					who.participated_in = activityAct
					activityAct.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300393177",
															   label="Professional Activities")
					activityAct.took_place_at = where
				elif pltyp == "place of visit/tour":
					tourAct = model.Activity()
					who.participated_in = tourAct
					tourAct.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300162867", label="Journeys")
					tourAct.took_place_at = where
				elif pltyp == "place lived":
					who.residence = where
			elif who.type == "Group":
				if pltyp == "founded":
					who.formed_by.took_place_at = where
				elif pltyp == "location":
					who.residence = where

	nationality = a.xpath('./lido:nationalityActor/lido:term/text()', namespaces=nss)
	# No ConceptIDs for nationality
	# and lots of free text eg "German, active in Britain (from 1676)"
	# XXX Parse this into nationality concepts
	if nationality:
		val = nationality[0]
		what = val.split()[0].lower()
		if what[-1] == ",":
			what = what[:-1]
		n = f"{what} nationality"
		if n in vocab.instances:
			natl = vocab.instances[n]
			who.classified_as = natl
		else:
			# print(f"Nationality not found: {what} = ({val} --> BiographyStatement)")
			who.referred_to_by = vocab.BiographyStatement(content=val)

	birthDate = a.xpath('./lido:vitalDatesActor/lido:earliestDate/text()', namespaces=nss)
	if birthDate:
		try:
			date = make_datetime(birthDate[0])
		except:
			# print(f"{fn} | birth | {birthDate[0]}")
			date = None
		if date:
			if who.type == "Person":
				if not hasattr(who, 'born'):
					b = model.Birth()
					who.born = b
				ts = model.TimeSpan()
				who.born.timespan = ts
				ts.begin_of_the_begin = date[0]
				ts.end_of_the_end = date_time_minus_one_second(date[1])
				birthDateTyp = a.xpath('./lido:vitalDatesActor/lido:earliestDate/@lido:type', namespaces=nss)[0]
				if birthDateTyp == "estimatedDate":
					ts.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300054617", label="Estimated")
			elif who.type == "Group":
				if not hasattr(who, 'formed_by'):
					b = model.Formation()
					who.formed_by = b
				ts = model.TimeSpan()
				who.formed_by.timespan = ts
				ts.begin_of_the_begin = date[0]
				ts.end_of_the_end = date_time_minus_one_second(date[1])

	deathDate = a.xpath('./lido:vitalDatesActor/lido:latestDate/text()', namespaces=nss)
	if deathDate:
		try:
			date = make_datetime(deathDate[0])
		except ValueError:
			# print(f"{fn} | death | {deathDate[0]}")
			date = None

		if date and date[0] < "2020":
			if who.type == "Person":
				if not hasattr(who, 'died'):
					d = model.Death()
					who.died = d
				ts = model.TimeSpan()
				who.died.timespan = ts
				ts.begin_of_the_begin = date[0]
				ts.end_of_the_end = date_time_minus_one_second(date[1])
				deathDateTyp = a.xpath('./lido:vitalDatesActor/lido:latestDate/@lido:type', namespaces=nss)[0]
				if deathDateTyp == "estimatedDate":
					ts.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300054617", label="Estimated")
			elif who.type == "Group":
				if not hasattr(who, 'dissolved_by'):
					d = model.Dissolution()
					who.dissolved_by = d
				ts = model.TimeSpan()
				who.dissolved_by.timespan = ts
				ts.begin_of_the_begin = date[0]
				ts.end_of_the_end = date_time_minus_one_second(date[1])

	gender = a.xpath('./lido:genderActor/text()', namespaces=nss)
	if gender:
		if gender[0] == "male": 
			who.classified_as = vocab.instances['male']
		elif gender[0] == "female": 
			who.classified_as = vocab.instances['female']
		elif gender[0] in ["unknown", 'undetermined']:
			pass
		else:
			print(f"Unknown gender: {gender[0]}")

	# if 0:
	# 	# These just come from ULAN, skip and re-import in merge
	# 	lifeRoles = a.xpath('./lido:actorInRole/lido:roleActor[./lido:conceptID[@lido:type="Life role"]]', namespaces=nss)
	# 	for lr in lifeRoles:
	# 		t = make_concept(lr)
	# 		if not t:
	# 			continue
	# 		done = False
	# 		tid = "urn:uuid:" + aat_uuids.get(t.id[-9:], t.id)
	# 		if hasattr(who, 'carried_out'):
	# 			for act in who.carried_out:
	# 				#print(f"{tid} in {[x.id for x in act.classified_as]}")
	# 				for x in act.classified_as:
	# 					if tid == x.id or (not x.id and t._label == x._label):
	# 						done = True
	# 						break
	# 		if not done:
	# 			if exists and filename != who._filename:
	# 				print(f" --- Matched a new activity for {who.id} / {who._label}")
	# 			act = vocab.Active()
	# 			act.classified_as = t
	# 			who.carried_out = act	

	return (who, "serialize")

def make_place(elm, localid=None,issite=False):
	# take a lido:place element and make a Place
	# Check if we already have the place

	if not elm.getchildren():
		return (None, "cached")

	ids = elm.xpath('./lido:placeID', namespaces=nss)
	if not ids and elm.getparent().tag.endswith('eventPlace'):
		# This SHOULD be in place/placeID, but isn't for eventPlace for both units
		ids = elm.xpath('./lido:placeClassification/lido:conceptId', namespaces=nss)

	if localid:
		ids_to_check = [localid]
	else:
		ids_to_check = []
	for i in ids:
		# Some are full URIs to begin with
		if not i.text:
			continue
		if i.text.startswith('http'):
			pi = i.text.strip()
			ids_to_check.append(pi)
		else:
			uri = get_concept_uri(i, map_to_uuid=False)
			if uri:
				ids_to_check.append(uri)

	# And check ids
	uu = None
	prefid = None
	for i2c in ids_to_check:
		uu = map_uuid_uri(i2c, automap=False)
		#if uu and file_exists('place', uu):
			#return (model.Place(ident=uu), "cached")
		if uu:
			break
		if i2c.startswith('http://vocab.getty.edu/'):
			prefid = i2c
		elif i2c.startswith('https://www.wikidata.org/') and not prefid:
			prefid = i2c
		elif i2c.startswith('http://sws.geonames.org/') and not prefid:
			prefid = i2c

	names_to_check = []
	names = elm.xpath('./lido:namePlaceSet/lido:appellationValue', namespaces=nss)
	plbl = ""
	for n in names:
		pref = n.attrib.get('{%s}pref' % nss['lido'], None)
		val = str(n.text).strip()
		if val:
			if (pref and pref[0] == "preferred") or len(names) == 1:
				plbl = val
				names_to_check.append(('primary', val))
			else:
				if not plbl:
					plbl = val
				names_to_check.append(('', val))

	if not uu:
		prefname = ''
		for t, n2c in names_to_check:
			if f"place:{n2c}" in NAMEDB:
				# print(f"Found place - {n2c}")
				uu = NAMEDB[f"place:{n2c}"]
				#
				#if file_exists('place', uu):
					#return (model.Place(ident=uu), "cached")
			if t == 'primary':
				prefname = n2c
			elif not prefname:
				prefname = n2c


		if prefid:
			uu = map_uuid_uri(prefid)
		elif uu:
			# Assigned from n2c
			pass
		else:
			# No useful identifier, no pre-established uuid for name ...
			# print(f"No identifier for {names_to_check}")
			uu = f"urn:uuid:{uuid.uuid4()}"
			NAMEDB[f"place:{prefname}"] = uu
			NAMEDB[uu] = f"place:{prefname}"
			NAMEDB.commit()
	where = model.Place(ident=urn_to_url_json(uu,"place"))
	if issite == True:

		where.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300240057", label="Gallery (place)")
	for i2c in ids_to_check:
		if i2c != localid:
			where.equivalent = model.Place(ident=i2c)
	
	if hasattr(where, 'identified_by'):
		names = [x.content for x in where.identified_by if x.type == "Name"]
	else:
		names = []

	for t, n2c in names_to_check:
		if not n2c in names:
			if t == "primary":
				where.identified_by = vocab.PrimaryName(value=n2c)
			else:
				where.identified_by = model.Name(value=n2c)

	lbl = elm.xpath('../lido:displayPlace/text()', namespaces=nss)
	if plbl:
		where._label = plbl
	elif not names and lbl:
		# assign the display label as a last resort
		where._label = lbl[0]
		where.identified_by = model.Name(value=lbl[0])

	coords = elm.xpath('./lido:gml/gml:Point/gml:coordinates/text()', namespaces=nss)
	if coords:
		coords = coords[0].strip()
		coords = coords.replace(',', '')
		coords = coords.split(' ')
		coords = [float(coords[1]), float(coords[0])]
		# skip over null island
		if coords[0] != 0 and coords[1] != 0:
			geojson = {'features': [{'geometry': {'coordinates': coords, 'type': 'Point'}, 'type': 'Feature'}], 'type': 'FeatureCollection'}
			where.defined_by = json.dumps(geojson)

	# thankfully placeClassification is not used elsewhere, otherwise we would process it here
	return (where, "serialize")

def lookup_or_map(key):
	if DB[key]:
		uu = DB[key]
	else:
		uu = f"urn:uuid:{uuid.uuid4()}"
		DB[key] = uu
		DB.commit()
	return uu

def record_new_activities():
		sql = "INSERT INTO activity (ycbaluxid,entitytype,statustype,created,updated,cmsid) VALUES (%s,%s,%s,%s,%s,%s)"
		#print(newactivities)
		cursor_act.executemany(sql, newactivities)
		db_act.commit()
def record_updated_activities():
	sql = "UPDATE activity SET statustype = %s,updated = %s WHERE ycbaluxid = %s"
	#print(updatedactivities)
	cursor_act.executemany(sql, updatedactivities)
	db_act.commit()

def urn_to_url_json(s1,typ1):
	#print(f"urn_to_url:{s1}")
	#s4 = "https://lux.britishart.yale.edu/"
	s4 = "https://ycba-lux.s3.amazonaws.com/v3/"
	a = s1.split(":")
	s2 = a[2]
	s3 = s2[:2]
	s4 = s4 + typ1 + "/" + s3 + "/" + s2 + ".json"
	return s4

def date_time_minus_one_second(s):
	t = datetime.datetime.strptime(s,'%Y-%m-%dT%H:%M:%SZ')
	t2 = t - datetime.timedelta(seconds=1)
	s2 = t2.strftime('%Y-%m-%dT%H:%M:%SZ')
	return s2

def get_concept_from_term(term,boundary):
	conceptuu = map_uuid("ycba", f"concept/{term.replace(' ','_')}")
	concepttype = model.Type(ident=urn_to_url_json(conceptuu,"concept"), label = term)
	if boundary==True:
		concepttype.identified_by = vocab.PrimaryName(value=term)
	return concepttype

sets = {
	"ycba:ps": "Paintings and Sculpture Collection, Yale Center for British Art",
	"ycba:pd": "Prints and Drawings Collection, Yale Center for British Art",
	"ycba:frames": "Frames Collection, Yale Center for British Art"
}
serialize_global = []
sets_model = {}
for (k,v) in sets.items():
	setuu = map_uuid("ycba", f"set/{k}")
	setobj = vocab.Set(ident=urn_to_url_json(setuu,"set"))
	setident = model.Identifier(value=k)
	ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281")  # Yale Center for British Art by TMS con ID
	ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"), label="Yale Center for British Art")
	att_ass = model.AttributeAssignment()
	att_ass.carried_out_by = ycbagroup
	setident.assigned_by = att_ass
	setobj.identified_by = setident
	setobj.identified_by = model.Name(value=v)
	setobj._label = v
	sets_model[k] = setobj
	setgroupuu = map_uuid("ycba", f"actor/ycba_actor_{k}")
	setgroup = model.Group(ident=urn_to_url_json(setgroupuu, "group"), label=v.replace("Collection","Department"))
	setgroupname= model.Name(value=v.replace("Collection","Department"))
	setgroup.identified_by = setgroupname
	setgroup.member_of = ycbagroup
	setgroup.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300263534", label="Department")
	curation_act = model.Activity(label="Curating")
	curation_act.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300054277", label="Curating")
	if k == "ycba:frames":
		framesgroupuu = map_uuid("ycba", f"actor/ycba_actor_ycba:ps")
		framesgroup = model.Group(ident=urn_to_url_json(framesgroupuu, "group"), label="Paintings and Sculpture Department, Yale Center for British Art")
		curation_act.carried_out_by = framesgroup
	else:
		curation_act.carried_out_by = setgroup
	setobj.used_for = curation_act
	setobj.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300025976", label="Collection")
	serialize_global.append(setobj)
	serialize_global.append(setgroup)
serialize_method(serialize_global)

# local:1281 and local:1253 are both YCBA, which is ULAN/500303557
site_uu = map_uuid('ycba', 'place/site')
NAMEDB['place:Yale Center for British Art'] = site_uu
ycba_uuid = map_uuid('ulan', '500303557')
ycba_info = DB[ycba_uuid]
if not 'ycba' in ycba_info:
	DB['ycba:actor/1281'] = ycba_uuid
	DB['ycba:actor/1253'] = ycba_uuid
	ycba_info['ycba'] = ['actor/1281', 'actor/1253']
	ycba_info['class'] = "Group"
	DB[ycba_uuid.replace('urn:', '')] = ycba_info
	DB.commit()


# Process LIDO files

fileidx = 0
files = os.listdir(source)
files.sort(key=lambda x: int(x[:-4]))

# connect to activity db
f=open("t.properties","r")
lines=f.readlines()
pw_from_t=lines[1]
dbschema=lines[2]
f.close()
db_act = pymysql.connect(host = "spinup-db0017cd.cluster-c9ukc6s0rmbg.us-east-1.rds.amazonaws.com",
					 user = "admin",
					 password = pw_from_t.strip(),
					 database = dbschema.strip())
cursor_act = db_act.cursor()

# Process LIDO from db
f=open("t.properties","r")
lines=f.readlines()
pw_from_t=lines[0]
f.close()
db = pymysql.connect(host = "oaipmh-prod.ctsmybupmova.us-east-1.rds.amazonaws.com",
					 user = "oaipmhuser",
					 password = pw_from_t.strip(),
					 database = "oaipmh")
cursor = db.cursor()

if config1 == "test":
	sql = "select local_identifier, xml from metadata_record where local_identifier in (34) and status != 'deleted' order by cast(local_identifier as signed) asc"
	#sql = ""
else:
	sql = "select local_identifier, xml from metadata_record where status != 'deleted' order by cast(local_identifier as signed) asc"
lido = []
ids = []

try:
	cursor.execute(sql)
	results = cursor.fetchall()
	for row in results:
		ids.append(row[0])
		lido.append(row[1])
except:
	print("Error: unable to fetch LIDO data")

if config1 == "test":
	#sql = "SELECT local_identifier,set_spec FROM record_set_map where local_identifier in (34,107,5005,38526,17820,22010,22023,425,11602,82154) order by cast(local_identifier as signed) asc"
	sql = "SELECT local_identifier,set_spec FROM record_set_map where local_identifier in (34) order by cast(local_identifier as signed) asc"
else:
	sql = "SELECT local_identifier,set_spec FROM record_set_map order by cast(local_identifier as signed) asc"
id_and_set = {}
try:
	cursor.execute(sql)
	results = cursor.fetchall()
	for row in results:
		if row[1] != "ycba:incomplete":
			id_and_set[row[0]] = row[1]
except:
	print("Error: unable to fetch SET data")
db.close()

cnt = -1
for doc in lido:
	cnt += 1
	fn = ids[cnt]
	try:
		set = id_and_set[fn]
	except:
		print(f"ERROR finding set for {fn}")
		continue

	if int(fn) in suppress:
		print(f"INFO per list suppressing processing of {fn}")
		continue

	#aeon variables
	aeonSet= set
	aeonLabel = "Accessible by request in the study Room"
	aeonHost = "https://aeon-mssa.library.yale.edu/aeon.dll?"
	aeonAction = "10"
	aeonForm = "20"
	aeonValue = "GenericRequestPD"
	aeonSite = "YCBA"
	aeonCallNumber = ""
	aeonItemTitle = ""
	aeonItemAuthor = []
	aeonItemDate = ""
	aeonFormat = ""
	aeonLocation = ""
	aeonMfhdID = ""
	aeonEADNumber = f"https://collections.britishart.yale.edu/catalog/tms:{fn}"
	#print("SEE BELOW")
	print(fn)
	#print(doc)
	#print(type(doc))
	#print(set)
	doc_str = ''.join(doc)
	#dom = etree.parse(doc_str, parser)
	try:
		dom = etree.fromstring(doc_str,parser)
	except Exception as e:
		print(f"ERROR parsing doc {fn} Exception {e}")
		continue
	to_serialize = []

	# <lido:lidoRecID lido:source="Yale Center for British Art" lido:type="local">YCBA/lido-TMS-17</lido:lidoRecID>
	fields = dom.xpath(f'{wrap}/lido:lido/lido:lidoRecID', namespaces=nss)
	t = None
	#provEntry = None
	for f in fields:
		t = f.text 
		# Strip to only trailing integer
		if '-' in t:
			t = t[t.rfind('-')+1:]

	wid = lookup_or_map(f"ycba:object/{t}")
	#note passing AUTOURI as ident to models generates a new autouri
	what = model.HumanMadeObject(ident=urn_to_url_json(wid,"object"))
	id1 = t
	if t:
		ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281")  # Yale Center for British Art by TMS con ID
		ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"), label="Yale Center for British Art")
		att_ass = model.AttributeAssignment()
		att_ass.carried_out_by = ycbagroup
		sysnum = vocab.SystemNumber(label="Local System Number", value=t)
		sysnum.assigned_by = att_ass
		what.identified_by = sysnum

	to_serialize.append(what)

	what.member_of = sets_model[set]


	# Ignore category/conceptID, already in object instantiation

	descMd = dom.xpath(f'{wrap}/lido:lido/lido:descriptiveMetadata', namespaces=nss)[0]

	# <lido:objectWorkType><lido:conceptID lido:source="AAT" lido:type="Object name">300033618</lido:conceptID>
	# Other classifications: "Object name"; "Genre"   (that's all)
	fields = descMd.xpath('./lido:objectClassificationWrap/lido:objectWorkTypeWrap/lido:objectWorkType', namespaces=nss)
	classns = []
	genres = []
	for f in fields:
		term = ""
		if f.xpath('./lido:term/text()',namespaces=nss):
			term = f.xpath('./lido:term/text()',namespaces=nss)[0]
		if f.xpath('./lido:conceptID[@lido:type="Object name"]',namespaces=nss):
			typ = make_concept(f)
			if not typ:
				continue
			uri = typ.id
			classns.append(uri)
			what.classified_as = typ
			typ.classified_as = vocab.instances['work type']
		if f.xpath('./lido:conceptID[@lido:type="Genre"]',namespaces=nss):
			if f.xpath('./lido:conceptID[@lido:type="Genre"]/text()',namespaces=nss)[0].startswith("ycba_term_"):
				typ = get_concept_from_term(term,True)
				type_inline = get_concept_from_term(term,False)
				genres.append(type_inline)
				to_serialize.append(typ)
			else:
				typ = make_concept(f)
				genres.append(typ)
			if not typ:
				continue
			typ.classified_as = vocab.instances['work type']

	# <lido:classification><lido:conceptID lido:source="AAT" lido:type="Classification">300033618</lido:conceptID>
	# This is the object type, so process first. Only "Classification" as a type value
	fields = descMd.xpath('./lido:objectClassificationWrap/lido:classificationWrap/lido:classification', namespaces=nss)
	if len(fields) > 1:
		print(f"Record {f} has more than one classification")
	classterms = []
	for f in fields:
		typ = make_concept(f)
		cl = f.xpath('./lido:term/text()', namespaces=nss)[0]
		classterms.append(cl)
		#print(f"class:{cl}")
		supertype = supertypes.get(cl)
		#print(f"supertype:{supertype}")
		supertype_uri = supertype_uris.get(supertype)
		supertype_type = model.Type(ident=supertype_uri, label=supertype)
		supertype_type.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300241507", label="Super Type")
		what.classified_as = supertype_type
		if not typ:
			continue
		if typ.id in classns:
			# no need to duplicate this
			continue
		classns.append(typ.id)
		what.classified_as = typ
		typ.classified_as = vocab.instances['work type']

	classtype = ""
	if "Manuscript" in classterms or "Rare Book" in classterms:
		classtype = "text"
		wtid = lookup_or_map(f"ycba:text/{t}")
		whattext = model.LinguisticObject(ident=urn_to_url_json(wtid,"text"))
		what.carries = whattext
		for genre in genres:
			whattext.classified_as = genre
		to_serialize.append(whattext)
	if ("Manuscript" not in classterms and "Rare Book" not in classterms) or len(classterms) > 1:
		classtype = "visual"
		wvid = lookup_or_map(f"ycba:visual/{t}")
		whatvi = model.VisualItem(ident=urn_to_url_json(wvid,"visual"))
		what.shows = whatvi
		for genre in genres:
			whatvi.classified_as = genre
		to_serialize.append(whatvi)

	#placeholder for future implementation #update: implemented above
	#add_supertype(classns)

	# /lido:lido/lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet
	# <lido:titleSet lido:type="Repository title">
    #      <lido:appellationValue lido:pref="preferred" xml:lang="eng">Westminster from Lambeth, ...
    # See title types in spreadsheet
    # pref = preferred | alternate

	fields = descMd.xpath('./lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet', namespaces=nss)
	for f in fields:
		value = f.xpath('./lido:appellationValue/text()', namespaces=nss)
		if not value or not value[0]:
			continue
		else:
			value = value[0]
		typ = f.xpath('./@lido:type', namespaces=nss)[0]
		if typ == "Repository title":
			aeonItemTitle = value[:254] #cap length of http param
		pref = f.xpath('./lido:appellationValue/@lido:pref', namespaces=nss)[0]
		n = model.Name(value=value)
		what.identified_by = n
		if not hasattr(what, '_label'):
			what._label = value
			if classtype == "visual":
				whatvi._label = f'"{value}" - Visual Content'
			if classtype == "text":
				whattext._label = f'"{value}" - Linguistic Content'
		if pref == "preferred":
			n.classified_as = vocab.instances['primary']
			n.identified_by = vocab.DisplayName(value="Current Title")
			n.language = vocab.Language(ident="http://vocab.getty.edu/aat/300388277",label="English")
			# Override the first with preferred
			what._label = value
			if classtype == "visual":
				whatvi._label = f'"{value}" - Visual Content'
			if classtype == "text":
				whattext._label = f'"{value}" - Linguistic Content'
		else:
			n.identified_by = vocab.DisplayName(value="Former Title(s)")
		if not typ in ['Repository title', 'Alternate title', 'Alternative title']:
			# XXX Process other title types here
			pass
	try:
		if classtype == "visual":
			whatvi.identified_by = model.Name(content=whatvi._label)
		if classtype == "text":
			whattext.identified_by = model.Name(content=whattext._label)
	except:
		print(f" --- {fn} does not have an appellation!")
		if classtype == "visual":
			whatvi.identified_by = model.Name(content="Unnamed Content")
			whatvi._label = "Unnamed Content"
		if classtype == "text":
			whattext.identified_by = model.Name(content="Unnamed Content")
			whattext._label = "Unnamed Content"
		if not hasattr(what, '_label'):
			what._label = "Unnamed Object"
			what.identified_by = model.Name(content="Unnamed Object")

	#    <lido:inscriptions lido:type="Signature">
    #      <lido:inscriptionTranscription>Signed, lower left: "S. Scott"</lido:inscriptionTranscription>
    #    </lido:inscriptions>
    # type values: Inscriptions / Inscription, Signature / Signed, Marks, Lettering 
	fields = descMd.xpath('./lido:objectIdentificationWrap/lido:inscriptionsWrap/lido:inscriptions', namespaces=nss)
	for f in fields:
		value = f.xpath('./lido:inscriptionTranscription/text()', namespaces=nss)
		if not value or not value[0]:
			continue
		typ = f.xpath('./@lido:type', namespaces=nss)[0].lower()

		# Statement with the right type
		if typ in ['inscriptions', 'inscription', 'lettering']:
			ct = vocab.InscriptionStatement
		elif typ in ['signature', 'signed']:
			ct = vocab.SignatureStatement
		elif typ in ['mark', 'marks']:
			ct = vocab.MarkingsStatement
		stmt = ct(value=value[0])
		what.referred_to_by = stmt


	# <lido:workID lido:type="inventory number">B1978.43.15</lido:workID>
	# type values:  always "inventory number"
	fields = descMd.xpath('./lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet/lido:workID', namespaces=nss)
	for f in fields:
		typ = f.xpath('./@lido:type', namespaces=nss)[0].lower()
		value = f.xpath('./text()')[0]
		if typ == "inventory number":
			ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281")  # Yale Center for British Art by TMS con ID
			ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"), label="Yale Center for British Art")
			att_ass = model.AttributeAssignment()
			att_ass.carried_out_by = ycbagroup
			accnum = vocab.AccessionNumber(value=value)
			accnum.assigned_by = att_ass
			what.identified_by = accnum
			aeonCallNumber = value
		if typ == "lux yuag object" or typ == "object wikidata":
			pclss = model.HumanMadeObject
			what.equivalent = pclss(ident=value.replace("https","http").replace("/wiki/","/entity/"))
		if typ == "lux yuag visual item":
			pclss = model.VisualItem
			whatvi.equivalent = pclss(ident=value)

	# repositorySet -- type is always current --> current owner
	#      <lido:repositoryName>
	#        <lido:legalBodyID lido:source="ULAN" lido:type="local">500303557</lido:legalBodyID>
	#        <lido:legalBodyName><lido:appellationValue>Yale Center for British Art</lido:appellationValue></lido:legalBodyName>
	#        <lido:legalBodyWeblink>http://britishart.yale.edu</lido:legalBodyWeblink>
	#      </lido:repositoryName>
	# This needs to be merged, once, with the site data below
	# <lido:repositoryLocation><lido:partOfPlace><lido:namePlaceSet>
	#      <lido:appellationValue lido:label="On view or not">On view</lido:appellationValue>
	#      <lido:appellationValue lido:label="Concatenated location description">YCBA, 401, Bay20</lido:appellationValue>
	#      <lido:appellationValue lido:label="Site">Yale Center for British Art</lido:appellationValue>
	#               <lido:gml><gml:Point><gml:coordinates>41.3080060, -72.9306282</gml:coordinates>


	bid = descMd.xpath('./lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet/lido:repositoryName/lido:legalBodyID/text()', namespaces=nss)[0]
	try:
		lbl = descMd.xpath('./lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet/lido:repositoryName/lido:legalBodyName/lido:appellationValue/text()', namespaces=nss)[0]
	except:
		print(f'no legal body label in {fn}')
		lbl = ''

	owner = map_uuid('ulan', bid)
	owner = map_uuid('ycba', 'actor/ycba_actor_1281') #hardcode YCBA based on TMS CON ID 1281
	done_owner = file_exists("group", owner)

	# We need siteplace for the concat location tree
	pop = './lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet/lido:repositoryLocation/lido:partOfPlace'
	site = descMd.xpath(f'{pop}[./lido:namePlaceSet/lido:appellationValue/@lido:label="Site"]', namespaces=nss)[0]
	(siteplace, srlz) = make_place(site, 'ycba:place/site',True)

	if not done_owner or config2 == "do_site_everytime":
		# This is boilerplate for owner and siteplace
		done_owner = True
		owner = vocab.MuseumOrg(ident=urn_to_url_json(owner,"group"), label=lbl)
		owner.equivalent = model.Group(ident=entity_templates['ulan'].format(ident=bid))
		owner.identified_by = model.Name(value=lbl)
		owner.residence = siteplace
		actor_id_source[owner.id] = "pub"

		ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281")  # Yale Center for British Art by TMS con ID
		ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"), label="Yale Center for British Art")
		att_ass = model.AttributeAssignment()
		att_ass.carried_out_by = ycbagroup
		sysnum = vocab.SystemNumber(value="ycba_actor_1281")
		sysnum.assigned_by = att_ass
		owner.identified_by = sysnum

		#hardcode Formation for YCBA:
		#results in error AttributeError: 'Group' object has no attribute 'formed_by'
		#ts = model.TimeSpan()
		#owner.formed_by.timespan = ts
		#ts.begin_of_the_begin = "1977-01-01T00:00:00Z"
		#ts.end_of_the_end = "1978-01-01T00:00:00Z"

		#hardcode alt names for YCBA:
		owner.identified_by = model.Name(value="BAC")
		owner.identified_by = model.Name(value="YCBA")
		owner.identified_by = model.Name(value="British Art Center")

		hp = descMd.xpath('./lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet/lido:repositoryName/lido:legalBodyWeblink/text()', namespaces=nss)
		if hp:
			do = vocab.WebPage(label=f"Online home page for {lbl}")
			do.identified_by = model.Name(content=do._label)
			do.format="text/html"
			do.access_point= model.DigitalObject(ident=hp[0])
			lo = model.LinguisticObject(label=f"Home page for {lbl}")
			lo.digitally_carried_by = do
			owner.subject_of = lo
		to_serialize.append(owner)
		to_serialize.append(siteplace)

	# This would be better fixed in the LIDO by nesting partOfPlace under site
	gallery = descMd.xpath(f'{pop}/lido:namePlaceSet/lido:appellationValue[@lido:label="Concatenated location description"]/text()', namespaces=nss)
	if gallery:
		bits = gallery[0].split(',')
		bits = [x.lower().strip() for x in bits]
		if len(bits) < 2:
			# dunno what this is
			what.current_location = siteplace
		else:
			parent = siteplace
			# only do the room
			b = bits[1]
			localid = f"ycba-{b}"
			galid = map_uuid('ycba', f'place/{localid}')
			gal = vocab.Gallery(ident=urn_to_url_json(galid,"place"), label=f"YCBA Location {b}")
			gal.identified_by = model.Name(value=f"YCBA Location {b}")
			gal.part_of = parent
			to_serialize.append(gal)
			what.current_location = gal
	else:
		what.current_location = siteplace

	# eg ycba-14282
	#  <lido:displayStateEditionWrap>
    #    <lido:sourceStateEdition>Lister 74</lido:sourceStateEdition> ... this is meaningless??
    #  </lido:displayStateEditionWrap>
    # eg ycba-21708
    # <lido:displayStateEditionWrap><lido:displayState>1st state</lido:displayState></lido:displayStateEditionWrap>'
    # eg ycba-21831
    # <lido:displayStateEditionWrap><lido:displayState>engraver\'s proof</lido:displayState></lido:displayStateEditionWrap>'
    # eg ycba-33477 
	# <lido:displayEdition>Edition 11/45: portfolio of 16 screenprints, box title and colophon pages</lido:displayEdition></lido:displayStateEditionWrap>
	# YUAG just has string, eg 30665
	# <lido:displayStateEditionWrap>Edition of 45 (arabic), 10 (roman), printed by Peter Kneubuhler, Zurich. Published by Peter Blum Editions, New York. 6/45</lido:displayStateEditionWrap>

	dsew = descMd.xpath('./lido:objectIdentificationWrap/lido:displayStateEditionWrap', namespaces=nss)
	for dse in dsew:
		kids = dse.getchildren()
		if not kids and dse.text and dse.text.strip():
			val = dse.text.strip()
			what.referred_to_by = vocab.EditionStatement(value=val)
		elif kids:
			# Might have displayEdition, displayState, sourceStateEdition
			src = dse.xpath('./lido:sourceStateEdition/text()', namespaces=nss)
			val = dse.xpath('./lido:displayEdition/text()', namespaces=nss)
			if val:
				what.referred_to_by = vocab.EditionStatement(value=val[0])
			sval = dse.xpath('./lido:displayState/text()', namespaces=nss)
			if sval:
				what.referred_to_by = vocab.PhysicalStatement(value=sval[0])

			# XXX What to do with source?

	# <lido:objectMeasurementsWrap><lido:objectMeasurementsSet>
	#	<lido:displayObjectMeasurements>29.5 x 80.4 cm (11 5/8 x 31 5/8 in.)</lido:displayObjectMeasurements>
	#	<lido:objectMeasurements>
	#		<lido:measurementsSet><lido:measurementType>height</lido:measurementType><lido:measurementUnit>cm</lido:measurementUnit>
	#			<lido:measurementValue>29.50</lido:measurementValue>
	#       <lido:extentMeasurements>overall</lido:extentMeasurements>

	dims = descMd.xpath('./lido:objectIdentificationWrap/lido:objectMeasurementsWrap/lido:objectMeasurementsSet', namespaces=nss)
	# Can be multiple dimension sets for framed / unframed etc
	# But only ever 1 objectMeasurements per objectMeasurementsSet, and 1 measurementsSet per objectMeasurements
	for d in dims:
		stmt = d.xpath('./lido:displayObjectMeasurements/text()', namespaces=nss)
		extent = d.xpath('./lido:objectMeasurements/lido:extentMeasurements/text()', namespaces=nss)
		if extent:
			extentlabel = extent[0] + ": "
		else:
			extentlabel = ""
		if stmt:
			dimstat = vocab.DimensionStatement(value=extentlabel + stmt[0])
		what.referred_to_by = dimstat
		mss = d.xpath('./lido:objectMeasurements/lido:measurementsSet', namespaces=nss)
		for ms in mss:
			mst = ms.xpath('./lido:measurementType/text()', namespaces=nss)[0]
			try:
				msu = ms.xpath('./lido:measurementUnit/text()', namespaces=nss)[0]
			except:
				msu = "unknown"
			msv = ms.xpath('./lido:measurementValue/text()', namespaces=nss)[0]

			mtype = dimtypemap.get(str(mst), vocab.PhysicalDimension)
			#munit = dimunitmap.get(str(msu), unknownUnit)
			munit = dimunitmap.get(str(msu))
			try:
				mval = float(msv)
			except:
				print(str(msv))
			if mval > 0:
				dim = mtype(value=mval)
				dim.unit = munit
				if extent:
					dim.classified_as = get_concept_from_term(extent[0], False)
					to_serialize.append(get_concept_from_term(extent[0], True))
				what.dimension = dim

			# XXX process extent into a technique on an attributeassignment on the dimension
			# per decision here: https://github.com/linked-art/linked.art/issues/251#issuecomment-697605983
			# or into a separate part for support, to allow materials to be associated?


	# Events!

	events = descMd.xpath('./lido:eventWrap/lido:eventSet', namespaces=nss)
	for e in events:
		event = e.xpath('./lido:event', namespaces=nss)[0]
		stmt = e.xpath('./lido:displayEvent/text()', namespaces=nss)

		actor_source = "object"
		# Type:
		#note_etypes = ["Curatorial comment","Curatorial description","Gallery label","Published catalog entry"]
		etyp = event.xpath('./lido:eventType/lido:conceptID/text()', namespaces=nss)
		etyp = etyp[0]
		#print(f"etyp:{etyp}")
		if etyp == "300055863":
			#Provenance ex: 766
			provnote = vocab.ProvenanceStatement()
			stmt = f'<span class=\"lux_data\">{stmt[0]}</span>'
			provnote.content = stmt.replace("\n", "</br>").replace("\\n", "").replace("---", "")
			what.referred_to_by = provnote
			#if provEntry is not None:
				#provEntry.referred_to_by = provnote
		elif etyp == "300048722":
			#Gallery Label 38536
			if stmt:
				stmtHTML = f'<span class=\"lux_data\">{stmt[0]}</span>'
				stmtHTML2 = stmtHTML.replace("\n", "</br>").replace("\\n", "").replace("---", "")
				gallerylabel = vocab.GalleryLabel(value=stmtHTML2)
				gallerylabel.identified_by = model.Name(value="Gallery Label")
				what.referred_to_by = gallerylabel
		elif etyp == "300111999":
			#Published Catalog Entry 38536
			if stmt:
				stmtHTML = f'<span class=\"lux_data\">{stmt[0]}</span>'
				stmtHTML2 = stmtHTML.replace("\n", "</br>").replace("\\n", "").replace("---", "")
				pubcatentry = vocab.PubCatEntry(value=stmtHTML2)
				pubcatentry.identified_by = model.Name(value="Published Catalog Entry")
				what.referred_to_by = pubcatentry
		elif etyp == "300435416":
			#Curatorial Comment/Curatorial Description 15340
			if stmt:
				stmtHTML = f'<span class=\"lux_data\">{stmt[0]}</span>'
				stmtHTML2 = stmtHTML.replace("\n", "</br>").replace("\\n", "").replace("---", "</br>")
				currComment = vocab.CurComment(value=stmtHTML2)
				currComment.identified_by = model.Name(value="Curatorial Comment")
				what.referred_to_by = currComment
		if etyp == "300054713":
			# Production, add to object
			eventobj = model.Production()
			what.produced_by = eventobj
		elif etyp == "300054686":
			#bypassing etyp publications, handling this instead in relworks
			continue
		elif etyp == "300054766":
			# exhibition
			# exhibition's eventID is not unique across venues AND not unique across records
			# Construct fake id by combining earliest date and eid

			# Exhibitions not as good as main object
			#actor_source = "life"
			actor_source = "exh" #ERJ override "life" to get an actor for exhibits

			eid = event.xpath('./lido:eventID[@lido:type="local"]/text()', namespaces=nss)
			equiv_eid = event.xpath('./lido:eventID[@lido:type="LUX YUAG exhibition"]/text()', namespaces=nss)
			edate = event.xpath('./lido:eventDate/lido:date/lido:earliestDate/text()', namespaces=nss)

			if stmt:
				exhlabel = stmt[0]
			else:
				exhlabel = "Unnamed Exhibition"

			if eid and edate:
				exid = f"{eid[0]}-{edate[0]}"
			elif eid:
				exid = eid[0]
			else:
				exid = None
			if exid:
				euu = map_uuid("ycba", f"exhibition/{exid}")
				eventobj = vocab.Exhibition(ident=urn_to_url_json(euu,"activity"),label=exhlabel)
				ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281") #Yale Center for British Art by TMS con ID
				ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"),label="Yale Center for British Art")
				att_ass = model.AttributeAssignment()
				att_ass.carried_out_by = ycbagroup
				sysnum = vocab.SystemNumber(value=exid)
				sysnum.assigned_by = att_ass
				eventobj.identified_by = sysnum
				to_serialize.append(eventobj)
			# XXX Check if this is a second date for the same eventID
			# if so make a parent exhibition
			else:
				#does this ever happen?
				eventobj = vocab.Exhibition(ident=AUTO_URI,label=exhlabel)
				# no id to match against :(
			if equiv_eid:
				equiv_act = model.Activity
				equiv_act_id = equiv_eid[0].replace("\n","")
				eventobj.equivalent = equiv_act(ident=equiv_act_id)
			k = f"exhibitset-{exid}"
			v = f"Exhibit set for \"{eventobj._label}\""
			setuu = map_uuid("ycba", f"set/{k}")
			setobj = vocab.Set(ident=urn_to_url_json(setuu, "set"))
			setident = model.Identifier(value=k)
			ycbagroupuu = map_uuid("ycba", "actor/ycba_actor_1281")  # Yale Center for British Art by TMS con ID
			ycbagroup = model.Group(ident=urn_to_url_json(ycbagroupuu, "group"), label="Yale Center for British Art")
			att_ass = model.AttributeAssignment()
			att_ass.carried_out_by = ycbagroup
			setident.assigned_by = att_ass
			setobj.identified_by = setident
			#setobj.identified_by = model.Identifier(value=k)
			setobj.identified_by = model.Name(value=v)
			setobj._label = v
			exh_activity = model.Activity(ident=eventobj.id,label=eventobj._label)
			setobj.used_for = exh_activity
			setobj.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300054766", label="Exhibition")
			eventobj.used_specific_object = setobj
			what.member_of = setobj
			to_serialize.append(setobj)
		elif etyp == "300157782":
			# acquisition, make a prov entry - but only one of prov per object in YCBA
			# No need to look these up as won't be referred to elsewhere
			eid = event.xpath('./lido:eventID/text()', namespaces=nss)
			if eid:
				prov_uu = lookup_or_map(f"ycba:prov/{eid[0]}")
			else:
				#prov_uu = AUTO_URI #ERJ 8/17/2021
				prov_uu = lookup_or_map(f"ycba:prov/{fn}")
			provEntry = vocab.ProvenanceEntry(ident=urn_to_url_json(prov_uu,"provenance"))
			#suppress provenance entities temporarily per https://git.yale.edu/LUX/pipeline/issues/201
			#to_serialize.append(provEntry)
			provEntry._label = f"Acquisition of \"{what._label}\""
			provEntry.identified_by = vocab.PrimaryName(content=provEntry._label)
			eventobj = model.Acquisition()
			eventobj.transferred_title_of = what
			provEntry.part = eventobj
			acqactors = event.xpath('./lido:eventActor', namespaces=nss)
			for acqa in acqactors:
				acqactor_elm = acqa.xpath("./lido:actorInRole/lido:actor", namespaces=nss)[0]
				acqroleactor = acqa.xpath("./lido:actorInRole/lido:roleActor/lido:conceptID/text()", namespaces=nss)
				acqrelowner = acqa.xpath("./lido:actorInRole/lido:roleActor/lido:term/text()", namespaces=nss)
				if len(acqroleactor) > 0 and len(acqrelowner) > 0 and acqroleactor[0] == "300203630" and acqrelowner[0] == "Owner":
					(acqwho, srlz) = make_actor(acqactor_elm,"acq")
					what.current_owner = acqwho
		elif etyp in ["300111999","300048722","300055863","300435416"]:
			#already processed respectively PubCatEntry, Gallery Label,Provenance, Curatorial
			continue
		else:
			print(f"Unknown event type: {etyp}")
			continue

		# names

		for s in stmt:
			eventobj.identified_by = vocab.DisplayName(value=s)

		if etyp != "300054686": # Don't put name of work on publishing activity
			names = event.xpath('./lido:eventName/lido:appellationValue/text()', namespaces=nss)
			if names:
				eventobj.identified_by = vocab.PrimaryName(value=names[0])
				if len(names) > 1:
					for n in names[1:]:
						eventobj.identified_by = model.Name(value=n)

		# dates 
		eventDateLbl = event.xpath('./lido:eventDate/lido:displayDate/text()', namespaces=nss)
		eventDateBegin = event.xpath('./lido:eventDate/lido:date/lido:earliestDate/text()', namespaces=nss)
		eventDateEnd = event.xpath('./lido:eventDate/lido:date/lido:latestDate/text()', namespaces=nss)

		if eventDateLbl or eventDateBegin or eventDateEnd:
			ts = model.TimeSpan()
			if eventDateLbl:
				ts.identified_by = vocab.DisplayName(value=eventDateLbl[0])
			if eventDateBegin:
				try:
					dt = make_datetime(eventDateBegin[0])
				except ValueError:
					# print(f"{fn} | eventBegin | {eventDateBegin[0]}")
					dt = None
				if dt:
					if eventDateLbl and eventDateLbl[0] == "undated":
						ts.begin_of_the_begin = dt[0]
						ts.begin_of_the_end = dt[0]
					else:
						ts.begin_of_the_begin = dt[0]
						ts.end_of_the_begin = date_time_minus_one_second(dt[1])
			if eventDateEnd:
				try:
					dt = make_datetime(eventDateEnd[0])
				except ValueError:
					# print(f"{fn} | eventEnd | {eventDateEnd[0]}")
					dt = None
				if dt:
					if eventDateLbl and eventDateLbl[0] == "undated":
						ts.end_of_the_begin = date_time_minus_one_second(dt[1])
						ts.end_of_the_end = date_time_minus_one_second(dt[1])
					else:
						ts.begin_of_the_end = dt[0]
						ts.end_of_the_end = date_time_minus_one_second(dt[1])
			#note: the following logic only puts ts on first eventobj
			if not hasattr(eventobj, 'timespan'):
				eventobj.timespan = ts
				if etyp == "300054713": #get the production date only for aeon
					aeonItemDate = eventDateEnd[0]
		# Period during which the event occured
		period = event.xpath('./lido:periodName', namespaces=nss)
		if period:
			for p in period:
				# This isn't really a Type, but a Period
				# So build from scratch
				pconc = p.xpath('./lido:conceptID', namespaces=nss)
				pt = p.xpath('./lido:term/text()', namespaces=nss)
				if pconc:
					puri = get_concept_uri(pconc[0])
				else:
					puri = None
				if pt:
					plbl = pt[0]
				else:
					plbl = ''
				if puri or plbl:
					if puri:
						period = model.Period(ident=puri)
					else:
						period = model.Period()
					if plbl:
						period._label = plbl
					eventobj.occurs_during = period

		matstmt = event.xpath('./lido:eventMaterialsTech/lido:displayMaterialsTech/text()', namespaces=nss)
		if matstmt:
			what.referred_to_by = vocab.MaterialStatement(value=matstmt[0])
			aeonFormat = matstmt[0]
		mats = event.xpath('./lido:eventMaterialsTech/lido:materialsTech/lido:termMaterialsTech', namespaces=nss)
		for m in mats:
			# type can be:  proof (??), medium/material, support, technique
			# map only technique to technique, others to materials

			mt = m.xpath('./lido:conceptID/@lido:type', namespaces=nss)[0].lower()
			if mt == "technique":
				t = make_concept(m)
				if t:
					if not t.id:
						try:
							missing_techniques[t._label] += 1
						except:
							missing_techniques[t._label] = 1
					else:
						eventobj.technique = t
			else:
				t = make_concept(m, clss=model.Material)
				if t:
					if not t.id:
						try:
							missing_materials[t._label] += 1
						except:
							missing_materials[t._label] = 1
					else:
						what.made_of = t
						if mt == "support":
							# XXX Maybe make this into a part?
							pass

		objectculture = event.xpath('./lido:culture/lido:conceptID', namespaces=nss)
		# XXX What do do about culture???
		# Is this tied to production place, or the style?

		#actors
		actors = event.xpath('./lido:eventActor', namespaces=nss)
		actorObjs = []

		prodactor_count = 0
		for a in actors:
			actor_elm = a.xpath("./lido:actorInRole/lido:actor", namespaces=nss)[0]
			(who, srlz) = make_actor(actor_elm, source=actor_source)
			if etyp == "300054713": #production
				prodactor_count += 1
				if hasattr(who, '_label') and prodactor_count == 1:
					aeonItemAuthor.append(who._label)
				aqas = a.xpath('./lido:actorInRole/lido:attributionQualifierActor/text()', namespaces=nss)
				if not aqas: #29444 Robert Smirke
					partprod = model.Production()
					partprod.carried_out_by = who
					if prodactor_count == 1:
						partprod.classified_as = model.Type(ident="http://vocab.getty.edu/page/aat/300025103",label="Primary Artist")
					eventobj.part = partprod
				actorDone = False
				for aqa in aqas:
					#print(aqa)
					if aqa.lower().startswith("and"): #21987 James McArdell
						aqa = aqa[3:]
					if aqa.lower().startswith("and:"):
						aqa = aqa[4:]
					elif aqa.lower().startswith("or"): #22205 Joseph Bishop Pratt
						aqa = aqa[2:]
					elif aqa.lower().startswith("and / or"):
						aqa = aqa[8:]
					if aqa.lower().startswith("/ or"):  # 29444 Mary Smirke
						aqa = aqa[4:]
					aqa = aqa.lstrip()
					#print(aqa)
					if not aqa:  # 29444 Robert Smirke
						partprod = model.Production()
						partprod.carried_out_by = who
						if prodactor_count == 1:
							partprod.classified_as = model.Type(ident="http://vocab.getty.edu/page/aat/300025103",label="Primary Artist")
						eventobj.part = partprod
						actorDone = True
					if aqa.lower() in attrib_qual_group and actorDone == False:
						qga_who = make_qualified_group_actor(actor_elm,aqa.lower(),who)
						to_serialize.append(qga_who)
						partprod = model.Production()
						partprod.carried_out_by = qga_who
						if prodactor_count == 1:
							partprod.classified_as = model.Type(ident="http://vocab.getty.edu/page/aat/300025103",label="Primary Artist")
						eventobj.part = partprod
						actorDone = True
					for aqi in attrib_qual_infl:
						if aqi in aqa.lower() and actorDone == False:
							partprod = model.Production()
							partprod.influenced_by = who
							if prodactor_count == 1:
								partprod.classified_as = model.Type(ident="http://vocab.getty.edu/page/aat/300025103",label="Primary Artist")
							#partprod.referred_to_by = vocab.CreatorDescription(content=aqa)
							#qual_type = get_concept_from_term(aqa)
							partprod.classified_as = get_concept_from_term(aqa,False)
							to_serialize.append(get_concept_from_term(aqa,True))
							eventobj.part = partprod
							actorDone = True
					if actorDone == False:
						partprod = model.Production()
						partprod.carried_out_by = who
						if prodactor_count == 1:
							partprod.classified_as = model.Type(ident="http://vocab.getty.edu/page/aat/300025103",label="Primary Artist")
						#partprod.referred_to_by = vocab.CreatorDescription(content=aqa)
						#qual_type = get_concept_from_term(aqa)
						partprod.classified_as = get_concept_from_term(aqa, False)
						to_serialize.append(get_concept_from_term(aqa, True))
						eventobj.part = partprod
						actorDone = True
			if etyp == "300054766" or etyp == "300157782":  # exhibition or acquisition
				eventobj.carried_out_by = who
			if srlz == "serialize":
				if not hasattr(who, 'identified_by'):
					print("Not serializing unnamed actor")
				else:
					to_serialize.append(who)
		# Methods for the event
		# e.g. type of acquisition
		meths = event.xpath('./lido:eventMethod', namespaces=nss)
		for meth in meths:
			t = make_concept(meth)			
			if t:
				# print(f"assigning: {t._label}")
				eventobj.classified_as = t
			# XXX this should maybe be `technique` rather than `classified_as`
 		#places
		places = event.xpath('./lido:eventPlace', namespaces=nss)
		for place in places:
			pl = place.xpath('./lido:place', namespaces=nss)
			for p in pl:
				(place, srlz) = make_place(p)
				if not place:
					continue
				if srlz == "serialize":
					to_serialize.append(place)
				if etyp == "300054766" :  # exhibition
					eventobj.took_place_at = place
	onview = descMd.xpath(f'{pop}/lido:namePlaceSet/lido:appellationValue[@lido:label="On view or not"]/text()', namespaces=nss)[0]
	if onview:
		if onview == "Not on view" and aeonSet == "ycba:pd":
			aeonLocation = "bacpd"
			if len(aeonItemAuthor) > 0:
				aeonItemAuthor1 = aeonItemAuthor[0]
			else:
				aeonItemAuthor1 = ""
			accessStmtURL = aeonHost + "Action=" + aeonAction + "&Form=" + aeonForm + "&Value=" + aeonValue + "&Site=" + aeonSite + "&Callnumber=" + aeonCallNumber + "&ItemTitle=" + aeonItemTitle + "&ItemAuthor=" + aeonItemAuthor1 + "&ItemDate=" + aeonItemDate + "&Format=" + aeonFormat + "&Location=" + aeonLocation + "&mfhdID=" + aeonMfhdID + "&EADNumber=" + aeonEADNumber
			accessStmt = f'<span class=\'lux_data\'><a href=\'{accessStmtURL}\'>{aeonLabel}</a></span>'
		else:
			accessStmt = onview
		#Hardcode Wiley location - removed 4/6/23
		#if fn == "82154":
			#accessStmt = "Not on view at Yale Center for British Art"
		what.referred_to_by = vocab.AccessStatement(content=accessStmt)

	# objectRelationWrap / subjects
	subjs = descMd.xpath('./lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectConcept', namespaces=nss)
	for sub in subjs:
		t = make_concept(sub)

		# If t is also a genre for the object, then it's the classification of the VI.
		# Most others are depicted_entities
		if t:
			if not t.id:
				try:
					missing_subjects[t._label] += 1
				except:
					missing_subjects[t._label] = 1
			else:
				if classtype == "visual":
					whatvi.represents = t
				if classtype == "text":
					whattext.represents = t

	subjs = descMd.xpath('./lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectObject', namespaces=nss)
	for sub in subjs:
		t = make_object_concept(sub)

		# If t is also a genre for the object, then it's the classification of the VI.
		# Most others are depicted_entities
		if t:
			if not t.id:
				try:
					missing_subjects[t._label] += 1
				except:
					missing_subjects[t._label] = 1
			else:
				if classtype == "visual":
					whatvi.represents = t
				if classtype == "text":
					whattext.represents = t

	eventSubjs = descMd.xpath('./lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectEvent', namespaces=nss)
	for es in eventSubjs:
		ets = es.xpath('./lido:event/lido:eventType', namespaces=nss)
		for et in ets:
			t = make_concept(et)
			if t:
				if not t.id:
					try:
						missing_subjects[t._label] += 1
					except:
						missing_subjects[t._label] = 1
				else:
					if classtype == "visual":
						whatvi.about = t
					if classtype == "text":
						whattext.about = t

	# PROBABLY these are people, but they might be groups? No way to know
	# PROBABLY they're depicted? Also no way to know
	peopleSubjs = descMd.xpath('./lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectActor/lido:actor', namespaces=nss)
	for peep in peopleSubjs:
		# Some peopleSubjs are really just classes
		aid = peep.xpath("./lido:actorID/@lido:source", namespaces=nss)
		aid2 = peep.xpath("./lido:actorID/@lido:type", namespaces=nss)[0]
		if "AAT" in aid:
			# concept
			u = get_concept_uri(peep.xpath('./lido:actorID', namespaces=nss)[0])
			t = model.Type(ident=u, label=peep.xpath('./lido:nameActorSet/lido:appellationValue/text()', namespaces=nss)[0])
			if classtype == "visual":
				if aid2 == "subjectActorAbout":
					whatvi.about = t
				else:
					whatvi.represents = t
			if classtype == "text":
				if aid2 == "subjectActorAbout":
					whattext.about = t
				else:
					whattext.represents = t
		else:
			(who, srlz) = make_actor(peep, source="subject")
			if classtype == "visual":
				if aid2 == "subjectActorAbout":
					whatvi.about = who
				else:
					whatvi.represents = who
			if classtype == "text":
				if aid2 == "subjectActorAbout":
					whattext.about = who
				else:
					whattext.represents = who
			if srlz == "serialize":
				to_serialize.append(who)

	placeSubjs = descMd.xpath('./lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectPlace', namespaces=nss)
	# YCBA are all depicted
	# YUAG are first word in displayPlace
	for ps in placeSubjs:
		disp = ps.xpath('./lido:displayPlace/text()', namespaces=nss)
		pl = ps.xpath('./lido:place', namespaces=nss)

		for p in pl:
			(place, srlz) = make_place(p)
			if not place:
				continue
			if srlz == "serialize":
				to_serialize.append(place)
			if disp:
				# try to get relationship from display text
				dispwds = disp[0].split()
				rel = subjplace_rels.get(dispwds[0], "depicted")
				place._label = disp[0]
			else:
				rel = "depicted"

			if rel == "found":
				# make a provenance entry for the encounter
				prov_uu = lookup_or_map(f"ycba:prov/{what._label}")
				pe = vocab.ProvenanceEntry(ident=AUTO_URI)
				pe.identified_by = vocab.PrimaryName(content='Finding of "{what._label}"')
				to_serialize.append(pe)
				enc = model.Encounter()
				pe.part = enc
				enc.took_place_at = place
				enc.encountered = what
			elif rel == "production":
				# Check if the location is on the production, if not add it
				try:
					curr = what.produced_by.took_place_at
				except:
					curr = []

				if not curr:
					what.produced_by.took_place_at = place
				else:
					# XXX FIXME: reconcile here!
					# for now just add unless absolutely in it
					if not place in curr:
						what.produced_by.took_place_at = place
			elif rel == "owned":
				# ???????
				pass
			elif rel == "depicted":
				# add the place as depicted in the visualitem
				if classtype == "visual":
					whatvi.represents = place
				if classtype == "text":
					whattext.represents = place


	# Related Works

	relWorks = descMd.xpath('./lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet/lido:relatedWork', namespaces=nss)

	# make textual works
	# XXX grab metadata from OCLC
	# e.g. http://experiment.worldcat.org/oclc/51223673.jsonld

	for rw in relWorks:
		disp = rw.xpath('./lido:displayObject/text()', namespaces=nss)
		#oids = rw.xpath('./lido:object/lido:objectID', namespaces=nss)
		biboids = rw.xpath('./lido:object/lido:objectID[@lido:source="YCBA TMS Bibliographic Module record referenceID"]', namespaces=nss)

		for oid in biboids:
			if oid.text and oid.text.strip():
				uri = get_concept_uri(oid)
				if disp:
					what.referred_to_by = vocab.Citation(content=disp[0])

	# Credit line/Rights
	# where rightsWorkSet/rightsType/conceptID/text() == 500303557 
	#	lido:administrativeMetadata/lido:rightsWorkWrap/lido:rightsWorkSet/lido:creditLine
    
	adminMd = dom.xpath(f'{wrap}/lido:lido/lido:administrativeMetadata', namespaces=nss)[0]

	#rights
	rightsurl1 = ""
	rightsterm1 = ""
	copyright1 = ""
	rightsurl = adminMd.xpath('./lido:rightsWorkWrap/lido:rightsWorkSet/lido:rightsType/lido:conceptID[@lido:label="copyright statement"]/following-sibling::*[@lido:type="url"]/text()',namespaces=nss)
	if rightsurl:
		rightsurl1 = rightsurl[0]
	rightsterm = adminMd.xpath('./lido:rightsWorkWrap/lido:rightsWorkSet/lido:rightsType/lido:conceptID[@lido:label="copyright statement"]/following-sibling::*[@lido:type="url"]/@lido:label',namespaces=nss)
	if rightsterm:
		rightsterm1 = rightsterm[0]
	copyright = adminMd.xpath('./lido:rightsWorkWrap/lido:rightsWorkSet/lido:rightsType/lido:conceptID[@lido:label="copyright"]/../lido:term/text()',namespaces=nss)
	if copyright and rightsterm:
		copyright1 = copyright[0]
		if rightsterm1 == "In Copyright":
			if classtype == "visual":
				whatvi.referred_to_by = vocab.RightsStatement(value=copyright1)
			if classtype == "text":
				whattext.referred_to_by = vocab.RightsStatement(value=copyright1)
	if rightsurl and rightsterm:
		if classtype == "visual":
			right = model.Right(label="Copyright of Image")
			right.classified_as = model.Type(ident=rightsurl1, label=rightsterm1)
			right.identified_by = vocab.DisplayName(value=rightsterm1)
			whatvi.subject_to = right
		if classtype == "text":
			right = model.Right(label="Copyright of Text")
			right.classified_as = model.Type(ident=rightsurl1, label=rightsterm1)
			right.identified_by = vocab.DisplayName(value=rightsterm1)
			whattext.subject_to = right

	# creditline
	# this xpath should work as the following is hardcoded
	#<lido:conceptID lido:source="YCBA" lido:type="local" lido:label="object ownership">500303557</lido:conceptID>
	credit = adminMd.xpath('./lido:rightsWorkWrap/lido:rightsWorkSet[./lido:rightsType/lido:conceptID/text()="300055603"]/lido:creditLine/text()', namespaces=nss)
	if credit:
		what.referred_to_by= vocab.CreditLine(value=credit[0])

    # recordWrap -- homepage
	homepage = adminMd.xpath('./lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink[./@lido:formatResource="html"]/text()', namespaces=nss)
	if homepage:
		try:
			hp = vocab.WebPage(label=f"Online home page for \"{what._label}\"")
		except:
			hp = vocab.WebPage(label=f"Online home page for object")
		hp.identified_by = vocab.PrimaryName(content=f"View this record on the Yale Center for British Art website")
		hp.format = "text/html"
		hp.access_point = model.DigitalObject(ident=homepage[0].strip())
		try:
			lo = model.LinguisticObject(label=f"Home page for \"{what._label}\"")
		except:
			lo = model.LinguisticObject(label=f"Home page for object")
		lo.digitally_carried_by = hp
		what.subject_of = lo

	# resourceWrap -- pull in images, seeAlso to lido, iiif manifest
	resources = adminMd.xpath('./lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation', namespaces=nss)
	images = {}
	has_image_resource = False
	for res in resources:
		has_image_resource = True
		typ = res.xpath('./@lido:type', namespaces=nss)
		uri = res.xpath('./lido:linkResource/text()', namespaces=nss)
		if not uri or not typ:
			continue
		uri = uri[0].strip()
		typ = typ[0].strip()
		if typ in ['thumb', 'medium', 'large', 'original']:
			images[typ] = uri			
		elif typ == "http://iiif.io/api/presentation/2/context.json":
			val = f"IIIF v2 manifest for \"{what._label}\""
			do = model.DigitalObject(label=val)
			do.conforms_to = model.InformationObject(ident="http://iiif.io/api/presentation/2/context.json")
			do.format = "application/ld+json"
			do.access_point = model.DigitalObject(ident=uri)
			do.identified_by = model.Name(value=val)
			lo = model.LinguisticObject(label=val)
			lo.digitally_carried_by = do
			what.subject_of = lo
		elif typ == "http://iiif.io/api/presentation/3/context.json":
			val = f"IIIF v3 manifest for \"{what._label}\""
			do = model.DigitalObject(label=val)
			do.conforms_to = model.InformationObject(ident="http://iiif.io/api/presentation/3/context.json")
			do.format = "application/ld+json"
			do.access_point = model.DigitalObject(ident=uri)
			do.identified_by = model.Name(value=val)
			lo = model.LinguisticObject(label=val)
			lo.digitally_carried_by = do
			what.subject_of = lo

	if has_image_resource:
		thumbdo = model.DigitalObject(label="Primary Image")
		thumbdo.access_point = model.DigitalObject(ident=f"https://media.collections.yale.edu/thumbnail/ycba/obj/{id1}")
		thumbdo.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300215302", label="Digital Image")
		thumbvi = model.VisualItem(label="Primary Image")
		thumbvi.digitally_shown_by = thumbdo
		what.representation = thumbvi


	img = images.get('large', images.get('medium', images.get('thumb', None)))
	if img:
		do = vocab.DigitalImage(ident=AUTO_URI)
		do.format = "image/jpeg"
		do.access_point = model.DigitalObject(ident=img)
		if classtype == "visual":
			whatvi.digitally_shown_by = do
		if classtype == "text":
			whattext.digitally_shown_by = do
		to_serialize.append(do)
	serialize_method(to_serialize)
	fileidx += 1
	if fileidx > PROCESS_RECS:
		break

#for diagnostic
#i = -1
#for x in DB:
#	i += 1
#	print(x)
#	if i > 5:
#		break
#for x in DB:
#	if "wikidata" in x[0]:
#		print(x)
#print("--------")
#for x in NAMEDB:
#	print(x)

record_new_activities()
record_updated_activities()

missed_terms = {"techniques": missing_techniques, "subjects": missing_subjects, "materials": missing_materials}
fh = open('ycba_missed_terms.json', 'w')
outs = json.dumps(missed_terms)
fh.write(outs)
fh.close()
db_act.close()
print("--- %s seconds ---" % (time.time() - start_time))