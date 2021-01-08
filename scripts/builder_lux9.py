import os
import re
import uuid
import sys
import pathlib
import json
import time
from lxml import etree
from io import BytesIO
from cromulent import model, vocab
from cromulent.extract import date_cleaner
from lmdb_utils import LMDB


vocab.register_vocab_class("AccessStatement", {"parent": model.LinguisticObject, "id": "300133046", "label": "Access Statement", "metatype": "brief text"})
vocab.register_vocab_class("CallNumber", {"parent": model.Identifier, "id": "300311706", "label": "Call Number"})
vocab.register_vocab_class("Performance", {"parent": model.Activity, "id": "300069200", "label": "Performance"})
vocab.register_vocab_class("AttributionStatement", {"parent": model.LinguisticObject, "id": "300056109", "label": "Attribution Statement", "metatype": "brief text"})
vocab.register_vocab_class("ReproductionStatement", {'parent': model.LinguisticObject, "id":"300411336", "label": "Reproduction Statement", "metatype": "brief text"})


DEBUG = True
NO_OVERWRITE = False

model.factory.auto_assign_id = False
if not DEBUG:
	model.factory.validate_properties = False
	model.factory.validate_profile = False
	model.factory.validate_range = False
	model.factory.validate_multiplicity = False
	model.factory.json_serializer = "fast"
	model.factory.order_json = False

source = "../../lux"
dest = "../output_lux"

# LMDBs that map name --> uuid
class NameUuidDB(LMDB):

	def get(self, key):
		# name --> uuid, and generate a uuid if not present
		# ensure key is lowercase and stripped
		key = key.strip().lower()
		val = LMDB.get(self, key)
		if not val:
			val = str(uuid.uuid4())
			self.set(key, val)
			self.commit()
		if val in counts:
			counts[val] += 1
		else:
			counts[val] = 1
		return val

counts = {}

person_map = NameUuidDB('lux_person_db', open=True, map_size=int(2e9))
group_map = NameUuidDB('lux_group_db', open=True, map_size=int(2e9))
type_map = NameUuidDB('lux_type_db', open=True, map_size=int(2e9))   # including material, language, type, currency, unit
place_map = NameUuidDB('lux_place_db', open=True, map_size=int(2e9))
period_map = NameUuidDB('lux_period_db', open=True, map_size=int(2e9))
event_map = NameUuidDB('lux_event_db', open=True, map_size=int(2e9))
text_map = NameUuidDB('lux_text_db', open=True, map_size=int(2e9))
set_map = NameUuidDB('lux_set_db', open=True, map_size=int(1e9))

name_maps = {
	model.Person: person_map,
	model.Group: group_map,
	model.Place: place_map,
	model.Period: period_map,
	model.LinguisticObject: text_map,
	model.Event: event_map,
	model.Activity: event_map,
	model.Type: type_map,
	model.Language: type_map,
	model.Material: type_map,
	model.Currency: type_map,
	model.MeasurementUnit: type_map
}


st_uri = {'Collages': 'http://vocab.getty.edu/aat/300033963', 'Drawings': 'http://vocab.getty.edu/aat/300033973', 
	'Installations': 'http://vocab.getty.edu/aat/300047896', 'Paintings': 'http://vocab.getty.edu/aat/300033618', 
	'Photographs': 'http://vocab.getty.edu/aat/300046300', 'Prints': 'http://vocab.getty.edu/aat/300041273', 
	'Sculptures': 'http://vocab.getty.edu/aat/300047090', 'Pamphlets': 'http://vocab.getty.edu/aat/300220572', 
	'Maps': 'http://vocab.getty.edu/aat/300028094', 'Globes': 'http://vocab.getty.edu/aat/300028089', 
	'Atlases': 'http://vocab.getty.edu/aat/300028053', 'Cartography': 'http://vocab.getty.edu/aat/300028052', 
	'Realia': 'http://id.loc.gov/vocabulary/marcgt/rea', 'Animals': 'http://vocab.getty.edu/aat/300249395', 
	'Fossils': 'http://vocab.getty.edu/aat/300247919', 'Plants': 'http://vocab.getty.edu/aat/300132360', 
	'Minerals': 'http://vocab.getty.edu/aat/300011068', 'Meteorites': 'http://vocab.getty.edu/aat/300266159', 
	'Specimens': 'http://vocab.getty.edu/aat/300235576', 'Audio': 'http://vocab.getty.edu/aat/300028633', 
	'Models': 'http://vocab.getty.edu/aat/300247279', 'Slides': 'http://vocab.getty.edu/aat/300128371', 
	'Databases and Software': 'http://vocab.getty.edu/aat/300266679', 'Archival and Manuscript Material': 'x1', 
	'Kits': 'http://vocab.getty.edu/aat/300247921', 'Arms and Armor': 'x2', 'Wall Drawings': 'http://vocab.getty.edu/aat/300438620', 
	'Sound Devices': 'http://vocab.getty.edu/aat/300387677', 'Ritual Objects': 'http://vocab.getty.edu/aat/300312158', 
	'Calligraphy': 'http://vocab.getty.edu/aat/300266660', 'Jewelry': 'http://vocab.getty.edu/aat/300209286', 
	'Flatware': 'http://vocab.getty.edu/aat/300199800', 'Time-Based Media': 'http://vocab.getty.edu/aat/300185191', 
	'Masks': 'http://vocab.getty.edu/aat/300138758', 'Numismatics': 'http://vocab.getty.edu/aat/300054419', 
	'Containers': 'http://vocab.getty.edu/aat/300045611', 'Timepieces': 'http://vocab.getty.edu/aat/300041573', 
	'Furniture': 'http://vocab.getty.edu/aat/300037680', 'Lighting Devices': 'http://vocab.getty.edu/aat/300037581', 
	'Furnishings': 'http://vocab.getty.edu/aat/300037335', 'Posters': 'http://vocab.getty.edu/aat/300027221', 
	'Notated Music': 'http://vocab.getty.edu/aat/300417622', 'Datasets': 'http://vocab.getty.edu/aat/300312038', 
	'Clothing and Dress': 'http://vocab.getty.edu/aat/300266639', 'Text': 'http://vocab.getty.edu/aat/300263751', 
	'Toys and Games': 'http://vocab.getty.edu/aat/300218781', 'Journals': 'http://vocab.getty.edu/aat/300215390', 
	'Equipment': 'http://vocab.getty.edu/aat/300122241', 'Decorative Arts': 'http://vocab.getty.edu/aat/300054168', 
	'Moving Images': 'http://vocab.getty.edu/aat/300054168', 'Software': 'http://vocab.getty.edu/aat/300028566', 
	'Databases': 'http://vocab.getty.edu/aat/300028543', 'Books': 'http://vocab.getty.edu/aat/300028051', 
	'Theses and Dissertations': 'http://vocab.getty.edu/aat/300028028', 'Newspapers': 'http://vocab.getty.edu/aat/300026656', 
	'Tools and Equipment': 'http://vocab.getty.edu/aat/300022238', 'Textiles': 'http://vocab.getty.edu/aat/300014063', 
	'Two-Dimensional Objects': 'http://vocab.getty.edu/aat/300010332', 'Three-Dimensional Objects': 'http://vocab.getty.edu/aat/300010331', 
	'Architectural Elements': 'http://vocab.getty.edu/aat/300000885', 'Periodicals': 'http://vocab.getty.edu/aat/300026657'}

st_to_class = {
	"Archival and Manuscript Material": model.Set,
	"Databases and Software": model.DigitalObject,
	"Datasets": model.DigitalObject,
	"Notated Music": model.LinguisticObject,
	"Text": model.LinguisticObject,
	"Maps": model.HumanMadeObject,
	"Numismatics": model.HumanMadeObject,
	"Objects": model.HumanMadeObject,
	"Sculptures": model.HumanMadeObject,
	"Specimens": model.HumanMadeObject,
	"Three-Dimensional Objects": model.HumanMadeObject,
	"Two-Dimensional Objects": model.HumanMadeObject,
	"Paintings, Prints, and Drawings": model.HumanMadeObject,
	"Audio": model.InformationObject,
	"Video": model.InformationObject,
	"Time-Based Media": model.InformationObject,
	"Cartography": model.LinguisticObject
}

agent_type_to_class = {
	"http://id.loc.gov/ontologies/bibframe/Person": model.Person,
	"http://id.loc.gov/ontologies/bibframe/Meeting": model.Group,
	"http://id.loc.gov/ontologies/bibframe/Organization": model.Group,
	"http://vocab.getty.edu/page/aat/300024979": model.Person,
	"http://vocab.getty.edu/page/aat/300025969": model.Group,
	"http://vocab.getty.edu/page/aat/300026004": model.Group,
	'http://id.loc.gov/ontologies/bibframe/Family': model.Group,
	"family": model.Group,	
	"person": model.Person,
	"meeting": model.Group,
	"organization": model.Group,
	"institution": model.Group,
	"corporation": model.Group,
	"http://vocab.getty.edu/page/aat/300400513": model.Group,  # "other"
	"http://vocab.getty.edu/page/aat/300055768": model.Group   # "culture"
}


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
}

agent_roles_production = {
	"creator": "",
	"contributor" : "",
	"author": "",
	"artist": "",
	"maker": "",
	"collaborator" : "",
	'originator': "",
	"writer": "",

	"fabricator": "",
	"editor and translator": "",
	"editor": "",
	"editor in chief": "",
	"compiler": "",
	"illustrator": "",
	"abridger": "",
	"cartographer": "",
	"arranger": "",
	"translator": "",
	"lyricist": "",
	"composer": "",
	"calligrapher": "",
	"adapter": "",
	"printer": "",
	"carver": "",
	"engraver": "",
	"wood engraver": "",
	"wood-engraver": "",
	"metal engraver": "",
	"metal-engraver": "",
	"lithographer": "",
	"etcher": "",
	"decorator": "",
	"diesinker": "",
	"sculptor": "",
	"inscriber": "",
	"modeler": "",
	"woodcutter": "",
	"printmaker": "",
	"printer of plates": "",
	'photographer': "",
	'mint': "",
	'manufacturer': "",
	'typographer': "",
	'electrotyper': "",
	'stereotyper': "",
	'designer': "",
	'typesetter': "",
	'compositor': "",
	'architect': "",
	'inventor': "",
	'book designer': "",
	'bookplate designer': "",
	'cover designer': "",
	'bookjacket designer': "",
	'book jacket designer': '',
	'type designer': "",
	'binding designer': "",
	'binding designers': "",
	'letterer': "",
	'colorist': "",
	'organizer': "",
	'book producer': "",
	'binder': "",
	'binders': '',
	'papermaker': "",
	'fore-edge painter': "",
	'scribe': "",
	'screenwriter': "",
	'adapter': "",
	'bookbinder': "",
	'transcriber': "",
	'copyist': "",
	'draftsman': "",
	'mounter': "",
	'illuminator': '',
	"paper engineer": "",
	"paper maker": "",
	"collotyper": "",
	"animator": "",
	"proofreader": "",
	"series editor": "",
	"chromo-lithographer": "",
	"playing card maker": "",
	"book artist": "",
	"penciller": "",
	"inker": "",
	"cover artist": "",
	"after": "",
	"clockmaker": "",
	"potter": "",
	"gilder": "",
	"casemaker": "",
	"repairer": ""
}

agent_roles_sponsor = {
	"commissioned by": "",
	"sponsor": "",
	"sponsoring body": "",
	"host institution": "",
	"degree granting institution": "",
	"commissioning body": "",
	"funder": "",
	"funder/sponsor": "",
	"sponsoring institution": "",
	"degree supervisor": ""
}

agent_roles_publication = {
	"publisher": "",
	"publishers": "",
	"pub": "",
	"issuing body": "",
	"distributor": "",
	"bookseller": "",
	"retailer": "",
	"enacting jurisdiction": "",
	"broadcaster": "",
	"film distributor": "",
	"issuer": "",
	"bank": ""
}

agent_roles_performance = {
	"performer": "",
	"conductor": "",
	"librettist": "",
	"singer": "",
	"tenor": "",
	"soprano": "",
	"boy soprano": "",
	"narrator": "",
	"pianist": "",
	"musician": "",
	"orchestra": "",
	"stage director": "",
	"violoncellist": "",
	"director": "",
	"actor": "",
	"film director": "",
	"television director": "",
	"producer": "",
	"interviewer": "",
	"interpreter": "",
	"interviewee": "",
	"praeses": "",
	"saxophonist": "",
	"cast": "",
	"dancer": "",
	"choreographer": "",
	"vocalist": "",
	"instrumentalist": "",
	"production personnel": "",
	"production": "",
	"cinematographer": "",
	"film editor": "",
	"camera": "",
	"television producer": "",
	"production company": "",
	"costume designer": "",
	"lighting designer": "",
	"costume designer": "",
	"bass": "",
	"violist": "",
	"hornist": "",
	"guitarist": "",
	"trombonist": "",
	"set designer": "",
	"lighting designer": "",
	"recording engineer": "",
	"stage manager": "",
	"trumpet player": "",
	"organist": "",
	"video designer": "",
	"direction": "",
	"music": "",
	"clarinet": "",
	"film producer": "",
	"director of photography": "",
	"voice actor": "",
	"filmmaker": "",
	"on-screen presenter": "",
	"on-screen participant": "",
	"cast member": "",
	"script": "",
	"production designer": "",

}

agent_roles_encounter = {
	"collector": "",
	"art collector": "",
	"expedition": "",
	"discoverer": ""
}

agent_roles_provenance = {
	"donor": "",
	"owner": "",
	"former owner": "",
	"client": "",
	"patron": "",
	"source": "",
	"copyright holder": "",
	"auctioneer": "",
	"licensor": "",
	"current owner": "",
	"purchaser": "",
	"vendor": "",
	"exchanger": "",
	"land owner": "",
	"previous owner": "",
	"licensee": "",
	"commissaire-priseur": ""
}

agent_roles_exhibition = {
	"borrower": "",
	"exhibition": "",
	"lender": ""
}

agent_roles_other = {
	"correspondent": "",
	"respondent": "",
	"respondant": "",
	"defendant": "",
	"defendent": "",
	"appellee": "",
	"associated name": "",
	"addressee": "",
	"judge": "",
	"witness": "",
	"appellant": "",
	"recipient": "",
	"plaintiff": "",
	"honouree": "",
	"complainant": "",
	"petitioner": "",
	"claimant": "",
	"sender": "",
	"user": "",
	"dedicatee": "",
	"commentator": "",
	"curator": "",
	"assignee": "",
	"annotator": "",
	"magistrate": "",
	"proprietor": "",
	"advertiser": "",
	"signer": "",
	"pressman": "",
	"auxilium": "",
	"reporter": "",
	"other": "",
	"presenter": "",
	"speaker": "",
	"honoree": "",
	"conservator": "",
	"reviewer": "",
	"subject of parody": "",
	"court reporter": "",
	"pseud": "",
	"digitiser": "",
	"panelist": "",
	"moderator": ""
}

place_roles_production = {
	"made": "",
	"manufactured": "",
	"fabricated": "",
	"creation place": "",
	"designed": "",
	"decorated": "",
	"assembled": ""
}
place_roles_publication = {
	"publication place": "",
	"publication": "",
	"published": "",
	"retailed": "",
	"printed": ""
}
place_roles_encounter = {
	"collected": "",
	"excavated": "",
	"found": ""
}

place_roles_provenance = {
	"owned": ""
}

place_roles_other = {
	"depicted or about": "",
	"depicted": "",
	"about": "",
	"associated place": "",
	"location": "",
	"place": ""
}

place_roles_sponsor = {}
place_roles_performance = {}
place_roles_exhibition = {}


date_roles_sponsor = {}
date_roles_performance = {
	"broadcast": ""
}
date_roles_production = {
	"created": "",
	"dated": "",
	"original date": "",
	"creation date": ""
}
date_roles_publication = {
	"publication": "",
	"publication date": "",
	"issued": "",
	"reprint/reissue date": ""
}
date_roles_encounter = {
	"collected": ""
}
date_roles_provenance = {
	"copyright": "",
	"copyright date": ""
}
date_roles_exhibition = {
	"event": "" # is this correct?
}

date_roles_other = {
	"modified": "",
	"digitized": "",
	"deaccession": "",
	"existence": "",
	"other": ""
}


agent_roles_missed = {}
place_roles_missed = {}
date_roles_missed = {}

def construct_text(rec, clss):
	# value, language, language_uri, character_set, character_set_uri, direction, direction_uri

	if not 'value' in rec:
		return None
	elif not rec['value']:
		return None
	what = clss(content=rec['value'])

	# Language
	lang_label = rec.get('language', None)
	lang_uri = rec.get('language_URI', None)
	if lang_label:
		what.language = model.Language(ident=lang_uri, label=lang_label)

	return what

def construct_facet(facet):

	# Should title and publication be LinguisticObjects ?

	ftypl = facet.get('facet_type_label', '').lower().strip()
	ftyp = facet.get('facet_type', '').lower().strip()
	if not ftyp and not ftypl:
		# No facet type or facet type label, bail
		return None
	if ftyp in ['place'] or ftypl in ['place']:
		what = model.Place()
	elif ftyp in ['person'] or ftypl in ['person']:
		what = model.Person()
	elif ftyp in ['topic'] or ftypl in ['topic', 'occupation', 'function', 'form']:
		what = model.Type()

		# XXX classify what sort of Type

	elif ftyp in ['period'] or ftypl in ['period', 'date']:
		what = model.Period()
	elif ftyp in ['genre'] or ftypl in ['genre']:
		what = model.Type()
	elif ftyp in ['organization', 'culture', 'family', 'meeting'] or ftypl in ['organization', 'culture']:
		what = model.Group()

		# XXX classify what sort of group

	elif ftyp in ['title', 'publication']:
		what = model.LinguisticObject()
		# This might be a manuscript or a text ... play it safe with text
	else:
		print(f"Unknown facet type: {ftyp} / {ftypl} ")
		what = model.Type()

	fdisp = facet.get('facet_display', [])
	for fd in fdisp:
		txt = construct_text(fd, model.Name)
		if txt:
			what.identified_by = txt

	if not hasattr(what, 'identified_by'):
		# a facet with no label ... bail
		return None

	# Now set the URI
	name_map = name_maps.get(what.__class__, None)
	if name_map is not None:
		name = what.identified_by[0].content
		u = name_map[name]  # such a simple line...
		what.id = f"urn:uuid:{u}"

	rolel = facet.get('facet_role_label', '').lower()
	rolec = facet.get('facet_role_code', '').lower()
	roleu = facet.get('facet_role_URI', [])
	if roleu:
		roleu = roleu[0]

	# XXX What to do with these?


	return what


def transform_json(record, fn):
	#sys.stdout.write('.')
	#sys.stdout.flush()

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
	if len(nst) == 1:
		st = nst[0][0]
	else:
		# check multiple super types :S
		# print(f"- XXX - multiple supertypes: {nst} / {fn}")
		# Default to the first...
		st = nst[0][0]

	clss = st_to_class.get(st, model.HumanMadeObject)
	main = clss(ident=f"http://lux.yale.edu/resource/{md_identifier}")

	if clss == model.HumanMadeObject:
		# Construct a VisualItem to have the subject
		vi = model.VisualItem(ident=f"http://lux.yale.edu/visual/{md_identifier}")
		main.shows = vi		

	# Add lowest Supertype to instance
	for stl in nst:
		st = stl[-1]
		sturi = st_uri.get(st, f"http://lux.yale.edu/supertype/{st}")
		t = model.Type(ident=sturi, label=st)
		main.classified_as = t

	specific_type = bd.get('specific_type', [])
	specific_type_uri = bd.get('specific_type_URI', [])

	# Assume if uri, then type
	if specific_type:
		for s in range(len(specific_type)):
			st = specific_type[s]
			if len(specific_type_uri) > s:
				sturi = specific_type_uri[s]
			else:
				sturi = None
			t = model.Type(ident=sturi, label=st)
			main.classified_as = t

	# titles
	titles = record['titles']
	for title in titles:
		ttype_label = title.get('title_type', None)
		ttype_uri = title.get('title_type_URI', None)
		tlabel = title.get('title_label', None)
		tvalues = title.get('title_display', [])

		tt = None
		ttype = model.Name
		title_target = main
		if ttype_label:
			if ttype_label == "primary":
				ttype = vocab.PrimaryName
			elif ttype_label == "sort":
				ttype = vocab.SortName
			elif ttype_label == "contents":
				# title of a part of the work
				# make a part, using main's class
				part = main.__class__()
				if main.__class__ == model.Set:
					main.member = part
				else:
					main.part = part
				title_target = part
			elif ttype_label == "work related":
				rel = model.LinguisticObject()
				# XXX -- what relationship to the main is this? main.related = rel
				title_target = rel
			else:
				tt = model.Type(ident=ttype_uri, label=ttype_label)

		for tval in tvalues:
			# construct text
			title = construct_text(tval, ttype)
			if title:
				if tt:
					title.classified_as = tt
				title_target.identified_by = title

	# identifiers
	identifiers = record.get('identifiers', [])
	for i in identifiers:
		ival = i.get('identifier_value', '')
		if ival:
			ident = model.Identifier(content=ival)			
			ityp = i.get('identifier_type', None)
			if ityp:
				ityp_uri = i.get('identifier_type_URI', [None])
				ident.classified_as = model.Type(ident=ityp_uri[0], label=ityp)
			idisp = i.get('identifier_display', '')
			if idisp and idisp != ival:
				iname = vocab.DisplayName(content=idisp)
				ident.identified_by = iname
			main.identified_by = ident

	# basic_descriptor notes
	if 'edition_display' in bd:
		for ed in bd['edition_display']:
			txt = construct_text(ed, vocab.EditionStatement)
			if txt:
				main.referred_to_by = txt
	if 'imprint_display' in bd:
		for imp in bd['imprint_display']:
			txt = construct_text(imp, vocab.ProductionStatement)
			if txt:
				main.referred_to_by = txt
	if 'materials_display' in bd:
		for md in bd['materials_display']:
			txt = construct_text(md, vocab.MaterialStatement)
			if txt:
				main.referred_to_by = txt
	if 'inscription_display' in bd:
		for ins in bd['inscription_display']:
			txt = construct_text(ins, vocab.InscriptionStatement)
			if txt:
				main.referred_to_by = txt
	if 'provenance_display' in bd:
		for pd in bd['provenance_display']:
			txt = construct_text(pd, vocab.ProvenanceStatement)
			if txt:
				main.referred_to_by = txt
	if 'acquisition_source_display' in bd:
		for asd in bd['acquisition_source_display']:
			txt = construct_text(asd, vocab.AcquisitionStatement)
			if txt:
				main.referred_to_by = txt

	# materials
	mats = bd.get('materials_type', [])
	mat_uris = bd.get('materials_type_URI', [])
	if mats and clss is not model.HumanMadeObject:
		# print(f"Attempting to set materials on {clss} in {fn}")
		pass
	else:
		for m in range(len(mats)):
			mlbl = mats[m]
			if len(mat_uris) > m:
				muri = mat_uris[m]
			else:
				muri = None
			main.made_of = model.Material(ident=muri, label=mlbl)

	# notes
	notes = record.get('notes', [])
	for n in notes:
		nt = n.get('note_type', None)
		noteType = vocab.Note
		ntype = None
		if nt:
			if nt == "300":
				noteType = vocab.PhysicalStatement
			elif nt == "245c":
				noteType = vocab.AttributionStatement
			elif nt == "500":
				pass
			elif nt == "533":
				noteType = vocab.ReproductionStatement
			elif nt == "504": 
				noteType = vocab.BibliographyStatement
			else:
				ntype = model.Type(label=nt)
		nl = n.get('note_label', None)
		if nl and nl.lower() != nt.lower():
			ndisp = vocab.DisplayName(content=nl)
		else:
			ndisp = None
		for d in n.get('note_display', []):
			note = construct_text(d, vocab.Note)
			if note:
				if ntype:
					note.classified_as = ntype
				if ndisp:
					note.identified_by = ndisp
				main.referred_to_by = note

	# citations // as notes
	cites = record.get('citations', [])
	for c in cites:
		cd = c.get('citation_string_display', [])
		for cite in cd:
			txt = construct_text(cite, vocab.Citation)
			if txt:
				main.referred_to_by = txt
		if 'citation_identifier_value' in c:
			# print(f"Found civ: {c['citation_identifier_value']} {c['citation_identifier_type']}")
			pass
		if 'citation_URI' in c:
			print(f"Found c_URI: {c}")

	# locations
	locations = record.get('locations', [])

	done_colls = []
	done_refs = []
	for loc in locations:
		ards = loc.get('access_in_repository_display', [])		
		for ard in ards:
			txt = construct_text(ard, vocab.AccessStatement)
			if txt:
				main.referred_to_by = txt
		refs = loc.get('access_in_repository_URI', [])
		for r in refs:
			if r and not r in done_refs:
				done_refs.append(r)
				main.subject_of = vocab.WebPage(ident=r)
		colls = loc.get('collections', [])
		for c in colls:
			if c and not c in done_colls:
				done_colls.append(c)
				ident = set_map[c]
				coll = vocab.CollectionSet(ident=f"urn:uuid:{ident}")
				coll.identified_by = model.Name(content=c)
				main.member_of = coll
		yhi = loc.get('yul_holding_institution', [])
		for y in yhi:
			if y:
				ident = set_map[y]
				coll = vocab.CollectionSet(ident=f"urn:uuid:{ident}")
				coll.identified_by = model.Name(content=f"Holdings of {y}")
				main.member_of = coll
		cd = loc.get('campus_division', [])
		if not yhi and cd:
			for c in cd:
				ident = set_map[c]
				coll = vocab.CollectionSet(ident=f"urn:uuid:{ident}")
				coll.identified_by = model.Name(content=f"Holdings of {c}")
				main.member_of = coll
		lcn = loc.get('location_call_number', "")
		if lcn:
			cn = vocab.CallNumber(content=lcn)
			# Assign to the division as a group
			aa = model.AttributeAssignment()
			who = cd[0]
			ident = group_map[who]
			aa.carried_out_by = model.Group(ident=f"urn:uuid:{ident}", label=who)
			cn.assigned_by = aa
			main.identified_by = cn

	# measurements
	measurements = record.get('measurements', [])
	for mez in measurements:
		lbl = mez.get('measurement_label', '')
		forms = mez.get('measurement_form', [])
		for form in forms:
			dnames = form.get('measurement_display', [])
			for d in dnames:
				txt = construct_text(d, vocab.DimensionStatement)
				if txt:
					if lbl:
						txt.identified_by = vocab.DisplayName(content=lbl)
					main.referred_to_by = txt
			aspects = form.get('measurement_aspect', [])
			for a in aspects:
				at = a.get('measurement_type', None)
				atu = a.get('measurement_type_URI', None)
				if atu:
					atu = atu[0]
				au = a.get('measurement_unit', None)
				auu = a.get('measurement_unit_URI', None)				
				if auu:
					auu = auu[0]
					if type(auu) == list:
						auu = auu[0]
				av = a.get('measurement_value', None)

				if not av or not (au or auu):
					# meaningless without both of these
					continue

				try:
					unit = model.MeasurementUnit(ident=auu, label=au)
				except:
					print(f"auu: {auu}")
				d = model.Dimension()
				try:
					d.value = float(av)
				except:
					# broken value, continue
					continue
				d.unit = unit
				if atu or at:
					typ = model.Type(ident=atu, label=at)
					d.classified_as = typ
				main.dimension = d

			# XXX Dunno what to do with this without harmonization
			elem = form.get('measurement_element', "")

	# languages (only for texts)
	if isinstance(main, model.LinguisticObject):
		langs = record.get('languages', [])
		for l in langs:
			luri = l.get('language_URI', [])
			if luri and type(luri) == list:
				luri = luri[0]
			if type(luri) == list:
				if luri:
					luri = luri[0]
				else:
					luri = None
			llbl = l.get('language_display', "")
			lcode = l.get('language_code', '')
			lang = model.Language(ident=luri, label=llbl)
			if lcode:
				lang.identified_by = model.Identifier(content=lcode)
			main.language = lang

	# rights
	rights = record.get('rights', [])
	for r in rights:
		orig_disp = r.get("original_rights_status_display", [])
		for od in orig_disp:
			txt = construct_text(od, vocab.RightsStatement)
			if txt:
				main.referred_to_by = txt

		orig_type_label = r.get( "original_rights_type_label", "")
		orig_type = r.get("original_rights_type", "")
		orig_type_uri = r.get("original_rights_type_URI", "")

		orig_uri = r.get('original_rights_URI', '')
		orig_notes = r.get('original_rights_notes', '')

	# digital_assets
	digass = record.get('digital_assets', [])
	for da in digass:
		da_uris = da.get('asset_URI', [])

		captions = da.get('asset_caption_display', [])
		for c in captions:
			txt = construct_text(c, vocab.Description)
			if txt:
				pass

		da_type = da.get('asset_type', None)
		da_type_URI = da.get('asset_type_URI', [])
		da_flag = da.get('asset_flag', None)
		da_flag_URI = da.get('asset_flag_URI', [])
		#asset_rights_status_display
		#asset_rights_notes
		#asset_rights_type
		#asset_rights_type_URI
		#asset_rights_type_label

	# hierarchies
	hiers = record.get('hierarchies', [])
	for h in hiers:

		htype = h.get('hierarchy_type', "") # "EAD; Series"
		htype_uri = h.get('hierarchy_type_URI', "") 
		root_id = h.get('root_internal_identifier', '') # https://.../resources/417
		ancestor_names = h.get('ancestor_display_names', []) # ["coll of foo"]
		ancestor_ids = h.get('ancestor_internal_identifiers', []) # [".../resources/417"]
		ancestor_uris = h.get('ancestor_URIs', []) 

		descends = h.get('descendant_count', 0)
		sibs = h.get('sibling_count', 0)
		max_depth = h.get('maximum_depth', 0)


	#### Categories of Activity

	# Sponsorship, Commissioning, Mandating of work
	# Production, Design of Object / Writing of Text / Composition of Music
	# Publication and Distribution of Work
	# Performance [& interpretation] of Work
	# Discovery / Collection of Object/Specimen
	# Ownership  / Donor / Selling / Buying / Acquisition (Provenance)
	# Lending / Borrowing / Exhibition
	# Deaccession / Destruction
	# + Related to Content, rather than Activity

	# agents
	agents = record.get('agents', [])
	for a in agents:
		dnames = a.get('agent_display', [])
		sname = a.get('agent_sortname', '')
		if not sname and not dnames:
			continue

		atyp = a.get('agent_type_URI', [])
		if atyp and type(atyp) == list and atyp[0]:
			atyp = atyp[0]
		else:
			atyp = a.get('agent_type_display', None)
			if not atyp:
				if 'ycba' in fn:
					atyp = "organization"
				else:	
					print(f"No type: {dnames}")					
					atyp = "person"
			else:
				atyp = atyp.lower()

		agent_class = agent_type_to_class[atyp]
		agent = agent_class()
		dnvals = []
		if dnames:
			for ad in dnames:
				dnvals.append(ad['value'])
				n = construct_text(ad, model.Name)
				if n:
					agent.identified_by = n

		if sname and not sname in dnvals:
			agent.identified_by = vocab.SortName(content=sname)

		if hasattr(agent, 'identified_by'):
			name = agent.identified_by[0].content
			agent.id = f"urn:uuid:{name_maps[agent_class][name]}"

		auri = a.get('agent_URI', [])
		for au in auri:
			agent.equivalent = agent_class(ident=au)

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

		if not rolel and not rolec and not roleu and not context:
			# An actor with no role, associate in a vanilla activity
			act = model.Activity()
			act.carried_out_by = agent
			main.used_for = act
		else:

			if context == "publication event":
				# Main for this is a work about the object.
				# author, publisher, editor
				# XXX Make below work on a referencing work

				what = model.LinguisticObject()
				main.referred_to_by = what
			else:
				what = main

			# XXX Process "A and B" "A & B"

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

			if not roleu and rolec:
				roleu = f"http://lux/roles/agent/{rolec}"
			else:
				roleu = None

			if rolel in agent_roles_production or (not rolel and context == "production"):
				if isinstance(main, model.HumanMadeObject):
					if hasattr(main, 'produced_by'):
						act = main.produced_by
					else:
						act = model.Production()
						main.produced_by = act
					part = model.Production()
				else:
					if hasattr(main, 'created_by'):
						act = main.created_by
					else:
						act = model.Creation()
						main.created_by = act				
					part = model.Creation()
				act.part = part
				part.carried_out_by = agent
				part.classified_as = model.Type(ident=roleu, label=roleLabel)

			# Publication
			elif rolel in agent_roles_publication:
				act = vocab.Publishing()
				act.carried_out_by = agent
				act.classified_as = model.Type(ident=roleu, label=roleLabel)
				main.used_for = act

			# Discovery
			# XXX This should be a provenance activity in the future
			elif rolel in agent_roles_encounter:
				# collector of specimen, etc
				if clss == model.HumanMadeObject:
					if hasattr(main, 'encountered_by'):
						enc = main.encountered_by[0]
					else:		
						enc = model.Encounter()
						main.encountered_by = enc
					enc.carried_out_by = agent
					enc.classified_as = model.Type(ident=roleu, label=roleLabel)
				elif clss == model.Set:
					if hasattr(main, 'created_by'):
						act = main.created_by
					else:
						act = model.Creation()
						main.created_by = act					
					act.carried_out_by = agent
					act.classified_as = model.Type(ident=roleu, label=roleLabel)
				else:
					print(f"Can't set a collector on type {main.__class__} / {fn}")

			# Sponsor
			elif rolel in agent_roles_sponsor:
				# cause or influence on the production
				if isinstance(main, model.HumanMadeObject):
					if hasattr(main, 'produced_by'):
						act = main.produced_by
					else:
						act = model.Production()
						main.produced_by = act
				else:
					if hasattr(main, 'created_by'):
						act = main.created_by
					else:
						act = model.Creation()
						main.created_by = act				

				part = model.Activity()
				part.classified_as = model.Type(ident=roleu, label=roleLabel)
				part.carried_out_by = agent
				act.influenced_by = part
		
			# Performance
			elif rolel in agent_roles_performance:
				act = vocab.Performance()
				main.used_for = act
				act.classified_as = model.Type(ident=roleu, label=roleLabel)
				act.carried_out_by = agent

			# Provenance
			elif rolel in agent_roles_provenance or context in ["provenance", "acquisition"]:

				# "copyright holder"
				# "owner"
				# "current owner"
				# "former owner" 
				# "previous owner
				# "donor"

				# "client"
				# "patron"
				# "source"

				# "licensee"

				# "purchaser"
				# "exchanger"

				# "auctioneer"
				# "licensor"
				# "vendor"

				# "land owner"




				pass

			# Exhibitions
			elif rolel in agent_roles_exhibition or context == "exhibition":
				pass

			# Other
			elif rolel in agent_roles_other:
				pass

			else:
				print(f"Unknown agent role: {rolel} / {rolec} / {roleu} / {context}")
				try:
					agent_roles_missed[rolel] += 1
				except:
					agent_roles_missed[rolel] = 1


	# places
	places = record.get('places', [])
	for p in places:
		place = model.Place()
		pnames = p.get('place_display', [])
		if not pnames or (pnames and len(pnames) == 1 and not pnames[0]['value']):
			continue
		else:
			for pn in pnames:
				txt = construct_text(pn, model.Name)
				if txt:
					place.identified_by = txt
		puris = p.get('place_URI', [])
		for pu in puris:
			place.equivalent = model.Place(ident=pu)

		if hasattr(place, 'identified_by'):
			name = place.identified_by[0].content
			place.id = f"urn:uuid:{place_map[name]}"

		# Only in YPM?
		pts = p.get('place_type_display', "")
		pturis = p.get('place_type_URI', [])
		# continent, country, stateprovince, county, locality
		if pts and not pts in ["continent", "country", "stateprovince", "county", "locality", "waterbody"]:
			print(f"pts: {pts}")

		# Only for maps?
		coords = p.get("place_coordinates_display", "")
		if coords:
			place.defined_by = coords

		rolel = p.get('place_role_label', '').lower()
		rolec = p.get('place_role_code', '').lower()
		roleu = p.get('place_role_URI', [])
		if roleu:
			roleu = roleu[0] # could still be ""

		if rolel.startswith('made, '):
			rolel = "made"
		elif rolel.startswith('found, '):
			rolel = "found"

		if not rolel and not rolec and not roleu:
			# anonymous activity?
			print(f"no place role in {fn}")
		elif rolel in place_roles_production:

			if isinstance(main, model.HumanMadeObject):
				if hasattr(main, 'produced_by'):
					act = main.produced_by
				else:
					act = model.Production()
					main.produced_by = act
			else:
				if hasattr(main, 'created_by'):
					act = main.created_by
				else:
					act = model.Creation()
					main.created_by = act
			act.took_place_at = place
		elif rolel in place_roles_publication:
			# Find the publication activity
			if not hasattr(main, 'used_for'):
				pub = vocab.Publishing()
				main.used_for = pub
			else:
				pub = None
				for p in main.used_for:
					if isinstance(p, vocab.Publishing):
						pub = p
						break
				if not pub:
					pub = vocab.Publishing()
					main.used_for = pub
			pub.took_place_at = place

		elif rolel in place_roles_encounter:

			if clss == model.HumanMadeObject:
				if hasattr(main, 'encountered_by'):
					enc = main.encountered_by[0]
				else:		
					enc = model.Encounter()
					main.encountered_by = enc
				enc.took_place_at = place
			elif clss == model.Set:
				if hasattr(main, 'created_by'):
					act = main.created_by
				else:
					act = model.Creation()
					main.created_by = act					
				act.took_place_at = place
			else:
				print(f"Can't set a collection place on type {main.__class__} / {fn}")

		elif rolel in place_roles_provenance:
			pass

		elif rolel in place_roles_other:
			pass

		else:
			print(f"Unknown Place role {rolel} / {rolec} / {roleu}")
			try:
				place_roles_missed[rolel] += 1
			except:
				place_roles_missed[rolel] = 1

	# dates
	dates = record.get('dates', [])
	for dt in dates:

		rolel = dt.get('date_role_label', '').lower()
		rolec = dt.get('date_role_code', '').lower()
		roleu = dt.get('date_role_URI', [])
		if roleu:
			roleu = roleu[0] # could still be ""

		dnames = dt.get('date_display', [])
		botb = dt.get('date_earliest', None)
		eote = dt.get('date_latest', None)
		if not botb:
			botb = dt.get('year_earliest', None)
		if not eote:
			eote = dt.get('year_latest', None)

		if not dnames and not botb and not eote:
			continue
		ts = model.TimeSpan()
		if botb:
			ts.begin_of_the_begin = botb
		if eote:
			ts.end_of_the_end = eote
		if dnames:
			for d in dnames:
				txt = construct_text(d, vocab.DisplayName)
				if txt:
					ts.identified_by = txt

		if not rolel and not rolec and not roleu:
			# XXX assume production???
			continue
		if rolel in date_roles_production:
			if isinstance(main, model.HumanMadeObject):
				if hasattr(main, 'produced_by'):
					act = main.produced_by
				else:
					act = model.Production()
					main.produced_by = act
			else:
				if hasattr(main, 'created_by'):
					act = main.created_by
				else:
					act = model.Creation()
					main.created_by = act
			if hasattr(act, 'timespan'):
				if isinstance(main, model.Set):
					act = vocab.Assembling()
					main.used_for = act				
			act.timespan = ts		

		elif rolel in date_roles_publication:
			# Find the publication activity
			if not hasattr(main, 'used_for'):
				pub = vocab.Publishing()
				main.used_for = pub
			else:
				pub = None
				for p in main.used_for:
					if isinstance(p, vocab.Publishing):
						pub = p
						break			
				if not pub:
					pub = vocab.Publishing()
					main.used_for = pub
			pub.timespan = ts
		elif rolel in date_roles_encounter:
			if clss == model.HumanMadeObject:
				if hasattr(main, 'encountered_by'):
					enc = main.encountered_by[0]
				else:		
					enc = model.Encounter()
					main.encountered_by = enc
				enc.timespan = ts
			elif clss == model.Set:
				if hasattr(main, 'created_by'):
					act = main.created_by
				else:
					act = model.Creation()
					main.created_by = act					
				act.timespan = ts
			else:
				print(f"Can't set a collection place on type {main.__class__} / {fn}")			

		elif rolel in date_roles_performance:
			if not hasattr(main, 'used_for'):
				perf = vocab.Performance()
				main.used_for = perf
			else:
				perf = None
				for p in main.used_for:
					if isinstance(p, vocab.Performance):
						perf = p
						break			
				if not perf:
					perf = vocab.Performance()
					main.used_for = perf
			perf.timespan = ts
		elif rolel in date_roles_provenance:
			pass
		elif rolel in date_roles_exhibition:
			pass
		elif rolel in date_roles_other:
			pass

		else:
			print(f"Unknown date role: {rolel} / {rolec} / {roleu}")
			try:
				date_roles_missed[rolel] += 1
			except:
				date_roles_missed[rolel] = 1

	# subjects
	subjects = record.get('subjects', [])
	if subjects:
		if clss == model.HumanMadeObject:
			target = main.shows[0]
		elif clss in [model.LinguisticObject, model.InformationObject, model.DigitalObject, model.Set]:
			target = main

	for sub in subjects:
		suri = sub.get('subject_heading_URI', [])
		if suri:
			suri = suri[0]
		shd = sub.get('subject_heading_display', [])
		# shs = sub.get('subject_heading_sortname', '')
		facets = sub.get('subject_facets', [])

		subject = None
		if len(facets) == 1:
			# Just do the one facet
			subject = construct_facet(facets[0])			
			if subject:
				if suri:
					subject.equivalent = subject.__class__(ident=suri)
		else:
			# Construct an overall type and then associate the facets with it
			subject = model.Type()
			if suri:
				subject.equivalent = model.Type(ident=suri)
			for d in shd:
				txt = construct_text(d, model.Name)
				if txt:
					subject.identified_by = txt
			#if shs:
			#	subject.identified_by = vocab.SortName(content=shs)

			if facets:
				cre = model.Creation()
				subject.created_by = cre
				for facet in facets:
					f = construct_facet(facet)
					if f:
						cre.influenced_by = f
		if subject:
			target.about = subject

	return main


def process_jsonl(fn):
	ofn = fn.replace('../../lux/', '')
	ofn = ofn.replace('.jsonl', '-jsonld.jsonl')
	outfn = os.path.join(dest, ofn)
	if NO_OVERWRITE and os.path.exists(outfn):
		return 0
	x = 0
	lines = []
	with open(fn) as fh:
		for l in fh.readlines():
			try:
				js = json.loads(l)
				x += 1
			except:
				print(f"Bad JSON in {fn} line {x}")
				continue
			rec = transform_json(js, f"{fn}-{x}")
			recstr = model.factory.toString(rec, compact=True)			
			lines.append(recstr)
	# Now serialize out
	fh = open(outfn, 'w')
	for l in lines:
		fh.write(l + '\n')
	fh.close()
	return x

def process_json(fn):
	ofn = fn.replace('../../lux/', '')
	ofn = ofn.replace('.json', '-jsonld.json')
	outfn = os.path.join(dest, ofn)
	if NO_OVERWRITE and os.path.exists(outfn):
		return 0
	with open(fn) as fh:
		try:
			js = json.load(fh)
		except:
			print(f"Bad JSON in {fn}")
			return 0
		rec = transform_json(js, fn)
		recstr = model.factory.toString(rec, compact=True)	
		# And write it
		fh = open(outfn, 'w')
		fh.write(recstr)
		fh.close()		
	return 1

total = 0
units = os.listdir(source)
units.sort()
last_report = 0
report_every = 9999
start = time.time()
for unit in units[1:2]:
	unitfn = os.path.join(source, unit)
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
			print(f"Processed {total} in {secs} at {total/secs}/sec") 

