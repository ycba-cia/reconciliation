import os
import re
import uuid
import sys
import pathlib
import json
import time
from io import BytesIO

from cromulent import model, vocab
from cromulent.extract import date_cleaner
from lmdb_utils import LMDB

DEBUG = False
NO_OVERWRITE = False

source = "../../lux"  # S3 buckets for v9
# lux/ils/*
# lux/aspace/<dir>/*
# lux/ycba
# lux/ypm
# lux/yuag
dest = "../output_lux"


vocab.register_vocab_class("AccessStatement", {"parent": model.LinguisticObject, "id": "300133046", "label": "Access Statement", "metatype": "brief text"})
vocab.register_vocab_class("CallNumber", {"parent": model.Identifier, "id": "300311706", "label": "Call Number"})
vocab.register_vocab_class("Performance", {"parent": model.Activity, "id": "300069200", "label": "Performance"})
vocab.register_vocab_class("AttributionStatement", {"parent": model.LinguisticObject, "id": "300056109", "label": "Attribution Statement", "metatype": "brief text"})
vocab.register_vocab_class("ReproductionStatement", {'parent': model.LinguisticObject, "id":"300411336", "label": "Reproduction Statement", "metatype": "brief text"})
vocab.register_vocab_class("CartographicStatement", {'parent': model.LinguisticObject, 'id':'300053163', 'label': "Cartography Note", 'metatype': 'brief text'})
# Note that 28052 is used below for maps/globes/atlases supertype, and don't want them to collide
# vocab.register_vocab_class("LanguageStatement", {'parent': model.LinguisticObject, 'id':'300435433', 'label': "Languages Note", 'metatype': 'brief text'})
vocab.register_vocab_class('IndexingStatement', {'parent': model.LinguisticObject, 'id':'300054640', 'label': "Indexes Note", 'metatype': 'brief text'})
vocab.register_vocab_class('PreferCiteNote', {'parent': model.LinguisticObject, 'id':'300311705', 'label': "Preferred Citation Note", 'metatype': 'brief text'})
# Note that 311705 is also Citation ... so can't distinguish a note about citations of this thing (but is that bibliography?) vs how to cite this thing
vocab.register_vocab_class('ArrangementNote', {'parent': model.LinguisticObject, 'id':'300XXXXX2', 'label': "Contents Organization/Arrangement Note", 'metatype': 'brief text'})
vocab.register_vocab_class('DigitalFileNote', {'parent': model.LinguisticObject, 'id':'300266011', 'label': "Digital Characteristics Note", 'metatype': 'brief text'})
vocab.register_aat_class("SeparatedMaterialStatement", {"parent": model.LinguisticObject, "id": "300053748", "label": "Separated Material", 'metatype': 'brief text'})
vocab.register_aat_class("ProcessingInfoStatement", {"parent": model.LinguisticObject, "id": "300077565", "label": "Processing Information", 'metatype': 'brief text'})
vocab.register_aat_class("PhysTechStatement", {"parent": model.LinguisticObject, "id": "300221194", "label": "Physical Properties and Technical Requirements", 'metatype': 'brief text'})
vocab.register_aat_class("AccrualsStatement", {"parent": model.LinguisticObject, "id": "300055458", "label": "Accruals", 'metatype': 'brief text'})
vocab.register_aat_class("PhysicalLocationStatement", {"parent": model.LinguisticObject, "id": "300248479", "label": "Location", 'metatype': 'brief text'})

vocab.register_aat_class("AlternativeTitle", {"parent": model.Name, 'id':'300417226', 'label': "Alternative Title"})
vocab.register_aat_class("PublishedTitle", {"parent": model.Name, 'id':'300417206', 'label': "Published Title"})
vocab.register_aat_class("CollectiveTitle", {"parent": model.Name, 'id':'300417198', 'label': "Collective Title"})
vocab.register_aat_class("InscribedTitle", {"parent": model.Name, 'id':'300417202', 'label': "Inscribed Title"})
vocab.register_aat_class("GivenTitle", {"parent": model.Name, 'id':'300417201', 'label': "Given Title (by maker)"})
vocab.register_aat_class("SeriesTitle", {"parent": model.Name, 'id':'300417214', 'label': "Series Title"})
vocab.register_aat_class("TranscribedTitle", {"parent": model.Name, 'id':'300404333', 'label':"Transcribed Title"})
vocab.register_aat_class("ConstructedTitle", {"parent": model.Name, 'id':'300417205', 'label':"Constructed/Computed Title"})
vocab.register_aat_class("InheritedTitle", {"parent": model.Name, 'id':'300XXXXX3', 'label':"Inherited Title"})
vocab.register_aat_class("DescriptiveTitle", {"parent": model.Name, 'id':'300417199', 'label':"Descriptive Title"})

vocab.register_aat_class("OccupationType", {"parent": model.Type, 'id':'300263369', 'label':"Occupation"})
vocab.register_aat_class("FunctionType", {"parent": model.Type, 'id':'300138088', 'label':"Function"})
vocab.register_aat_class("FormType", {"parent": model.Type, 'id':'300226816', 'label':"Form"})
vocab.register_aat_class("GenreType", {"parent": model.Type, 'id':'', 'label':""})
vocab.register_aat_class("OrganizationGroup", {"parent": model.Group, 'id':'300025948', 'label':"Organization"})
vocab.register_aat_class("FamilyGroup", {"parent": model.Group, 'id':'300055474', 'label':"Family"})
vocab.register_aat_class("CultureGroup", {"parent": model.Group, 'id':'300387171', 'label':"Culture"})
vocab.register_aat_class("MeetingGroup", {"parent": model.Group, 'id':'300054788', 'label':"Meeting"})


model.factory.auto_assign_id = False
if not DEBUG:
	model.factory.validate_properties = False
	model.factory.validate_profile = False
	model.factory.validate_range = False
	model.factory.validate_multiplicity = False
	model.factory.json_serializer = "fast"
	model.factory.order_json = False

# LMDBs that map name --> uuid
class NameUuidDB(LMDB):

	def get(self, key):
		# name --> uuid, and generate a uuid if not present
		# ensure key is lowercase and stripped
		key = key.strip().lower()
		if len(key) == 0:
			return None
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

person_map = NameUuidDB('lux_person_db', open=True, map_size=int(4e9))
group_map = NameUuidDB('lux_group_db', open=True, map_size=int(2e9))
type_map = NameUuidDB('lux_type_db', open=True, map_size=int(2e9))   # including material, language, type, currency, unit
place_map = NameUuidDB('lux_place_db', open=True, map_size=int(2e9))
period_map = NameUuidDB('lux_period_db', open=True, map_size=int(2e9))
event_map = NameUuidDB('lux_event_db', open=True, map_size=int(2e9))
text_map = NameUuidDB('lux_text_db', open=True, map_size=int(2e9))
set_map = NameUuidDB('lux_set_db', open=True, map_size=int(1e9))
uri_map = NameUuidDB('lux_uri_db', open=True, map_size=int(2e9))

counts = {}
ead_uri_map = {}

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


note_type_class = {
	"245c": vocab.AttributionStatement,
	'255': vocab.CartographicStatement,	
	"300": vocab.PhysicalStatement,
	'347': vocab.DigitalFileNote,
	'351': vocab.ArrangementNote,
	'382': vocab.MaterialStatement, # medium of performance -- material is also technique
	'500': None,
	'504': vocab.BibliographyStatement,
	'510': vocab.Citation,
	'516': vocab.DigitalFileNote,
	'520': vocab.Abstract,
	'524': vocab.PreferCiteNote, 
	'533': vocab.ReproductionStatement,
	'545': vocab.BiographyStatement,
	'546': vocab.LanguageStatement,
	'555': vocab.IndexingStatement,
	'561': vocab.ProvenanceStatement,
	'590': None,
	'note': None,
	'curatorial comment': None,
	'cataloguing': None,
	'odd': None,
	'mark(s)': vocab.MarkingsStatement,
	'signed': vocab.SignatureStatement,
	'description': vocab.Description,
	'container': vocab.PhysicalLocationStatement,
	'physdesc': vocab.PhysicalStatement,
	'scopecontent': vocab.Description,
	'abstract': vocab.Abstract,
	'prefercite': vocab.PreferCiteNote,
	'bioghist': vocab.BiographyStatement,
	'arrangement': vocab.ArrangementNote,
	'relatedmaterial': None,
	'materialspec': None,
	'altformavail': None,
	'otherfindaid': None,
	'fileplan': None,
	'separatedmaterial': vocab.SeparatedMaterialStatement,
	'langmaterial': vocab.LanguageStatement,
	'processinfo': vocab.ProcessingInfoStatement,
	'physloc': vocab.PhysicalLocationStatement,
	'phystech': vocab.PhysTechStatement,
	'appraisal': vocab.ConditionStatement,
	'originalsloc': None,
	'index': vocab.IndexingStatement,
	'accruals': vocab.AccrualsStatement,
	'bibliography': vocab.BibliographyStatement
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

agent_roles_production = {
	"creator": "aat:300386174",
	"contributor" : "aat:300403974",
	"author": "aat:300025492",
	"artist": "aat:300025103",
	'originator': "aat:300386177",
	"fabricator": "aat:300251917",
	"editor": "aat:300025526",
	"compiler": "aat:300121766",
	"illustrator": "aat:300025123",
	"abridger": "aat:300440753",
	"cartographer": "aat:300025593",
	"arranger": "aat:300025667",
	"translator": "aat:300025601",
	"lyricist": "aat:300025675",
	"composer": "aat:300025671",
	"calligrapher": "aat:300025107",
	"adapter": "aat:300410355",
	"printer": "aat:300025732",
	"printer_GROUP": "aat:300386347",
	"carver": "aat:300025256",
	"engraver": "aat:300239410",
	"wood-engraver": "aat:300025167",
	"lithographer": "aat:300025175",
	"etcher": "aat:300025174",
	"decorator": "aat:300435238",
	"sculptor": "aat:300025181",
	"inscriber": "aat:300121785",
	"modeler": "aat:300025417",
	"woodcutter": "aat:300025178",
	"printmaker": "aat:300025164",
	"printmaker_GROUP": "aat:300312299",
	"printer of plates": "aat:300025733",
	'photographer': "aat:300025687",
	'mint': "aat:300205362",  # minters, people as opposed to the building which is 300006031
	'manufacturer': "aat:300025230",
	'typographer': "aat:300025745",
	'stereotyper': "aat:300025743",
	'designer': "aat:300025190",
	'typesetter': "aat:300025744",
	'compositor': "aat:300025708",
	'architect': "aat:300024987",
	'architect_GROUP': "aat:300312082",
	'inventor': "aat:300025845",
	'type designer': "aat:300417840",
	'letterer': "aat:300025115",
	'colorist': "aat:300435165",
	'binder': "aat:300025704",
	'papermaker': "aat:300025344",
	'scribe': "aat:300025580",
	'screenwriter': "aat:300025515",
	'adapter': "aat:300410355",
	'bookbinder': "aat:300025704",
	'transcriber': "aat:300440751",
	'copyist': "aat:300025189",
	'draftsman': "aat:300112172",
	'illuminator': 'aat:300025122',
	"paper maker": "aat:300025344",
	"animator": "aat:300025646",
	"proofreader": "aat:300418027v",
	"chromo-lithographer": "aat:300251177",
	"book artist": "aat:300386346",
	"clockmaker": "aat:300025397",
	"potter": "aat:300025414",
	"gilder": "aat:300025261",
	"diesinker": "aat:300386327",

	"editor in chief": "",
	'electrotyper': "",
	'book designer': "",
	'bookplate designer': "",
	'cover designer': "",
	'book jacket designer': '',
	'binding designer': "",
	'organizer': "",
	'book producer': "",
	'mounter': "",
	'fore-edge painter': "",  # aat:300263620 is the technique fore-edge painting
	"paper engineer": "",  # aat:300260338 is the technique
	"collotyper": "",
	"series editor": "",
	"casemaker": "",
	"repairer": "",
	"metal-engraver": "",	
	"penciller": "",
	"inker": "",
	"cover artist": "",
	"after": "",
	"playing card maker": "",
}

agent_roles_sponsor = {
	"commissioned by": "aat:300400903",
	"sponsor": "aat:300188572",
	"sponsoring body": "aat:300400903",
	"degree granting institution": "",
	"commissioning body": "aat:300400903",
	"funder": "aat:300188572",
	"funder/sponsor": "aat:300188572",
	"sponsoring institution": "aat:300188572",
	"degree supervisor": "",
	"host institution": "",
}

agent_roles_publication = {
	"publisher": "aat:300025574",
	"publisher_GROUP": "aat:300386627", 
	"issuing body": "aat:300386627",
	"distributor": "aat:300404885",
	"bookseller": "aat:300025244",
	"retailer": "aat:300025246",
	"broadcaster": "aat:300025502",
	"film distributor": "",
	"issuer": "",
	"bank": "",
	"enacting jurisdiction": "",
}

agent_roles_performance = {
	"performer": "aat:300068931",
	"conductor": "aat:300025672",
	"librettist": "aat:300025674",
	"singer": "aat:300025684",
	"tenor": "aat:300206746", # the range not a role but close enough
	"soprano": "aat:300206741",
	"narrator": "aat:300417254",
	"pianist": "aat:300235018",
	"musician": "aat:300025666",
	"orchestra": "aat:300025666", # musician
	"stage director": "300312210",
	"violoncellist": "aat:300235047",
	"director": "aat:300025654",
	"actor": "aat:300025658",
	"film director": "aat:300312209",
	"producer": "aat:300197742",
	"dancer": "aat:300025653",
	"choreographer": "aat:300025649",
	"vocalist": "aat:300025684",  # same as singer
	"instrumentalist": "aat:300162131",
	"cinematographer": "aat:300025650",
	"film editor": "aat:300386237",
	"production company": "aat:300419391",
	"lighting designer": "aat:300386275",
	"costume designer": "aat:300163428",
	"bass": "aat:300206743",
	"violist": "aat:300235045",
	"guitarist": "aat:300235042",
	"set designer": "aat:300435127",
	"trumpet player": "aat:300235064",
	"organist": "aat:300235015",
	"film producer": "aat:300312211",
	"production designer": "aat:300435129",
	"videographer": "aat:300263895",
	"filmmaker": "aat:300075154",

	"on-screen presenter": "",
	"on-screen participant": "",
	"script": "",
	"interviewer": "",
	"interpreter": "",
	"interviewee": "",
	"praeses": "",
	"saxophonist": "",
	"production personnel": "",
	"production": "",
	"camera": "",
	"television director": "",	
	"television producer": "",
	"hornist": "",
	"trombonist": "",
	"recording engineer": "",
	"stage manager": "",
	"video designer": "",
	"direction": "",
	"music": "",
	"clarinet": "",
	"boy soprano": "",
	"voice actor": "",
}

agent_roles_encounter = {
	"collector": "aat:300025234",
	"art collector": "",
	"expedition": "", # aat:300069799
	"discoverer": "" # aat:300404386 
}

agent_roles_provenance = {
	"donor": "aat:300025240",
	"owner": "aat:300203630",
	"current owner": "",
	"former owner": "",
	"previous owner": "",	
	"land owner": "",
	"client": "aat:300025833",
	"patron": "aat:300115251",
	"auctioneer": "aat:300025208",
	"commissaire-priseur": "aat:300412173",	
	"purchaser": "aat:300025211",
	"vendor": "aat:300150791",
	"exchanger": "",
	"licensor": "",
	"licensee": "",
	"source": "",
	"copyright holder": "",	
}

agent_roles_exhibition = {
	"borrower": "aat:300311675",
	"exhibition": "",
	"lender": ""
}

agent_roles_other = {
	"correspondent": "aat:300225705",
	"judge": "aat:300025625",
	"plaintiff": "aat:300440758",
	"dedicatee": "aat:300121765",
	"curator": "aat:300025633c",
	"magistrate": "aat:300025467",
	"proprietor": "aat:300025241",
	"advertiser": "aat:300252554",
	"signer": "aat:300137375",
	"pressman": "aat:300025731",
	"reporter": "aat:300025508",
	"speaker": "aat:300136462",
	"honoree": "aat:300404867",
	"conservator": "aat:300102842",
	"reviewer": "aat:300440750",
	"court reporter": "aat:300136440",
	"pseud": "aat:300404657",
	"surveyor": "aat:300025100",
	"teacher": "aat:300025529",

	"respondent": "",
	"defendant": "",
	"appellee": "",
	"associated name": "",
	"addressee": "",
	"witness": "",
	"appellant": "",
	"recipient": "",
	"complainant": "",
	"petitioner": "",
	"claimant": "",
	"sender": "",
	"user": "",
	"commentator": "",
	"assignee": "",
	"annotator": "",
	"auxilium": "",
	"other": "",
	"subject of parody": "",
	"presenter": "",
	"digitiser": "",
	"panelist": "",
	"moderator": "",

	"editor and translator": "",
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
	'publisher': '',
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

# identifiers
identifier_classes = {
	'isbn': vocab.IsbnIdentifier,
	'issn': vocab.IssnIdentifier,
	'accession number': vocab.AccessionNumber,
	'system': vocab.SystemNumber,
	'TMS object identifier': vocab.SystemNumber,
	'YCBA blacklight identifier': vocab.LocalNumber,
	'catalog number': vocab.AccessionNumber,
	'identification number': vocab.SystemNumber,
	'ead': vocab.LocalNumber,
	'call number': vocab.CallNumber,
	'series unitid': vocab.LocalNumber,
	'voyager bib id': vocab.SystemNumber,
	'unitid': vocab.LocalNumber,
	'recordgrp unitid': vocab.LocalNumber,
}


# These might be better as Identifiers that are assigned, rather than types?
identifier_types = {
	"lccn": model.Type(ident=f"urn:uuid:{type_map['lccn']}", label="Library of Congress Control Number"),
	"lc class number": model.Type(ident=f"urn:uuid:{type_map['lc class number']}", label="Library of Congress Classification Number"),
	"oclc": model.Type(ident=f"urn:uuid:{type_map['oclc number']}", label="OCLC Number"),
	'publisher distributor number': model.Type(ident=f"urn:uuid:{type_map['publisher distributor number']}", label="Publisher/Distributor Number") # 028$b gives the publisher name
}

title_classes = {
	"primary": vocab.PrimaryName,
	"primary title": vocab.PrimaryName,
	"sort": vocab.SortName,
	"alternative": vocab.AlternativeTitle,
	"alternate": vocab.AlternativeTitle,
	"alternative title": vocab.AlternativeTitle,
	"series transcribed": vocab.TranscribedTitle,
	"transcribed": vocab.TranscribedTitle,
	"translated title": vocab.TranslatedTitle,
	"given title": vocab.GivenTitle,
	"inscribed title": vocab.InscribedTitle,
	"project/collective title": vocab.CollectiveTitle,
	"series": vocab.SeriesTitle,
	"portfolio/series title": vocab.SeriesTitle,
	"inherited": vocab.InheritedTitle,
	"published title": vocab.PublishedTitle,
	"descriptive title": vocab.DescriptiveTitle
}

bdnote_classes = {
	'edition_display': vocab.EditionStatement,
	'imprint_display': vocab.ProductionStatement,
	'materials_display':  vocab.MaterialStatement,
	'inscription_display': vocab.InscriptionStatement,
	'provenance_display': vocab.ProvenanceStatement,
	'acquisition_source_display': vocab.AcquisitionStatement
}

facet_classes = {
	'place': model.Place,
	'person': model.Person,
	'occupation': vocab.OccupationType,
	'function': vocab.FunctionType,
	'form': vocab.FormType,
	'period': model.Period,
	'date': model.Period,
	'genre': model.Type,
	'organization': vocab.OrganizationGroup,
	'culture': vocab.CultureGroup,
	'family': vocab.FamilyGroup,
	'meeting': vocab.MeetingGroup,
	'title': model.LinguisticObject,
	'publication': model.LinguisticObject,
	'topic': model.Type
}


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
	elif ftypl in facet_classes:
		what = facet_classes[ftpl]()
	elif ftyp in facet_classes:
		what = facet_classes[ftyp]()
	else:
		if ftyp or ftypl:
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
		try:
			u = name_map[name]  # such a simple line...
		except:
			print(f"Trying: '{name}' in {name_map}")
			raise
		what.id = f"urn:uuid:{u}"

	rolel = facet.get('facet_role_label', '').lower()
	rolec = facet.get('facet_role_code', '').lower()
	roleu = facet.get('facet_role_URI', [])
	# XXX Not sure what to do with these roles? --> MDWG

	return what


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
			if ttype_label == "contents":
				# title of a part of the work
				# make a part, using main's class
				part = main.__class__()
				if main.__class__ == model.Set:
					main.member = part
				else:
					main.part = part
				title_target = part
			elif ttype_label in ["work related", 'related']:
				rel = model.LinguisticObject()
				# XXX -- what relationship to the main is this? main.related = rel
				title_target = rel
			elif ttype_label in title_classes:
				ttype = title_classes[ttype_label]
			else:
				if not ttype_label in ['work', 'foreign title']:
					print(f"-- saw title type: {ttype_label}")
				tt = model.Type(ident=type_map[f'title - {ttype_label}'], label=ttype_label)

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
			ityp = i.get('identifier_type', None)
			if ityp and ityp in identifier_classes:
				ident = identifier_classes[ityp](content=ival)
			else:
				ident = model.Identifier(content=ival)
				if ityp in identifier_types:
					ident.classified_as = identifier_types[ityp]
				else:
					print(f"-- Unmapped identifier type: {ityp}")
					ident.classified_as = model.Type(label=ityp)
			idisp = i.get('identifier_display', '')
			if idisp and idisp != ival:
				iname = vocab.DisplayName(content=idisp)
				ident.identified_by = iname
			main.identified_by = ident

	# basic_descriptor notes
	for (prop, cl) in bdnote_classes.items():
		if prop in bd:
			for t in bd[prop]:
				txt = construct_text(t, cl)
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
			if nt in note_type_class:				
				ntc = note_type_class[nt]
				if ntc:
					noteType = ntc 
			else:
				ntype = model.Type(label=nt)
				print(f"-- Unknown note type: {nt}")
		nl = n.get('note_label', None)
		if nl and nl.lower() != nt.lower():
			ndisp = vocab.DisplayName(content=nl)
		else:
			ndisp = None
		for d in n.get('note_display', []):
			note = construct_text(d, noteType)
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
	ead_self_uri = None
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
				if r.startswith('https://archives.yale.edu/repositories/'):
					ead_self_uri = r
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

		if orig_type == "usage" and orig_notes:
			for o in orig_notes:
				txt = construct_text(o, vocab.RightsStatement)
				if txt:
					main.referred_to_by = txt
		elif orig_type in ["usage", '', None] and orig_uri:
			for o in orig_uri:
				main.referred_to_by = vocab.RightsStatement(ident=o, label="Rights Statement")
		elif orig_type == 'access' and orig_notes:
			for o in orig_notes:
				if type(o) == str:
					txt = vocab.AccessStatement(content=o)
				else:
					txt = construct_text(o, vocab.AccessStatement)
				if txt:
					main.referred_to_by = txt

		elif orig_uri or orig_notes:
			print(f"{orig_type_label} / {orig_type} / {orig_type_uri} / {orig_uri} / {orig_notes}")
		# if only a type but no content, then nothing to do

	# digital_assets
	digass = record.get('digital_assets', [])
	for da in digass:
		da_uris = da.get('asset_URI', [])
		da_type = da.get('asset_type', None) # image, soundcloud
		da_flag = da.get('asset_flag', None) # primary image,

		if da_uris:
			if not da_type:
				for u in da_uris:
					if u[-4:] in ['.jpg', '.png', '.tif', '.gif']:
						da_type = "image"
						break
					elif u[-4:] in ['.pdf', '.doc']:
						da_type = 'document'
						break
					elif u.endswith('.html') or u.endswith('.htm'):
						da_type = "webpage"
						break
			elif da_type == "soundcloud":
				da_type = "digital object link"

			if da_type in ["image", "thumbnail"] or da_flag == "primary image":
				# image of this entity; intent to show inline
				dobj = vocab.DigitalImage()
				for u in da_uris:
					dobj.access_point = u
				imgvi = model.VisualItem()
				imgvi.digitally_shown_by = dobj
				main.representation = imgvi
			elif da_type == "digital object link": 
				# webpage with digital representation of this entity; intent to create a link
				# not necessarily visual (e.g. soundcloud), just another interaction
				for u in da_uris:
					dobj = vocab.WebPage(ident=u, label="Digital Representation")
					main.subject_of = dobj
			else:
				if da_type or da_flag:
					print(f"dig: {da_uris} / {da_type} / {da_flag}")
				for u in da_uris:
					dobj = model.DigitalObject(ident=u)
					# XXX ref to by isn't quite right, but is the weakest sauce we have
					main.referred_to_by = dobj
			
		captions = da.get('asset_caption_display', [])
		for c in captions:
			txt = construct_text(c, vocab.Description)
			if txt:
				dobj.referred_to_by = txt

		arsd = da.get('asset_rights_status_display', [])
		arn = da.get('asset_rights_notes', [])
		art = da.get('asset_rights_type', [])
		artl = da.get('asset_rights_type_label', [])
		# Process these somehow?

	# hierarchies
	hiers = record.get('hierarchies', [])
	for h in hiers:

		htype = h.get('hierarchy_type', "") # "EAD; Series" / EAD; File / common names / taxonomic names
		ancestor_names = h.get('ancestor_display_names', []) # ["coll of foo"] / Animalia / Insects
		ancestor_ids = h.get('ancestor_internal_identifiers', []) # [".../resources/417"]
		root_id = h.get('root_internal_identifier', '') # https://.../resources/417

		# ancestor_uris = h.get('ancestor_URIs', [])  # empty?
		# descends = h.get('descendant_count', 0)
		# sibs = h.get('sibling_count', 0)
		# max_depth = h.get('maximum_depth', 0)
		# htype_uri = h.get('hierarchy_type_URI', "") 

		if htype.startswith('EAD'):
			# ASpace EAD partitioning structure
			if ancestor_ids:
				# link this as a member_of its parent
				parent = ancestor_ids[-1]
				parentid = ead_uri_map[parent]
				main.member_of = model.Set(ident=parentid, label=ancestor_names[-1])
				ead_uri_map[ead_self_uri] = main.id
			else:
				ead_uri_map[root_id] = main.id
		elif htype in ['taxonomic names', 'common names']:
			# YPM taxonomic hierarchy
			pass
		else:
			print(f"Unknown hierarchy type: {htype} in {fn}")

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
		auri = a.get('agent_URI', [])

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
		if auri and auri[0]:
			ident = f"urn:uuid:{uri_map[auri[0]]}"
			agent = agent_class(ident=ident)
		else:
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

		if not agent.id and hasattr(agent, 'identified_by'):
			name = agent.identified_by[0].content
			# XXX Enhancements here :)
			agent.id = f"urn:uuid:{name_maps[agent_class][name]}"


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
				if not rolel:
					rolel = "creator"
				if agent_roles_production[rolel]:
					roleu = agent_roles_production[rolel].replace('aat:', 'http://vocab.getty.edu/aat/')
				else:
					roleu = f"urn:uuid:{type_map[rolel]}"

				part.classified_as = model.Type(ident=roleu, label=roleLabel)

			# Publication
			elif rolel in agent_roles_publication:
				act = vocab.Publishing()
				act.carried_out_by = agent
				if agent_roles_publication[rolel]:
					roleu = agent_roles_publication[rolel].replace('aat:', 'http://vocab.getty.edu/aat/')
				else:
					roleu = f"urn:uuid:{type_map[rolel]}"				
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
					if agent_roles_encounter[rolel]:
						roleu = agent_roles_encounter[rolel].replace('aat:', 'http://vocab.getty.edu/aat/')
					else:
						roleu = f"urn:uuid:{type_map[rolel]}"
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
				if agent_roles_sponsor[rolel]:
					roleu = agent_roles_sponsor[rolel].replace('aat:', 'http://vocab.getty.edu/aat/')
				else:
					roleu = f"urn:uuid:{type_map[rolel]}"
				part.classified_as = model.Type(ident=roleu, label=roleLabel)
				part.carried_out_by = agent
				act.influenced_by = part
		
			# Performance
			elif rolel in agent_roles_performance:
				act = vocab.Performance()
				main.used_for = act
				if agent_roles_performance[rolel]:
					roleu = agent_roles_performance[rolel].replace('aat:', 'http://vocab.getty.edu/aat/')
				else:
					roleu = f"urn:uuid:{type_map[rolel]}"
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

		puris = p.get('place_URI', [])
		if puris and puris[0]:
			ident = f"urn:uuid:{uri_map[puris[0]]}"
			place = model.Place(ident=ident)
		else:
			place = model.Place()

		pnames = p.get('place_display', [])
		if not pnames or (pnames and len(pnames) == 1 and not pnames[0]['value']):
			continue
		else:
			for pn in pnames:
				txt = construct_text(pn, model.Name)
				if txt:
					place.identified_by = txt

		for pu in puris:
			place.equivalent = model.Place(ident=pu)

		if not place.id and hasattr(place, 'identified_by'):
			name = place.identified_by[0].content
			# XXX Enhancements on place reconciliation here

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

			#
			# XXX --- this should have an id ---
			#

			if suri:
				subject.equivalent = model.Type(ident=suri)
			for d in shd:
				txt = construct_text(d, model.Name)
				if txt:
					subject.identified_by = txt
			#if shs:
			#	subject.identified_by = vocab.SortName(content=shs)
			if facets:
				for facet in facets:
					f = construct_facet(facet)
					if f:
						subject.c_part = f
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
report_every = 29999
start = time.time()
for unit in units[:]:
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
			# break

