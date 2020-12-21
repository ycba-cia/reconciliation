import json
import os
from cromulent import model, vocab, reader
from identifiers import DB, map_uuid, map_uuid_uri, get_languages, rewrite_crom_ids
from data_utils import fetch, get_wikidata_entity
import shutil, pathlib
import datetime

model.factory.auto_assign_id = False

sources = ['../data/ycba/linked_art/person', '../data/ycba/linked_art/group']
dests = ['../output/person', '../output/group']

rdr = reader.Reader()

ulan_role_flag = "http://vocab.getty.edu/aat/300435108"
ulan_gender_flag = "http://vocab.getty.edu/aat/300055147"
ulan_nationality_flag = "http://vocab.getty.edu/aat/300379842"

ulan_biography_flag = "http://vocab.getty.edu/aat/300080102"
ulan_description_flag = "http://vocab.getty.edu/aat/300435416"

languages = get_languages()

wd_female = "wd:Q6581072"
wd_male = "wd:Q6581097"
uuid_female = map_uuid('aat', '300189557')
uuid_male = map_uuid('aat', '300189559')

# patch in get_gender, get_nationality on actor

all_nationalities = {}

natl_place_map = {
	"300107956": "7012149", # American
	"300111153": "1000062", # Austrian
	"300111156": "1000063", # Belgian
	"300111159": "7008591", # British
	"300111172" : "1000066", # Danish
	"300111175": "7016845", # Dutch
	"300111188": "1000070", # French
	"300111192": "7000084", # German
	"300111259": "1000078", # Irish
	"300111198": "1000080", # Italian
	"300107963": "7005560", # Mexican
	"300111204": "7006366", # Polish
	"300111215": "1000095", # Spanish
	"300111221": "7011731" # Swiss
}

place_natl_map = {}
for (k,v) in natl_place_map.items():
	place_natl_map[v] = k

place_natl_map["7002445"] = "300111159"  # england -> british
place_natl_map["7002444"] = "300111159" # scotland -> british

# And find the crom entities
natl_entities = {}
for w in vocab.instances.values():
	if isinstance(w, vocab.Nationality):
		natl_entities[w.id[-9:]] = w

def get_classn(self, meta):
	# Given the metatype, retrieve the classification on self
	result = []
	if hasattr(self, 'classified_as'):
		for c in self.classified_as:
			if hasattr(c, 'classified_as'):
				metas = [x.id for x in c.classified_as]
				if meta in metas:
					result.append(c)
	return result

def get_gender(self):
	return get_classn(self, map_uuid('aat', ulan_gender_flag[-9:]))
def get_nationality(self):
	return get_classn(self, map_uuid('aat', ulan_nationality_flag[-9:]))
def get_role(self):
	return get_classn(self, ulan_role_flag)

model.Actor.get_gender = get_gender
model.Actor.get_nationality = get_nationality

def make_datetime(txt):
	begin = datetime.datetime.strptime(txt, "%Y-%m-%dT%H:%M:%S")
	end = begin + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
	#biso = begin.isoformat()
	#eiso = end.isoformat()
	return (begin.isoformat()+"Z", end.isoformat()+"Z")

def fix_dt(txt):
	# Bad ULAN datetimes
	# check if 24:00:00 and subtract one
	if "24:00:00" in txt:
		return txt.replace('24:00:00', '23:59:59')
	else:
		return txt

for sx in [0,1]:
	src = sources[sx]
	output = dests[sx]
	chunks = os.listdir(src)
	chunks.sort()
	for c in chunks:
		cfn = os.path.join(src, c)
		files = os.listdir(cfn)
		files.sort()
		for f in files:
			fn = os.path.join(cfn, f)
			uuid = f"urn:uuid:{f[:-5]}"

			fnd = os.path.join(output, uuid[9:11])
			pathlib.Path(fnd).mkdir(parents=True, exist_ok=True)
			outfn = os.path.join(fnd, f"{uuid[9:]}.json")

			info = DB[uuid]
			if not info:
				print(f"No info ({info}) for {uuid} -- this shouldn't happen")
				info = {}
			ulanid = info.get('ulan', None)
			wdid = info.get('wikidata', None)
			viafid = info.get('viaf', None)

			if ulanid or wdid:
				fh = open(fn)
				data = json.load(fh)
				fh.close()			
				who = rdr.read(data)

				if ulanid:
					js = fetch('ulan', ulanid)
					if type(js) == list:
						# obsolete, continue
						print(f" ulan:{ulanid} Redirected ... fixme plz!")
					elif not js:
						# just broken
						pass
					else:
						# born, died, classified_as, carried_out, subject_of / referred_to_by
						# Names are full of Provenance junk, ignore

						if who.type != js['type']:
							print(f"Mismatch of type for {who.id}: {who.type} in data vs {js['type']} in ULAN)")
							continue

						roles = []
						if hasattr(who, 'classified_as'):
							my_classns = [x.id for x in who.classified_as]
						else:
							my_classns = []

						for c in js['classified_as']:
							meta = c['classified_as']
							metaids = [x['id'] for x in meta]
							if ulan_gender_flag in metaids:
								#print(f"Found gender: {c['id']}")
								gender = c['id'][-9:]
								uu = map_uuid('aat', gender, automap=False)
								if not uu:
									print(f"No map for gender {gender}")
								elif not uu in my_classns:
									who.classified_as = vocab.Gender(ident=uu)

							elif ulan_role_flag in metaids:
								#print(f"Found role: {c['id']}")
								role = c['id'][-9:]
								uu = map_uuid('aat', role, automap=False)
								if not uu:
									print(f"No map for role {role}")							
								else:
									roles.append(uu)
							elif ulan_nationality_flag in metaids:
								#print(f"Found nationality: {c['id']}")
								natl = c['id'][-9:]
								uu = map_uuid('aat', natl)
								if not uu:
									print(f"No map for natl {natl}")
								elif not uu in my_classns:
									who.classified_as = vocab.Nationality(ident=uu)
							else:
								# Doesn't seem to happen
								print(f"Unknown meta type: {metaids}")

						# We trust ULAN more than local data
						if js['type'] == 'Person':

							if 'born' in js:
								born = js['born']
								# Zero it out
								who.born = None
								who.born = model.Birth()
								when = born.get('timespan', None)
								if when:
									ts = model.TimeSpan()
									ts.begin_of_the_begin = when['begin_of_the_begin']
									ts.end_of_the_end = fix_dt(when['end_of_the_end'])
									who.born.timespan = ts
								where = born.get('took_place_at', None)
								if where:
									tgnid = where[0]
									if type(tgnid) == dict:
										tgnid = tgnid['id']
									tgnid = tgnid.replace('-place', '')
									tgnid = tgnid.replace('http://vocab.getty.edu/tgn/', '')
									uu = map_uuid('tgn', tgnid, automap=False)
									if not uu:
										print(f"No map for place {tgnid}")
									else:
										who.born.took_place_at = model.Place(ident=uu)
						
							if 'died' in js:
								died = js['died']
								who.died = None
								who.died = model.Death()
								when = died.get('timespan', None)
								if when:
									ts = model.TimeSpan()
									ts.begin_of_the_begin = when['begin_of_the_begin']
									ts.end_of_the_end = fix_dt(when['end_of_the_end'])
									who.died.timespan = ts							
								where = died.get('took_place_at', None)
								if where:
									tgnid = where[0]
									if type(tgnid) == dict:
										tgnid = tgnid['id']
									tgnid = tgnid.replace('-place', '')
									tgnid = tgnid.replace('http://vocab.getty.edu/tgn/', '')
									uu = map_uuid('tgn', tgnid, automap=False)					
									if not uu:
										print(f"No map for place {tgnid}")
									else:
										who.died.took_place_at = model.Place(ident=uu)
						else:
							# Group
							# Yes, ULAN has born and died for groups :/
							if 'born' in js:
								formed = js['born']
							elif 'formed_by' in js:
								formed = js['born']
							else:
								formed = None
							if 'died' in js:
								dissolved = js['died']
							elif 'dissolved_by' in js:
								dissolved = js['dissolved_by']
							else:
								dissolved = None							
							if formed:
								who.formed_by = None
								who.formed_by = model.Formation()
								when = formed.get('timespan', None)
								if when:
									ts = model.TimeSpan()
									ts.begin_of_the_begin = when['begin_of_the_begin']
									ts.end_of_the_end = fix_dt(when['end_of_the_end'])
									who.formed_by.timespan = ts
								where = formed.get('took_place_at', None)
								if where:
									tgnid = where[0]
									if type(tgnid) == dict:
										tgnid = tgnid['id']
									tgnid = tgnid.replace('-place', '')
									tgnid = tgnid.replace('http://vocab.getty.edu/tgn/', '')
									uu = map_uuid('tgn', tgnid, automap=False)					
									if not uu:
										print(f"No map for place {tgnid}")
									else:
										who.formed_by.took_place_at = model.Place(ident=uu)									
							if dissolved:
								who.dissolved_by = None
								who.dissolved_by = model.Dissolution()
								when = dissolved.get('timespan', None)
								if when:
									ts = model.TimeSpan()
									ts.begin_of_the_begin = when['begin_of_the_begin']
									ts.end_of_the_end = fix_dt(when['end_of_the_end'])
									who.dissolved_by.timespan = ts
								where = formed.get('took_place_at', None)
								if where:
									tgnid = where[0]
									if type(tgnid) == dict:
										tgnid = tgnid['id']
									tgnid = tgnid.replace('-place', '')
									tgnid = tgnid.replace('http://vocab.getty.edu/tgn/', '')
									uu = map_uuid('tgn', tgnid, automap=False)					
									if not uu:
										print(f"No map for place {tgnid}")
									else:
										who.dissolved_by.took_place_at = model.Place(ident=uu)
							
						if 'carried_out' in js:

							if hasattr(who, 'carried_out'):
								# XXX try to merge only active
								who.carried_out = []

							act = vocab.Active()
							if roles:
								for r in roles:
									act.classified_as = model.Type(ident=r)

							active = js['carried_out'][0]
							when = active.get('timespan', None)
							if when:
								ts = model.TimeSpan()
								ts.begin_of_the_begin = when['begin_of_the_begin']
								ts.end_of_the_end = fix_dt(when['end_of_the_end'])
								act.timespan = ts
							where = active.get('took_place_at', None)							
							if where:
								tgnid = where[0]
								if type(tgnid) == dict:
									tgnid = tgnid['id']
								tgnid = tgnid.replace('-place', '')
								tgnid = tgnid.replace('http://vocab.getty.edu/tgn/', '')
								uu = map_uuid('tgn', tgnid, automap=False)					
								if not uu:
									print(f"No map for place {tgnid}")
								else:
									act.took_place_at = model.Place(ident=uu)
							who.carried_out = act

						descs = []
						if 'subject_of' in js:
							for s in js['subject_of']:
								content = s['content']
								if content:
									lang = s.get('language', None)
									if lang:
										ll = lang[0]['_label']
										luu = languages.get(ll, None)
										if not luu:
											print(f"Unknown language: {ll}")
									stype = s.get('classified_as', [])
									descs.append((content, lang, stype))							

						if 'referred_to_by' in js:
							for r in js['referred_to_by']:
								content = r['content']
								if content:
									lang = r.get('language', None)
									if lang:
										ll = lang[0]['_label']
										luu = languages.get(ll, None)
										if not luu:
											print(f"Unknown language: {ll}")
									stype = r.get('classified_as', [])
									descs.append((content, lang, stype))

						for d in descs:
							dtyp = None
							for s in d[2]:
								if s['id'] == ulan_biography_flag:
									dtyp = vocab.BiographyStatement
									break
								elif s['id'] == ulan_description_flag:
									dtyp = vocab.Description
									break
							if not dtyp:
								print(f"Unknown statement type: {d[2]}")
								continue
							bio = dtyp(content=d[0])
							if d[1]:
								lang = d[1][0]
								ll = lang['_label']
								mylang = languages.get(ll, None)
								bio.language = model.Language(ident=mylang)
							who.referred_to_by = bio

				if wdid:
					# process wikidata to fill in gaps
					wdjs = fetch('wikidata', wdid)
					wdjs = get_wikidata_entity(wdjs, wdid)
					gender = wdjs.get('P21', None)
					if gender and not who.get_gender():
						if gender == wd_male:
							who.classified_as = vocab.instances['male']
						elif gender == wd_female:
							who.classified_as = vocab.instances['female']
						else:
							print(f"Unknown gender: {gender}")
					bdate = wdjs.get('P569', None)
					if bdate:
						if type(bdate) == list:
							bdate = bdate[0]							
						if bdate[-1] == 'Z':
							bdate = bdate[:-1]
						try:
							bstart = who.born.timespan.begin_of_the_begin
							bend = who.born.timespan.end_of_the_end
							if bstart[-1] == 'Z':
								bstart = bstart[:-1]
							if bend[-1] == 'Z':
								bend = bend[:-1]
						except:
							bstart = None
							bend = None

						if bstart and bend and bdate > bstart and bdate < bend:
							# More specific, replace
							new_bstart, new_bend = make_datetime(bdate)
						elif not bstart and not bend:
							new_bstart, new_bend = make_datetime(bdate)
							#print(f"{bdate} -- {bstart} / {bend}")
						else:
							new_bstart, new_bend = (None, None)

						if new_bstart:
							if not hasattr(who, 'born'):
								who.born = model.Birth()
								who.born.timespan = model.TimeSpan()
							who.born.timespan.begin_of_the_begin = new_bstart
							who.born.timespan.end_of_the_end = fix_dt(new_bend)

					bplace = wdjs.get('P19', None)
					try:
						curr_place = who.born.took_place_at[0]
					except:
						curr_place = None
					if bplace and not curr_place and not bplace[0] == '_' and not type(bplace) == list:
						# See if we know it
						bpuu = map_uuid('wikidata', bplace, automap=False)						
						if bpuu:
							who.born.took_place_at = model.Place(ident=bpuu)

					ddate = wdjs.get('P570', None)
					if ddate:
						if type(ddate) == list:
							ddate = ddate[0]							
						if ddate[-1] == 'Z':
							ddate = ddate[:-1]
						try:
							dstart = who.died.timespan.begin_of_the_begin
							dend = who.died.timespan.end_of_the_end
							if dstart[-1] == 'Z':
								dstart = dstart[:-1]
							if dend[-1] == 'Z':
								dend = dend[:-1]
						except:
							dstart = None
							dend = None

						if dstart and dend and ddate > dstart and ddate < dend:
							# More specific, replace
							new_dstart, new_dend = make_datetime(ddate)
						elif not dstart and not dend:
							new_dstart, new_dend = make_datetime(ddate)
							#print(f"{bdate} -- {bstart} / {bend}")
						else:
							new_dstart, new_dend = (None, None)

						if new_dstart:
							if not hasattr(who, 'died'):
								who.died = model.Death()
								who.died.timespan = model.TimeSpan()
							who.died.timespan.begin_of_the_begin = new_dstart
							who.died.timespan.end_of_the_end = fix_dt(new_dend)

					dplace = wdjs.get('P20', None)
					try:
						curr_place = who.born.took_place_at[0]
					except:
						curr_place = None
					if dplace and not curr_place and not dplace[0] == '_' and not type(dplace) == list:
						# See if we know it
						dpuu = map_uuid('wikidata', dplace, automap=False)						
						if dpuu:
							who.died.took_place_at = model.Place(ident=dpuu)


					nationality = wdjs.get('P27', None)
					# Can we even process WD nationality --> aat nationality?
					# print(gender, bdate, bplace, ddate, dplace, nationality)
					my_nat = who.get_nationality()
					if my_nat:
						my_nat_uu = [x.id for x in my_nat]
					else:
						my_nat_uu = []

					if nationality:
						if type(nationality) is not list:
							nationality = [nationality]
						for n in nationality:
							if n and n[0] != "_":
								# This is a Country in WD, so a Place in LA. 
								# Need to map back to Nationality, which is a concept
								uu = map_uuid('wikidata', n)
								info = DB[uu]
								if 'tgn' in info and info['tgn']:
									# Can align wd place -> tgn place
									tgn = info['tgn']
									if tgn in place_natl_map:
										aat_nat = place_natl_map[tgn]
										uu_nat = map_uuid('aat', aat_nat)
										if uu_nat in my_nat_uu:
											# good!
											#print("matched existing")
											pass
										elif my_nat:
											# Not good ... conflicting info
											# print(f"Had {my_nat_uu} and WD reports {uu_nat}")
											pass
										else:
											nat_ent = natl_entities.get(aat_nat, None)
											if nat_ent:
												print(f"No previous, assigned {nat_ent}")
												who.classified_as = nat_ent


									else:
										# print(f"Found tgn: {n} --> {tgn}")
										pass


				# reserialize over into final
				rewrite_crom_ids(who)
				model.factory.toFile(who, compact=False, filename=outfn)
			else:
				# Just copy into final
				# print("copying file")
				shutil.copyfile(fn, outfn)

