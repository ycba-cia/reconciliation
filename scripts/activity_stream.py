import time
start_time = time.time()
import pymysql
import json

typemap = {
	"activity": "Activity",
	"digital": "DigitalObject",
	"group": "Group",
	"object": "HumanMadeObject",
	"person": "Person",
	"place": "Place",
	"provenance": "Activity",
	"set":"Set",
	"text":"LinguisticObject",
	"visual":"VisualItem",
        "concept":"Concept"
}

# connect to activity db
f=open("t.properties","r")
lines=f.readlines()
pw_from_t=lines[1]
dbschema=lines[2]
#dbschema="prod" #for testing on workstation
f.close()
db_act = pymysql.connect(host = "spinup-db0017cd.cluster-c9ukc6s0rmbg.us-east-1.rds.amazonaws.com",
					 user = "admin",
					 password = pw_from_t.strip(),
					 database = dbschema.strip())
cursor_act = db_act.cursor()

sql = "select * from activity order by updated asc"
entities = []
bucket = "https://ycba-lux.s3.amazonaws.com/v3/"
try:
	cursor_act.execute(sql)
	results = cursor_act.fetchall()
	for row in results:
		uri = ""
		entities.append([bucket+row[2]+"/"+row[1][0:2]+"/"+row[1]+".json",row[2],row[3],row[5].strftime('%Y-%m-%dT%H:%M:%SZ')])
except Exception as e:
	print(f"pymysql fetch Exception: {e}")

#print(entities) # for testing

pagesize = 10000 #10000
page = 1
i = 0
collection =  {}
current = {}
ordered_items = []
host = "https://ycba-lux.s3.amazonaws.com/activity_stream"
for entity in entities:
	#breakpoint()
	if i % pagesize == 0 and i == 0:
		current["@context"] = "http://iiif.io/api/discovery/1/context.json"
		current["id"] = f"{host}/page{int(page)}.json"
		current["type"] = "OrderedCollectionPage"
		current["startIndex"] = i
		part_of = {}
		part_of["id"] = f"{host}/collection1.json"
		part_of["type"] = "OrderedCollection"
		current["partOf"] = part_of
		collection["@context"] = "http://iiif.io/api/discovery/1/context.json"
		collection["id"] = f"{host}/collection1.json"
		collection["type"] = "OrderedCollection"
		collection["totalItems"] = entities.__len__()
		first = {}
		first["id"] = f"{host}/page{int(page)}.json"
		first["type"] = "OrderedCollectionPage"
		collection["first"] = first
		last = {}
		lastpage = entities.__len__() / pagesize
		if (entities.__len__() % pagesize) > 0:
			lastpage += 1
		last["id"] = f"{host}/page{int(lastpage)}.json"
		last["type"] = "OrderedCollectionPage"
		collection["last"] = last
		fh = open("../data/ycba/activity_stream/collection1.json", "w")
		fh.write(json.dumps(collection,sort_keys=False,indent=2))
		fh.close()
	elif i % pagesize == 0 and i > 0:
		page += 1
		next1 = {}
		next1["id"] = f"{host}/page{int(page)}.json"
		next1["type"] = "OrderedCollectionPage"
		current["next"] = next1
		current["orderedItems"] = ordered_items
		fh = open(f"../data/ycba/activity_stream/page{int(page)-1}.json", "w")
		fh.write(json.dumps(current,sort_keys=False,indent=2))
		fh.close()
		current = {}
		ordered_items = []
		current["@context"] = "http://iiif.io/api/discovery/0/context.json"
		current["id"] = f"{host}/page{int(page)}.json"
		current["type"] = "OrderedCollectionPage"
		part_of = {}
		part_of["id"] = f"{host}/collection1.json"
		part_of["type"] = "OrderedCollection"
		current["startIndex"] = i
		current["partOf"] = part_of
		prev = {}
		prev["id"] = f"{host}/page{int(page)-1}.json"
		prev["type"] = "OrderedCollectionPage"
		current["prev"] = prev
	activity = {}
	activity["type"] = entity[2]
	object = {}
	object["id"] = entity[0]
	object["type"] = typemap[entity[1]]
	object["format"] =  "application/ld+json;profile =\"https://linked.art/ns/v1/linked-art.json\""
	activity["object"] = object
	activity["endTime"] = entity[3]
	ordered_items.append(activity)
	current["orderedItems"] = ordered_items
	i += 1
fh = open(f"../data/ycba/activity_stream/page{int(page)}.json", "w")
fh.write(json.dumps(current, sort_keys=False, indent=2))
fh.close()
db_act.close()
print("--- %s seconds ---" % (time.time() - start_time))
