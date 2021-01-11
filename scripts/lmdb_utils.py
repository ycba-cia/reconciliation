
import lmdb
import json
import uuid


class LMCursor(object):
	_database = None
	_cursor = None

	def __init__(self, db, cur, prefix=''):
		self._database = db
		self._cursor = cur
		self._prefix = prefix

	def __iter__(self):
		# Back up one, so the first entry in the range isn't skipped
		self._cursor.prev()
		return self

	def __next__(self):
		n = self._cursor.next()
		if not n:
			raise StopIteration
		return self.item()

	def first(self):
		return self._cursor.first()

	def last(self):
		return self._cursor.last()

	def set_range(self, start):
		if type(start) == str:
			start = start.encode('utf-8')
		elif type(start) != bytes:
			raise ValueError("Start must be str or bytes")		
		self._cursor.set_range(start)

	def item(self):
		(k, value) = self._cursor.item()
		k = k.decode('utf-8')
		if self._prefix and not k.startswith(self._prefix):
			raise StopIteration
		if type(value) == bytes:
			# It should always be this
			value = value.decode('utf-8')			
		if value[0] in ["{", "["]:
			value = json.loads(value)
		return (k, value)


class LMDB(object):

	def __init__(self, dbname, open=False, map_size=0):
		self.name = dbname
		self.env = None
		self.cxn = None
		self.auto_truncate_keys = True
		if not map_size:
			map_size = int(1e9)  # 1 GB
		self.map_size = map_size
		if open:
			self.open()

	def open(self, write=True, reinit=True):
		# Can use:  with db.open() as txn:   ...
		# and when txn falls out of scope, lmdb context handler will autocommit
		if self.env is None:
			try:
				self.env = lmdb.Environment(self.name, map_size=self.map_size)
			except:
				raise ValueError(self.name)
		try:
			self.cxn = lmdb.Transaction(self.env, write=write)
		except:
			raise ValueError("Cannot open transaction, try resetting environment with open(reinit=True)")
		return self.cxn

	def set(self, key, value):
		if type(value) not in [str, bytes]:
			value = json.dumps(value)
		if type(key) == str:
			key = key.encode('utf-8')
		elif type(key) != bytes:
			raise ValueError("Key must be str or bytes")
		if len(key) > 512:
			if self.auto_truncate_keys:
				key = key[:511] + "+"
			else:
				raise ValueError(f"Key length is > 512: {key}")
		if type(value) == str:
			value = value.encode('utf-8')
		return self.cxn.put(key, value)

	def get(self, key):
		if type(key) == str:
			key = key.encode('utf-8')
		elif type(key) != bytes:
			raise ValueError('Key must be str or bytes')
		if len(key) > 512 and self.auto_truncate_keys:
			key = key[:511] + "+"
		else:
			raise ValueError(f"Key length is > 512: {key}")
		value = self.cxn.get(key)
		if not value:
			return value
		if type(value) == bytes:
			# It should always be this
			value = value.decode('utf-8')			
		if value[0] in ["{", "["]:
			value = json.loads(value)
		return value

	def delete(self, key):
		if type(key) == str:
			key = key.encode('utf-8')
		return self.cxn.delete(key)

	def cursor(self, start=None, prefix=None):
		if prefix is not None and start is None:
			start = prefix
		if start is not None:
			if type(start) == str:
				start = start.encode('utf-8')
			elif type(start) != bytes:
				raise ValueError("Start must be str or bytes")
		c = self.cxn.cursor()	
		if start:				
			c.set_range(start)
		else:
			c.first()
		return LMCursor(self, c, prefix)

	def commit(self):
		self.cxn.commit()
		self.open()

	def __getitem__(self, key):
		return self.get(key)
	def __setitem__(self, key, value):
		return self.set(key, value)
	def __delitem__(self, key):
		return self.delete(key)
	def __contains__(self, key):
		return self.get(key) is not None
	def __len__(self):
		return self.cxn.stat()['entries']

	def __iter__(self):
		c = self.cursor()
		c.first()
		return iter(c)