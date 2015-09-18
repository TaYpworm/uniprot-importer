from datetime import datetime
from pymongo import MongoClient

class UniprotImporter(object):
	def __init__(self, mongodb_host, mongodb_port, db_name):
		if mongodb_host is None:
			raise ArgumentError('MongoDB server hostname cannot be None.')
		if mongodb_port <= 0 or mongodb_port > 65535:
			raise ArgumentError('MongoDB server port must be between 0..65535.')
		if db_name is None:
			raise ArgumentError('Database name cannot be None.')

		self.collection_name = 'uniprot_kb'
		self.client = MongoClient(mongodb_host, mongodb_port)
		self.db = self.client[db_name]
		self.collection = None

		self.date_added = 'DateAdded'
		self.date_updated = 'DateUpdated'
		self.uniprot_key = 'UniProtKB-AC'
		self.doc_header = [
				'UniProtKB-AC',		# 0
				'UniProtKB-ID',		# 1
				'GeneID',			# 2
				'RefSeq',			# 3
				'GI',				# 4
				'PDB',				# 5
				'GO',				# 6
				'UniRef100',		# 7
				'UniRef90',			# 8
				'UniRef50',			# 9
				'UniParc',			# 10
				'PIR',				# 11
				'NCBI-taxon',		# 12
				'MIM',				# 13
				'UniGene',			# 14
				'PubMed',			# 15
				'EMBL',				# 16
				'EMBL-CDS',			# 17
				'Ensembl',			# 18
				'Ensembl_TRS',		# 19
				'Ensembl_PRO',		# 20
				'Additional-PubMed'	# 21
			]


	def import_data(self, mapping_file_name, mapping_selected_file_name):
		if mapping_file_name is None:
			raise ArgumentError('Mapping file name cannot be None.')
		if mapping_selected_file_name is None:
			raise ArgumentError('Mapping selected file name cannot be None.')

		self._replace_mapping_selected(mapping_selected_file_name)
		self._update_mapping(mapping_file_name)

	def _replace_mapping_selected(self, file_name):
		'''
		Imports idmapping_selected.tab files.
		'''
		with open(file_name) as f:
			self.db.drop_collection(self.collection_name)
			self.collection = self.db[self.collection_name]
			
			for line in f:
				parsed_line = [ x.split('; ') if ';' in x else x for x in line.strip('\n').split('\t')]
				if len(parsed_line) != len(self.doc_header):
					raise MappingSelectedDataError('Line length {0} does not equal header length {1}.', len(parsed_line), len(self.doc_header))

				new_doc = dict(zip(self.doc_header, parsed_line))
				self._insert_document(new_doc)

	def _update_mapping(self, file_name):
		pass
		'''
		Imports idmapping.dat files.
		'''

		'''
		with open(file_name) as f:
			mapping_len = 3

			for line in f:
				tokens = line.strip('\n').split('\t')
				if len(tokens) != mapping_len:
					raise MappingDataError('Line length {0} does not equal expected length {1}.', len(parsed_line), len(self.doc_header))

				if tokens[1] not in self.doc_header:
					new_doc = {
						self.uniprot_key: tokens[0],
						tokens[1]: tokens[2]
					}

					existing_doc = self._get_document(new_doc[self.uniprot_key])
					if existing_doc:
						self._update_document(self._merge_docs(existing_doc, new_doc))
					else:
						self._insert_document(new_doc)
		'''

	def _merge_docs(self, a, b):
		retval = a.copy()
		for k, v in b.iteritems():
			if k in retval:
				if retval[k] != v:
					retval[k] = [retval[k]]
					retval[k].append(v)

		return retval

	def _insert_document(self, d):
		now = datetime.now()
		d[self.date_added] = now
		d[self.date_updated] = now

		self.collection.insert_one(d).inserted_id

	def _update_document(self, d):
		d[self.date_updated] = datetime.now()

		self.collection.replace_one( { self.uniprot_key: d[self.uniprot_key] }, d)

	def _get_document(self, uniprotkb_ac):
		return self.collection.find_one( { self.uniprot_key: uniprotkb_ac } )

class DataFormatError(Exception):
	pass

class ArgumentError(Exception):
	pass

class MappingSelectedDataError(Exception):
	pass

class MappingDataError(Exception):
	pass
