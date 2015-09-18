#!/usr/bin/python

import optparse
from uniprot_importer import UniprotImporter

def main():
	parser = optparse.OptionParser()
	parser.add_option('-d', '--datfile', dest='dat_file', action='store', help='uniprot mapping input file(.dat)')
	parser.add_option('-t', '--tabfile', dest='tab_file', action='store', help='uniprot mapping selected file (.tab)')
	parser.add_option('-s', '--server', dest='server', action='store', help='MongoDB server hostname', default='localhost')
	parser.add_option('-p', '--port', dest='port', action='store', type='int', help='MongoDB server port', default=27017)
	parser.add_option('-n', '--db', dest='db', action='store', help='MongoDB database name')

	options, args = parser.parse_args()

	ui = UniprotImporter(options.server, options.port, options.db)
	ui.import_data(options.dat_file, options.tab_file)

if __name__ == '__main__':
	main()