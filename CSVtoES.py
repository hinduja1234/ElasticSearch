from elasticsearch import Elasticsearch as ES

from elasticsearch import helpers


import json

import csv

import sys

reload(sys)

sys.setdefaultencoding('utf8')

with open('/home/rohit/Downloads/esMapping.json') as data_file:
    data = json.load(data_file)

mapping = json.dumps(data, indent=4)

print(mapping)

# reader = pd.read_csv("/home/rohit/data/test_data.csv",dtype={"pd":int}).transpose().to_dict()

reader = csv.DictReader(open("/home/rohit/data/test_data.csv", 'r'),delimiter=",")

print reader

host = "localhost"
port = 9200
index = "test26"
doc_type = "demo"

es = ES(hosts=host, port=port, timeout=100)


es.indices.create(index=index, body=mapping)


actions = []
raw_dict={}

print (list(reader))



for row in reader:
	for k,v in row:
		if v=="":
			row[k]=None
		if v=="null":
			row[k]=None
		else:
			pass

	actions.append({"_op_type":"index","_index":index,"_type":doc_type,"_source":row})

	try:
		if len(actions)>=50000:
			print helpers.bulk(es, actions, index=index, doc_type=doc_type)
			actions=[]
	except UnicodeDecodeError as e :
		print e

print helpers.bulk(es, actions, index=index, doc_type= doc_type)
# es.indices.delete(index=index)
