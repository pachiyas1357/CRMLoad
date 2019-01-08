import json
import os

print (" Extract Data Started")

print("--")
print(os.path.dirname(__file__))

#file='{"n"me":"selva","age":30}'
file=open("/home/hdfs/CRMDataLoad/Config/etl_config.json","r")
print(file.read())
f=json.load(file)

#file_new=json.loads(file)
#f=file_new['hdfsurl']

#json_conf = json.loads(open('/home/hdfs/CRMDataLoad/Config/etl_config.json').read())
#url = json_conf["hdfsurl"]
#print(url)