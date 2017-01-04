import pandas as pd
import pdb
import xmltodict

acl_owner= 'trace'
acl_group= 'hdfs'
with open('/home/abyres/Documents/trace_tm/falcon_sensitive_files/everything-about-trace.csv') as data:
	cred = pd.read_csv(data)
	number_of_datasource = len(cred['System'])
	for i in range(0,number_of_datasource):
		db = cred['System'][i]
		db_host = cred['IP'][i]
		db_port = cred['Port'][i]
		db_tsn = cred['TNS'][i]
		db_user = cred['Login'][i]
		db_pwd = cred['Password'][i]
		#pdb.set_trace()

		with open('datasource.xml','rw') as doc:
			ds = xmltodict.parse(doc.read())
	
			'''change the required word. nothing needed to be configured here.'''
			change_colo = '%sOracle'%(db.lower())
			change_desc = '%s Datasource'%(db.upper())
			change_ds_name = '%s'%(db.lower())
			change_dbName= 'dbName=%s'%(db.upper())
			change_endpoint='%s:%s/%s'%(db_host,db_port,db_tsn)
			change_user='%s'%(db_user)
			change_pwd='%s'%(db_pwd)
			change_acl_owner='%s'%(acl_owner)
			change_acl_group='%s'%(acl_group)

			'''modification to the scripts'''
			ds['datasource']['@colo'] = ds['datasource']['@colo'].encode('utf-8').replace('colo_sample',change_colo)
			ds['datasource']['@description'] = ds['datasource']['@description'].encode('utf-8').replace('desc_sample',change_desc)
			ds['datasource']['@name'] = ds['datasource']['@name'].encode('utf-8').replace('entity_name',change_ds_name)
			ds['datasource']['tags'] = ds['datasource']['tags'].encode('utf-8').replace('dbName=dbName_sample',change_dbName)

			'''changing username and password 2 times in XML. 2 occurences'''
			ds['datasource']['interfaces']['interface']['@endpoint'] = ds['datasource']['interfaces']['interface']['@endpoint'].encode('utf-8').replace('db_host:db_port/db_tsn',change_endpoint)
			ds['datasource']['interfaces']['interface']['credential']['userName'] = ds['datasource']['interfaces']['interface']['credential']['userName'].encode('utf-8').replace('user_name',change_user)
			ds['datasource']['interfaces']['interface']['credential']['passwordText'] = ds['datasource']['interfaces']['interface']['credential']['passwordText'].encode('utf-8').replace('user_pwd',change_pwd)
			ds['datasource']['interfaces']['credential']['userName'] = ds['datasource']['interfaces']['credential']['userName'].encode('utf-8').replace('user_name',change_user)
			ds['datasource']['interfaces']['credential']['passwordText'] = ds['datasource']['interfaces']['credential']['passwordText'].encode('utf-8').replace('user_pwd',change_pwd)

			'''changing ACL permission'''
			ds['datasource']['ACL']['@owner'] = ds['datasource']['ACL']['@owner'].encode('utf-8').replace('acl_owner',change_acl_owner)
			ds['datasource']['ACL']['@group'] = ds['datasource']['ACL']['@group'].encode('utf-8').replace('acl_group',change_acl_group)  
	
			'''convert back to XML file'''
			changes = xmltodict.unparse(ds,pretty=True)
			doc.close()

		with open('/home/abyres/Documents/trace_tm/falcon_sensitive_files/datasource-%s.xml'%(db.lower()),'w') as f:
			f.write(changes)
			f.close()

#pdb.set_trace()

