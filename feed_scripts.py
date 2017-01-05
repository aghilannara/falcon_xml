import pandas as pd
import pdb
import xmltodict
import datetime
import os

acl_owner= 'trace'
acl_group= 'hdfs'
data_owner = 'trace'
print("Expected a csv file with Data Source, Table Name and Daily Ingestion Time")
print("If error occur change the location of the files (feed.xml and data_ingestion_plan.csv) in python scripts")
print("ACL ownership need to be amended in the python scripts")

with open('/home/abyres/Documents/trace_tm/falcon_sensitive_files/data_ingestion_plan.csv') as data:
	tl = pd.read_csv(data)
	ds_list = tl['Data Source '].unique()
	total = len(tl['Data Source '])
	ds_total = len(ds_list)
	#for i in range(0,ds_total):
	#	ds_list[i] = ds_list[i].replace('Wholesale','')
	#	ds_list[i] = ds_list[i].replace('NOVA','')
	#	ds_list[i] = ds_list[i].replace('\xc2\xa0','')
	#	ds_list[i] = ds_list[i].replace('Network (ODIP)','ODIP')		
	#	ds_list[i] = ds_list[i].strip()
	for i in range(0,ds_total):
		table_no=0
		year = datetime.datetime.now().year
		month = datetime.datetime.now()
		month = datetime.datetime.strftime(month,"%m")
		day = datetime.datetime.now()
		day = datetime.datetime.strftime(day,"%d")
		#pdb.set_trace()
		for l in range(0,total):
			if tl['Data Source '][l] == ds_list[i]:
				db = '%s'%(ds_list[i].replace('Wholesale','').replace('NOVA','').replace('\xc2\xa0','').replace('Network (ODIP)','ODIP').strip()) 
				db_tablename = '%s'%(tl['Table Name'])[l]
				dt = datetime.datetime.strptime(tl['Daily Ingestion Time'][l],'%H:%M:%S %p')
				hour = dt.strftime("%H")
				minute = dt.strftime("%M")
				current_dt = '%s-%s-%s %s:%s'%(year,month,day,hour,minute)
				dt = datetime.datetime.strptime(current_dt,'%Y-%m-%d %H:%M')
				dt = dt - datetime.timedelta(hours=8)
				hour = dt.strftime("%H")
				minute = dt.strftime("%M")
				table_no = table_no+1
		#pdb.set_trace()
	
		
			with open('feed.xml','rw') as doc:
				fd = xmltodict.parse(doc.read())
	
				'''change the required word. nothing needed to be configured here.'''
				change_feed_name = '%stable%s'%(db.lower(),str(table_no).zfill(2))
				change_feed_desc = '%s Datasource'%(db.upper())
				change_tags_tableName = '%s'%(db_tablename.upper())
				change_tags_dbName= '%s'%(db.upper())
				change_retention = 'months(24)'
				change_validity_start = '%s-%s-%sT%s:%sZ'%(year,month,day,hour,minute)
				change_import_rdbms= '%s'%(db.lower())
				change_import_rdbms_tablename='%s'%(db_tablename.upper())
				change_hdfs_locations='/user/%s/source/%s/%s/Incremental/'%(data_owner,db.upper(),db_tablename.upper())
				change_acl_owner='%s'%(acl_owner)
				change_acl_group='%s'%(acl_group)

				'''modification to the scripts'''
				fd['feed']['@name'] = fd['feed']['@name'].encode('utf-8').replace('feed_name',change_feed_name)
				fd['feed']['@description'] = fd['feed']['@description'].encode('utf-8').replace('feed_desc',change_feed_desc)
				fd['feed']['tags'] = fd['feed']['tags'].encode('utf-8').replace('tags_tableName',change_tags_tableName)
				fd['feed']['tags'] = fd['feed']['tags'].encode('utf-8').replace('tags_dbName',change_tags_dbName)

				'''changing target cluster validity and locations'''
				fd['feed']['clusters']['cluster']['validity']['@start'] = fd['feed']['clusters']['cluster']['validity']['@start'].encode('utf-8').replace('validityTime',change_validity_start)
				fd['feed']['clusters']['cluster']['retention']['@limit'] = fd['feed']['clusters']['cluster']['retention']['@limit'].encode('utf-8').replace('retentionLimit',change_retention)
				fd['feed']['clusters']['cluster']['import']['source']['@name'] = fd['feed']['clusters']['cluster']['import']['source']['@name'].encode('utf-8').replace('rdbmsName',change_import_rdbms)
				fd['feed']['clusters']['cluster']['import']['source']['@tableName'] = fd['feed']['clusters']['cluster']['import']['source']['@tableName'].encode('utf-8').replace('rdbms_tableName',change_import_rdbms_tablename)

				'''changing target directory as per blueprint'''
				#pdb.set_trace()
				fd['feed']['clusters']['cluster']['locations']['location'][0]['@path'] = fd['feed']['clusters']['cluster']['locations']['location'][0]['@path'].encode('utf-8').replace('target_dir',change_hdfs_locations)
				fd['feed']['locations']['location'][0]['@path'] = fd['feed']['locations']['location'][0]['@path'].encode('utf-8').replace('target_dir',change_hdfs_locations)

				'''changing ACL ownership'''
				fd['feed']['ACL']['@owner'] = fd['feed']['ACL']['@owner'].encode('utf-8').replace('acl_owner',change_acl_owner)
				fd['feed']['ACL']['@group'] = fd['feed']['ACL']['@group'].encode('utf-8').replace('acl_group',change_acl_group)  
			
				'''convert back to XML file'''
				changes = xmltodict.unparse(fd,pretty=True)
				doc.close()
			

			filename = '/home/abyres/Documents/trace_tm/falcon_sensitive_files/feeds/%s/feed-%stable%s.xml'%(db.lower(),db.lower(),str(table_no).zfill(2)) 
			if not os.path.exists(os.path.dirname(filename)):
				try:
					os.makedirs(os.path.dirname(filename))
				except OSError as exc: # Guard against race condition
					if exc.errno != errno.EEXIST:
						raise
			

			with open(filename,'w') as f:
				f.write(changes)
				f.close()

#pdb.set_trace()
