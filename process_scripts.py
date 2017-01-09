import pandas as pd
import pdb
import xmltodict
import datetime
import os
import glob

hiveQL_owner = 'admin'
acl_owner= 'admin'
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
	#for i in range(0,ds_total):
	for i in range(0,2):
		table_no=0
		year = datetime.datetime.now().year
		month = datetime.datetime.now()
		month = datetime.datetime.strftime(month,"%m")
		day = datetime.datetime.now() + datetime.timedelta(days=1)
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
				dt = dt + datetime.timedelta(hours=4) #added 12 hours so that process entity have enough time to wait
				hour = dt.strftime("%H")
				minute = dt.strftime("%M")
				table_no = table_no+1
		#pdb.set_trace()
	
		
			with open('process.xml','rw') as doc:
				pr = xmltodict.parse(doc.read())
	
				'''change the required word. nothing needed to be configured here.'''
				change_process_name = '%stable%sprocess'%(db.lower(),str(table_no).zfill(2))
				change_tags_sourceHDFS = '%s.%s'%(db.lower(),db_tablename.upper())
				change_tags_targetTable= 'orc_%s_%s_Incremental'%(db.upper(),db_tablename.upper())
				change_tags_tableName='%s'%(db_tablename.upper())
				change_tags_dbName='%s'%(db.upper())	
				change_validity_start = '%s-%s-%sT%s:%sZ'%(year,month,day,hour,minute)
				change_input_feed = '%stable%s'%(db.lower(),str(table_no).zfill(2))
				change_input_name = '%stable%s'%(db.lower(),str(table_no).zfill(2))
				change_workflow_name = 'transformORC%s'%(change_input_feed)
				change_path = '/user/%s/hive-queries/orc-%s_%s.hql'%(hiveQL_owner,db.upper(),db_tablename.upper())
				change_acl_owner='%s'%(acl_owner)
				change_acl_group='%s'%(acl_group)

				'''modification to the scripts'''
				pr['process']['@name'] = pr['process']['@name'].encode('utf-8').replace('process_name',change_process_name)
				pr['process']['tags'] = pr['process']['tags'].encode('utf-8').replace('tags_sourceHDFS',change_tags_sourceHDFS)
				pr['process']['tags'] = pr['process']['tags'].encode('utf-8').replace('tags_targetTable',change_tags_targetTable)
				pr['process']['tags'] = pr['process']['tags'].encode('utf-8').replace('tags_tableName',change_tags_tableName)
				pr['process']['tags'] = pr['process']['tags'].encode('utf-8').replace('tags_dbName',change_tags_dbName)

				'''changing target cluster validity and locations'''
				pr['process']['clusters']['cluster']['validity']['@start'] = pr['process']['clusters']['cluster']['validity']['@start'].encode('utf-8').replace('validityTime',change_validity_start)
				pr['process']['inputs']['input']['@name'] = pr['process']['inputs']['input']['@name'].encode('utf-8').replace('input_name',change_input_name)
				pr['process']['inputs']['input']['@feed'] = pr['process']['inputs']['input']['@feed'].encode('utf-8').replace('input_feed',change_input_feed)
				
				'''change hive workflow path'''
				pr['process']['workflow']['@name'] = pr['process']['workflow']['@name'].encode('utf-8').replace('workflow_name',change_workflow_name)				
				pr['process']['workflow']['@path'] = pr['process']['workflow']['@path'].encode('utf-8').replace('workflow_path_name',change_path)
				
				'''changing ACL ownership'''
				pr['process']['ACL']['@owner'] = pr['process']['ACL']['@owner'].encode('utf-8').replace('acl_owner',change_acl_owner)
				pr['process']['ACL']['@group'] = pr['process']['ACL']['@group'].encode('utf-8').replace('acl_group',change_acl_group)  
			
				'''convert back to XML file'''
				changes = xmltodict.unparse(pr,pretty=True)
				doc.close()
			
			new_hive_query = '/home/abyres/Documents/trace_tm/falcon_sensitive_files/hive-queries/orc-%s_%s.hql'%(db.upper(),db_tablename.upper())
			if not os.path.exists(os.path.dirname(new_hive_query)):
				try:
					os.makedirs(os.path.dirname(new_hive_query))
				except OSError as exc: # Guard against race condition
					if exc.errno != errno.EEXIST:
						raise

			with open(new_hive_query,'w') as hv:
				hv.write('CREATE TABLE %s.orc_%s_%s_Incremental STORED AS ORC AS SELECT * FROM %s.%s;'%(db.lower(),db.upper(),db_tablename.upper(),db.lower(),db_tablename.lower()))
				hv.close()

			filename = '/home/abyres/Documents/trace_tm/falcon_sensitive_files/processes/%s/process-%stable%s.xml'%(db.lower(),db.lower(),str(table_no).zfill(2)) 
			if not os.path.exists(os.path.dirname(filename)):
				try:
					os.makedirs(os.path.dirname(filename))
				except OSError as exc: # Guard against race condition
					if exc.errno != errno.EEXIST:
						raise
			

			with open(filename,'w') as f:
				f.write(changes)
				f.close()

for listoffiles in glob.glob('/home/abyres/Documents/trace_tm/falcon_sensitive_files/processes/*/*00.xml'):
	os.remove(listoffiles)



#pdb.set_trace()
