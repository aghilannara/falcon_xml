import glob
import os
import shlex
import subprocess

hiveQL_owner = 'admin'

for files_path in glob.glob('/tmp/feeds/*/*'):
        files = files_path.split("/")[-1]
        db_name = files_path.split("/")[-2]
        cmd = "su - falcon -c 'falcon entity -submit -type feed -file /tmp/feeds/%s/%s'"%(db_name,files)
        splitted = shlex.split(cmd)
        print splitted
        subprocess.call(splitted)

for files_path in glob.glob('/tmp/hive-queries/*'):
        files = files_path.split("/")[-1]
        cmd = "su - hdfs -c 'hdfs dfs -put /tmp/hive-queries/%s /user/%s/hive-queries/'"%(files,hiveQL_owner.lower())
        cmd2 = "su - hdfs -c 'hdfs dfs -chown %s:hdfs /user/%s/hive-queries/%s'"%(hiveQL_owner.lower(),hiveQL_owner.lower(),files)

        splitted = shlex.split(cmd)
        splitted2 = shlex.split(cmd2)

        print splitted
        subprocess.call(splitted)

        print splitted2
        subprocess.call(splitted2)



for files_path in glob.glob('/tmp/processes/*/*'):
        files = files_path.split("/")[-1]
        db_name = files_path.split("/")[-2]
        cmd = "su - falcon -c 'falcon entity -submit -type process -file /tmp/processes/%s/%s'"%(db_name,files)
        splitted = shlex.split(cmd)
        print splitted
        subprocess.call(splitted)
