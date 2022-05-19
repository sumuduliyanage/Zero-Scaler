#Author: Sumudu Liyanage

import subprocess
import time
from pymongo import MongoClient

#mongo db 
client=MongoClient()
client = MongoClient("mongodb://localhost:27017/")
mydatabase = client['zeroscaler']
mycollection = mydatabase['data']

#command and executing
command = "hubble observe --protocol http"
sub = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)


#getting string from sub 
return_val = sub.stdout.read()
return_val = return_val.decode('utf-8')

lines = return_val.split('\n')
n = len(lines)
print(n)

#manipulating rows of data
i = 0
while i <n-1:
    line = lines[i]
    i = i+1
    chunks = line.split(' ')
    
    source_chunk = chunks[3].split('/')
    source_namespace = source_chunk[0]
    source_1 = source_chunk[1].split(':')
    source = source_1[0]

    dest_chunk = chunks[5].split('/')
    dest_namespace = dest_chunk[0]
    dest_1 = dest_chunk[1].split(':')
    dest = dest_1[0]

    record = {
    "Source": source, 
    "Source_Namespace":source_namespace,
    "Destination": dest,
    "Destination_Namespace": dest_namespace
    } 

    rec = mycollection.insert_one(record)

    #print(source, source_namespace, dest, dest_namespace)


time.sleep(60)

while True:
    command = "hubble observe --protocol http --since=1m"
    sub = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    return_val = sub.stdout.read()
    return_val = return_val.decode('utf-8')


    lines = return_val.split('\n')
    n = len(lines)
    print(n)

    #manipulating rows of data
    i = 0
    while i <n-1:
        line = lines[i]
        i = i+1
        chunks = line.split(' ')
        
        source_chunk = chunks[3].split('/')
        source_namespace = source_chunk[0]
        source_1 = source_chunk[1].split(':')
        source = source_1[0]

        dest_chunk = chunks[5].split('/')
        dest_namespace = dest_chunk[0]
        dest_1 = dest_chunk[1].split(':')
        dest = dest_1[0]

        print(source, source_namespace, dest, dest_namespace)

        record = {
        "Source": source, 
        "Source_Namespace":source_namespace,
        "Destination": dest,
        "Destination_Namespace": dest_namespace
        } 

        rec = mycollection.insert_one(record)
    
    time.sleep(60)
