#!/bin/bash

masterIPV4Address=$1
workerIPV4Address=$2
search_string="MasterServer.Endpoints"
replacement_string="MasterServer.Endpoints = tcp -h $masterIPV4Address -p 9099"

#==============================MASTERSERVER==============================

cp MasterServer/build/libs/MasterServer.jar deploy/MasterServer/MasterServer.jar
cp MasterServer/build/resources/main/master.cfg deploy/MasterServer/master.cfg
cd deploy/MasterServer

#Taken from ChatGPT. Prompt: bash script find line containing string and replace whole line   
file_path="./master.cfg"

# Use sed to replace the line
sed -i "/$search_string/c\\$replacement_string" "$file_path"
#------------------------------------------------------------------

jar uf MasterServer.jar master.cfg
jar ufm  MasterServer.jar META-INF/MANIFEST.MF  

cd ../..

#==============================WORKER SERVER==============================

cp WorkerServer/build/libs/WorkerServer.jar deploy/WorkerServer/WorkerServer.jar
cp WorkerServer/build/resources/main/worker.cfg deploy/WorkerServer/worker.cfg
cd deploy/WorkerServer

search_string="MasterServer.Proxy"
replacement_string="MasterServer.Proxy = Master : tcp -h $masterIPV4Address -p 9099"
search_string1="WorkerServer.Endpoints"
replacement_string1="WorkerServer.Endpoints = tcp -h $workerIPV4Address"

#Taken from ChatGPT. Prompt: bash script find line containing string and replace whole line   
file_path="./worker.cfg"

# Use sed to replace the line
sed -i "/$search_string/c\\$replacement_string" "$file_path"
sed -i "/$search_string1/c\\$replacement_string1" "$file_path"
#------------------------------------------------------------------

jar uf WorkerServer.jar worker.cfg
jar ufm  WorkerServer.jar META-INF/MANIFEST.MF  

cd ../..
