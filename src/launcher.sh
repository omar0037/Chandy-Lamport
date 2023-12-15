#!/bin/bash

# Change this to your netid
netid=oxs230000

# Root directory of your project
PROJDIR=$HOME/advOs

# Directory where the config file is located on your local system
CONFIGLOCAL=$HOME/launch/config.txt

n=0

declare -A matrix


input=""

cat $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" | sed -e "s/\r$//" |
(
    read i
    numOfNodes=$( echo $i | awk '{ print $1 }' )
    input+=$numOfNodes
    input+=' '
    minPerAct=$(echo $i | awk '{print $2}')
    input+=$minPerAct
    input+=' '
    maxPerAct=$(echo $i | awk '{print $3}')
    input+=$maxPerAct
    input+=' '
    minSendDel=$(echo $i | awk '{print $4}')
    input+=$minSendDel
    input+=' '
    snapDelay=$(echo $i | awk '{print $5}')
    input+=$snapDelay
    input+=' '
    maxNum=$(echo $i | awk '{print $6}')
    input+=$maxNum
    input+=' '
    input+=" -"
 while [[ $n -lt $numOfNodes ]]
 do
 	read line
	node=$(echo $line | awk '{print $1}')
	host=$(echo $line | awk '{print $2}')
	port=$(echo $line | awk '{print $3}')
	matrix["$node"]=$node
	matrix["$node"]+=' '
	matrix["$node"]+=$host
	matrix["$node"]+=' '
	matrix["$node"]+=$port
	matrix["$node"]+=' '
	n=$((n+1))
 done
 n=0
 while [[ $n -lt $numOfNodes ]]
 do
	read line
	matrix["$n"]+=$line
	input+=${matrix[$n]}
	input+=" -"
	n=$((n+1))
 done
 n=0
 netid=oxs230000
  while [[ $n -lt $numOfNodes ]]
   do
           host=$(echo ${matrix["$n"]} | awk '{print $2}')
	   a=${matrix[$n]}
	   #echo "------------------------------------------"
	   #echo $input
	   #ssh ${netid}@${host}.utdallas.edu "cd advOs && java Node $input" &
    	   gnome-terminal -e  "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host; cd advOs; java NodeV2 $input; exec bash" &
	   n=$((n+1))
   done
 )
