#This program is named Load balancer as it is responsible for balancing jobs across the different servers and server config provided by the user


#The following program creates 3 helper files for storing internal states of the program: 
# servers.config                    -----> This file stores the original config provided by the user
# prev.state                        -----> This file stores the server that received previous/last task
# <Given Servers>.trans             -----> These files stores current backlog tasks for each server (Number of .trans files =  number of servers provided by user)


#IMPORTANT NOTE: If the program needs to be run with a different configuration of servers or different servers, 
# the above files will need to be deleted, otherwise the previous config might be used.


import re
import subprocess
import os
import sys
import time
from datetime import datetime
from os import path
import collections

servers = {}
pstate = ''
newtransfile = ''
transserver = ''


# Function that implements load balancing for server (reads the input and processes it)
def balancer():
    if len(sys.argv) == 1:
        print("No Arguments Passed.. Exiting...")
        print("Usage: python loadbal.py Server:Size")
        exit()
    if len(sys.argv) > 1:
        if (':' in sys.argv[1]):
            noofservers = len(sys.argv) - 1
            for server in range(noofservers):
                tempdata = sys.argv[server + 1]
                tempserver = tempdata.split(':')[0]
                tempsize = int(tempdata.split(':')[1])
                # create Dict for all servers and sizes
                servers[tempserver] = tempsize
        else:
            print("Usage: python loadbal.py Server:Size")
            print("Server & Size Not Mentioned.. Exiting...")
            exit()
        newfile = 'servers.config'
        if not path.exists(newfile):
            fw = open(newfile, "w+")
            for server in sorted(servers, key=servers.get, reverse=True):
                newrecord = server + ":" + str(servers[server]) + '\n'
                fw.write(newrecord)
            fw.close()
        createtransfiles()
        pstate = prevstate()
        transdata()

# Checks whether all server .trans files have values zero, so they can be reset to the original config values
def checkzero():
    cntallzero = 0
    totalrecs=0
    howmanyzeros = 0
    with open("servers.config") as fr:
        for rec in fr:
            totalrecs = totalrecs + 1
    with open("servers.config") as fr:
        for rec in fr:
            (vserver, vsize) = rec.split(':')
            transfile = vserver + '.trans'
            with open(transfile) as fo:
                for rec in fo:
                    if int(rec) == 0:
                        howmanyzeros = howmanyzeros + 1


    if totalrecs == howmanyzeros:
        with open("servers.config") as fr:
            for rec in fr:
                (vserver, vsize) = rec.split(':')
                transfile = vserver + '.trans'
                transrecord = str(vsize)
                fw = open(transfile, "w+")
                fw.write(transrecord)
                fw.close()

# Retrieves the previous server used from .prev file
def prevstate():
    newfile = 'prev.state'
    pstate = '#'
    if not path.exists(newfile):
        fw = open(newfile, "w+")
        newrecord = '#'
        fw.write(newrecord)
        fw.close()
    else:
        with open("prev.state") as fs:
            for state in fs:
                pstate = state
    return pstate

#Replaces the .prev file's value to new value
def changestate(nstate):
    newfile = 'prev.state'
    fw = open(newfile, "w+")
    newrecord = nstate
    fw.write(newrecord)
    fw.close()

# Creates .trans files for all servers when the program is run with a new config for the first time
def createtransfiles():
    configdict = {}
    displayed = 0
    vprevstate = ''
    vnewstate = ''
    with open("servers.config") as fr:
        for rec in fr:
            (vserver, vsize) = rec.split(':')
            configdict[vserver] = int(vsize.rstrip('\n'))
        Z = sorted(configdict.items(), key=lambda item: item[1], reverse=True)
        for transserver, vvsize in Z:
            newtransfile = transserver + '.trans'
            if not path.exists(newtransfile):
                transrecord = str(vvsize)
                fw = open(newtransfile, "w+")
                fw.write(transrecord)
                fw.close()

# Figures out the next available server with the resources, so the task can be sent to that server 
def getcurrentstate():
    templist=[]
    with open("servers.config") as fr:
        for rec in fr:
            (server, vsize) = rec.split(':')
            nfile = server + '.trans'
            with open(nfile) as fn:
                for rec in fn:
                    if (int(rec) > 0):
                        templist.append(server)
        vprevstate = prevstate()
        for i in range(len(templist)):
            myTuple = (templist[i], templist[(i + 1) % len(templist)])
            if vprevstate=='#':
                return myTuple[0]
            if myTuple[0]==vprevstate:
                return myTuple[1]
        if vprevstate != myTuple[0]:
                return myTuple[0]

# Prints out the name of the random server 
def displayserver(incomingfile, ntransserver):
    with open(incomingfile) as ft:
        for rec in ft:
            if (int(rec) > 0):
                print(ntransserver)
                NewCount = int(rec) - 1
                transrecord = str(NewCount)
                if NewCount >= 0:
                    fn = open(incomingfile, "w")
                    fn.write(transrecord)
                    fn.close()
                    changestate(ntransserver)

# Main logic for displaying the available server
def transdata():
    displayed = 0
    vprevstate = ''
    vcurrent = ''
    checkzero()
    vprevstate = prevstate()
    vcurrent = getcurrentstate()

    with open("servers.config") as fr:
            for rec in fr:
                (transserver, vsize) = rec.split(':')
                if (transserver == vcurrent):
                    newtransfile = transserver + '.trans'
                    displayserver(newtransfile, transserver)
                    break



if __name__ == "__main__":
    balancer()


'''

How to run the program: 
>> python3 loadbal.py A:3 B:2 C:5


Code to test the program for the correct distribution: 
>> for i in {1..80}; do   python3 loadbal.py A:2 B:3 C:4 D:2; done > result.txt
>> grep 'A' result.txt | wc -l
>> grep 'B' result.txt | wc -l
>> grep 'C' result.txt | wc -l
>> grep 'D' result.txt | wc -l

''' 