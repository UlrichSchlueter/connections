#!/usr/bin/env python

import random
import json
import os
import uliConfig
from shardAdmin import Shard, ShardAdmin


friends = {}
names = []
dupes = {}

maxNamesToInsert = 100000

uc = uliConfig.UliConfig("config.sourceMe")


namesFile = uc.get("MIL_TESTNAMESFILE")

testnumbersfile = uc.get("MIL_TESTNUMBERSFILES")
charDistribFile = uc.get("MIL_CHARDISTRIBFILE")
friendsFile = uc.get("MIL_FRIENDSFILE")
buckets = uc.get("CBBUCKETS")
bucketlist = list(map(lambda x: x.strip(), buckets.split(",")))


def slurpFileIntoList(name, list, maxLines=0):
    with open(name, "r") as f:
        line = f.readline()
        while line:
            list.append(line.strip())
            if maxLines != 0 and len(list) > maxLines:
                break
            line = f.readline()


def buildBucketBalancedList(chars):

    buckets = []
    for _ in range(len(bucketlist)):
        buckets.append([])
    used = [0] * len(bucketlist)

    for k in chars:
        minUsed = 0
        for i in range(1, len(bucketlist)):
            if used[minUsed] > used[i]:
                minUsed = i
        buckets[minUsed].append(k)
        v = chars[k]
        used[minUsed] += v
    return buckets


slurpFileIntoList(namesFile, names, maxNamesToInsert)

print("Create Links")

with open(friendsFile, "r") as f:
    friends = json.load(f)


print("Distrib")
firstChars = {}
for name in friends:
    # print(name, friends[name])
    fc = name[0]
    if not fc in firstChars:
        firstChars[fc] = 0
    firstChars[fc] += 1


print("Char Distrib File")
with open(charDistribFile, "w") as f:
    for k, v in sorted(firstChars.items()):
        f.write(str(k) + "," + str(v) + "\n")

print("Recreate Buckets")
sAdmin = ShardAdmin(uc)
sAdmin.recreateBuckets()

print("Get Shards stuff")
bucketDistrib = buildBucketBalancedList(firstChars)
shardList = sAdmin.getShardNames()
for n in range(len(shardList)):
    sName = shardList[n]
    sAdmin.setPieceOfTheAction(sName, bucketDistrib[n])

print("Save SHARDS:")
sAdmin.saveToDB()

print("Inject:" + str(len(friends)))
numberOfFriends = len(friends)
for name in friends:

    bucket = sAdmin.getReadBucketForKey(name)
    collection = bucket.default_collection()
    collection.upsert(name, friends[name])

print("Done")

