import random
import json
import os
import uliConfig

from couchbase.cluster import (
    Cluster,
    PasswordAuthenticator,
    ClusterOptions,
    BucketManager,
    QueryIndexManager,
)
from couchbase.auth import PasswordAuthenticator

friends = {}
names = []
dupes = {}

minFriends = 0
maxFriends = 10
maxNamesToInsert = 100000

uc = uliConfig.UliConfig("config.sourceMe")


namesFile = uc.get("MIL_TESTNAMESFILE")
clusterName = uc.get("CBCLUSTER")
cbadminuser = uc.get("CBCLUSTERUSER")
cbadminpassword = uc.get("CBCLUSTERPASSWORD")
cbbucket = uc.get("CBBUCKET")
testnumbersfile = uc.get("MIL_TESTNUMBERSFILES")
charDistribFile = uc.get("MIL_CHARDISTRIBFILE")


def slurpFileIntoList(name, list, maxLines=0):
    with open(name, "r") as f:
        line = f.readline()
        while line:
            list.append(line.strip())
            if maxLines != 0 and len(list) > maxLines:
                break
            line = f.readline()


slurpFileIntoList(namesFile, names, maxNamesToInsert)

for name in names:
    noFriends = random.randint(minFriends, maxFriends)
    for _ in range(noFriends):
        if not name in friends:
            friends[name] = []
        friendsList = friends[name]
        for _ in range(len(friendsList), noFriends):
            x = random.randint(0, len(names) - 1)
            friend = names[x]
            if not friend in friends:
                friends[friend] = []
            friendsfreinds = friends[friend]
            if not name in friendsfreinds:
                friendsfreinds.append(name)
            if not friend in friendsList:
                friendsList.append(friend)


def buildBuckets(chars):
    buckets = list()
    buckets.append([])
    buckets.append([])
    buckets.append([])
    used = [0, 0, 0]

    for k in chars:
        minUsed = 0
        for i in range(1, 3):
            if used[minUsed] > used[i]:
                minUsed = i
        buckets[minUsed].append(k)
        v = chars[k]
        used[minUsed] += v
    return buckets


cluster = Cluster(
    "couchbase://" + clusterName,
    ClusterOptions(PasswordAuthenticator(cbadminuser, cbadminpassword)),
)

bucket = cluster.bucket(cbbucket)
bucket.flush()

query_result = cluster.query(
    "SELECT COUNT(*) as size FROM system:indexes where keyspace_id='"
    + cbbucket
    + "' and name= '#primary'"
)
size = int(list(query_result)[0]["size"])
if size == 0:
    quit = QueryIndexManager(cluster)
    quit.create_primary_index(cbbucket)

print("Distrib")
firstChars = {}
for name in friends:
    # print(name, friends[name])
    fc = name[0]
    if not fc in firstChars:
        firstChars[fc] = 0
    firstChars[fc] += 1

with open(charDistribFile, "w") as f:
    for k, v in sorted(firstChars.items()):
        f.write(str(k) + "," + str(v) + "\n")

buildBuckets(firstChars)

collection = bucket.default_collection()
print("Inject:" + str(len(friends)))
for name in friends:
    # print(name, friends[name])

    collection.upsert(name, friends[name])

print("Rand Pairs:" + str(len(friends)))
with open(testnumbersfile, "w") as f:
    for _ in range(1000):
        one = random.randint(0, len(friends) - 1)
        two = one
        while two == one:
            two = random.randint(0, len(friends) - 1)
        f.write(str(one) + "," + str(two) + "\n")


print("Done")

