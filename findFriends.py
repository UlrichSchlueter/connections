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
    QueryOptions,
)
from couchbase.auth import PasswordAuthenticator

uc = uliConfig.UliConfig("config.sourceMe")
clusterName = uc.get("CBCLUSTER")
cbadminuser = uc.get("CBCLUSTERUSER")
cbadminpassword = uc.get("CBCLUSTERPASSWORD")
cbbucket = uc.get("CBBUCKET")


peopleCache = {}


class QueryHandler:
    def __init__(self, cluster, bucket):
        self.cluster = cluster
        self.bucket = bucket

    def getNthID(self, n):
        res = self.cluster.query(
            "select meta().id from " + self.bucket + " LIMIT 1 OFFSET " + str(n)
        )
        id = list(res)[0]["id"]
        return id

    def getFriendsByID(self, id):
        res = self.cluster.query(
            "select * from " + self.bucket + " WHERE meta().id = $id",
            QueryOptions(named_parameters={"id": id}),
        )
        l = list(res)[0][self.bucket]
        # l = [n[cbbucket] for n in res]
        return l

    def getPersonByID(self, id):
        p = None
        if id in peopleCache:
            p = Person(id, peopleCache[id])
        else:
            friends = self.getFriendsByID(id)
            p = Person(id, friends)
            peopleCache[id] = friends
        return p

    def getCountOfEntries(self):
        query_result = cluster.query("select COUNT(*) as size from " + self.bucket)
        size = int(list(query_result)[0]["size"])
        return size


class Walker:
    def __init__(self, queryhandler, startHere):
        self.queryhandler = queryhandler
        self.startHere = startHere
        self.visited = {startHere}
        self.ToVisit = []
        self.ToVisit.extend(self.queryhandler.getFriendsByID(startHere))
        self.walked = 0

    def walk(self):
        if len(self.ToVisit) > 0:
            nextPersonID = self.ToVisit.pop()
            # p = self.queryhandler.getPersonByID(nextPersonID)
            self.visited.add(nextPersonID)
            for friend in self.queryhandler.getFriendsByID(nextPersonID):
                if not friend in self.visited:
                    if not friend in self.ToVisit:
                        self.ToVisit.append(friend)
        self.walked += 1

    def __str__(self):
        return (
            "ME:"
            + self.startHere
            + "\nWalked:"
            + str(self.walked)
            + "\nVisited:"
            + str(len(self.visited))
            + ":"
            + ",".join([str(n) for n in self.visited])
            + " \nToVisit:"
            + str(len(self.ToVisit))
            + ":"
            + ",".join([str(n) for n in self.ToVisit])
            + "\n----------------------------:"
        )


class Person:
    def __init__(self, id, friends):
        self.id = id
        self.friends = friends

    def __str__(self):
        return "P:" + self.id + "\nfriends:" + ",".join([str(n) for n in self.friends])


cluster = Cluster(
    "couchbase://" + clusterName,
    ClusterOptions(PasswordAuthenticator(cbadminuser, cbadminpassword)),
)


qh = QueryHandler(cluster, cbbucket)
size = qh.getCountOfEntries()

nTimes = 1000

numbers = []
with open("testnumbers", "r") as f:
    while True:
        line = f.readline().strip()
        if line is None or line == "":
            break
        else:
            one, two = line.split(",")

            numbers.append((one, two))

nTimes = min(nTimes, len(numbers) - 1)


while nTimes > 0:
    one, two = numbers[nTimes]

    firstID = qh.getNthID(one)
    secondID = qh.getNthID(two)

    firstWalker = Walker(qh, firstID)
    secondWalker = Walker(qh, secondID)
    nTimes -= 1

    #
    while True:
        firstWalker.walk()
        secondWalker.walk()
        inter = firstWalker.visited.intersection(secondWalker.visited)
        if len(inter) > 0:
            print("HHH", inter, firstWalker.walked, secondWalker.walked, one, two)
            break

    # print(firstWalker)
    # print(secondWalker)
