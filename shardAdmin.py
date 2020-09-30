import json
import uliConfig
import threading
import time


from couchbase.cluster import (
    Cluster,
    PasswordAuthenticator,
    ClusterOptions,
    BucketManager,
    QueryIndexManager,
    QueryOptions,
)
from couchbase.auth import PasswordAuthenticator
from couchbase.options import LockMode
from couchbase.collection import RemoveOptions
from couchbase.durability import (
    ClientDurability,
    Durability,
    ReplicateTo,
    PersistTo,
    ServerDurability,
)


class ShardAdmin:
    def __init__(self, uliconfig):

        self.clusterName = uliconfig.get("CBCLUSTER")
        self.cbadminuser = uliconfig.get("CBCLUSTERUSER")
        self.cbadminpassword = uliconfig.get("CBCLUSTERPASSWORD")
        self.shardbucketname = uliconfig.get("CBSHARDSBUCKET")
        self.commandbucketname = "Command"
        # cbbucket = uc.get("CBBUCKET")
        self.cbshardsbucket = uliconfig.get("CBSHARDSBUCKET")
        self.cbtargetsbuckets = uliconfig.getAsList("CBBUCKETS")
        self.cbaddbuckets = uliconfig.getAsList("CBADDBUCKETS")

        self.shards = {}
        self.bucketForKeyRead = {}

        self.bucketForKeyWrite = {}
        self.cluster = Cluster(
            "couchbase://" + self.clusterName,
            ClusterOptions(
                PasswordAuthenticator(self.cbadminuser, self.cbadminpassword),
            ),
        )
        self.cluster2 = Cluster(
            "couchbase://" + self.clusterName,
            ClusterOptions(
                PasswordAuthenticator(self.cbadminuser, self.cbadminpassword),
            ),
        )
        self.shardbucket = self.cluster.bucket(self.shardbucketname)
        self.commandBucket = self.cluster.bucket(self.commandbucketname)
        self.peopleCache = {}

        x = threading.Thread(target=self.workerThread, args=(1,))
        x.start()

    def workerThread(self, name):
        command = "Command"
        while 1:
            print("Thread : ")
            collection = self.commandBucket.default_collection()
            res = collection.get(command)
            x = res.content
            if not x == "":
                print(x, x[command])
                if x[command] == "C->D":
                    x[command] = "*" + x[command]
                    collection.upsert(command, x)
                    self.moveBucket("C", "D")
            time.sleep(2)

    def getReadBucketForKey(self, id):
        return self.bucketForKeyRead[id[0]]

    def writeBucketForKey(self, id):
        pass

    def getShardNames(self):
        return list(self.shards.keys())

    def getBucketByName(self, name):
        ll = self.shards.values()
        shard = list(filter(lambda x: x.bucketname == name, ll))[0]
        return shard.bucket

    def recreateBuckets(self):

        self.shards = {}
        self.bucketForKeyRead = {}
        self.bucketForKeyWrite = {}
        for bucketName in self.cbtargetsbuckets:
            bucket = self.cluster.bucket(bucketName)
            bucket.flush()
            shard = Shard(bucket, bucketName)
            self.shards[bucketName] = shard

            query_result = self.cluster.query(
                "SELECT COUNT(*) as size FROM system:indexes where keyspace_id='"
                + bucketName
                + "' and name= '#primary'"
            )
            size = int(list(query_result)[0]["size"])
            if size == 0:
                quit = QueryIndexManager(self.cluster)
                quit.create_primary_index(bucketName)

        self.shardbucket.flush()

    def moveBucket(self, fromBucketName, toBucketName):

        # collectionFrom = self.getBucketByName(fromBucketName).default_collection()
        # collectionFrom = self.cluster2.bucket(fromBucketName).default_collection()
        # collectionTo = self.cluster2.bucket(toBucketName).default_collection()

        res = self.cluster2.query(
            "INSERT INTO "
            + toBucketName
            + " (key _k, value _v) SELECT META().id _k, _v FROM "
            + fromBucketName
            + " _v;"
        )
        if res.done == True:
            print("Gone")
            res = self.cluster2.query(
                "INSERT INTO "
                + toBucketName
                + " (key _k, value _v) SELECT META().id _k, _v FROM "
                + fromBucketName
                + " _v;"
            )

        """ counter = 0
        while True:
            res = self.cluster2.query(
                "select meta().* from " + fromBucketName + " LIMIT 10 "
            )
            if len(list(res)) == 0:
                print("Move Done {0} ".format(str(counter)))
                break
            firstRes = list(res)[0]
            id = firstRes["id"]
            # cas = firstRes["cas"]
            # cas = firstRes.cas
            print("FOUND:", id)

            c = collectionFrom.get(id)
            print("SAVE:", id, c.cas)
            collectionTo.upsert(id, c.content)
            print("REM:", id)
            q = "delete from " + fromBucketName + " where meta().id=" + '"' + id + '"'
            # res = self.cluster2.query(q)
            # if res.done == True:
            #    print("Gone")
            t = collectionFrom.remove(
                id, RemoveOptions(durability=ServerDurability(Durability.MAJORITY),),
            )
            print("REM:", id, t.cas)
            # time.sleep(1) """
        # counter += 1
        return id

    def setPieceOfTheAction(self, shardName, keyList):
        shard = self.shards[shardName]
        shard.pieceOfTheAction = keyList
        for key in keyList:
            self.bucketForKeyRead[key] = shard.bucket

    def getShardsFromDB(self):
        collection = self.shardbucket.default_collection()
        result = collection.get(self.shardbucketname)
        self.shards = {}
        self.bucketForKeyRead = {}
        for bucketName in result.content_as[str].split(","):

            result = collection.get(bucketName)
            pieceOfTheAction = result.content
            bucket = self.cluster.bucket(bucketName)
            shard = Shard(bucket, bucketName)
            self.shards[bucketName] = shard
            self.setPieceOfTheAction(bucketName, pieceOfTheAction)

        self.recalcSizes()

    def recalcSizes(self):
        for _, shard in self.shards.items():
            size = self.getCountOfEntries(shard.bucketname)
            shard.setSize(size)

    def saveToDB(self):
        collection = self.shardbucket.default_collection()
        for _, shard in self.shards.items():
            collection.upsert(
                shard.bucketname, shard.pieceOfTheAction,
            )

        collection.upsert(
            self.shardbucketname,
            ",".join(map(lambda x: self.shards[x].bucketname, self.shards)),
        )

    def getNthID(self, n):
        bucket = ""
        indexInBucket = -1
        counter = int(n)
        for _, shard in self.shards.items():
            sizeOfshard = shard.size
            if counter < sizeOfshard:
                bucket = shard.bucketname
                indexInBucket = counter
                break
            counter -= sizeOfshard

        res = self.cluster.query(
            "select meta().id from " + bucket + " LIMIT 1 OFFSET " + str(indexInBucket)
        )
        id = list(res)[0]["id"]
        return id

    def getFriendsByIDFromDB(self, id):
        bucket = self.getReadBucketForKey(id)
        collection = bucket.default_collection()
        x = None
        try:
            res = collection.get(id)
            x = res.content
        except Exception as e:
            print(e)

        return x

    def getFriendsByID(self, id):
        if id in self.peopleCache:
            friends = self.peopleCache[id]
        else:
            friends = self.getFriendsByIDFromDB(id)
            self.peopleCache[id] = friends
        return friends

    def getCountOfEntries(self, bucket):
        query_result = self.cluster.query("select COUNT(*) as size from " + bucket)
        size = int(list(query_result)[0]["size"])
        return size


class Shard:
    def __init__(self, bucket, bucketname):
        self.bucket = bucket
        self.bucketname = bucketname
        self.pieceOfTheAction = ""
        self.size = 0

    def setSize(self, size):
        self.size = size


class Person:
    def __init__(self, id, friends):
        self.id = id
        self.friends = friends

    def __str__(self):
        return "P:" + self.id + "\nfriends:" + ",".join([str(n) for n in self.friends])

