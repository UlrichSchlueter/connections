import json
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


class ShardAdmin:
    def __init__(self, uliconfig):

        self.clusterName = uliconfig.get("CBCLUSTER")
        self.cbadminuser = uliconfig.get("CBCLUSTERUSER")
        self.cbadminpassword = uliconfig.get("CBCLUSTERPASSWORD")
        self.shardbucketname = uliconfig.get("CBSHARDSBUCKET")

        # cbbucket = uc.get("CBBUCKET")
        self.cbshardsbucket = uliconfig.get("CBSHARDSBUCKET")
        self.cbtargetsbuckets = uliconfig.getAsList("CBBUCKETS")

        self.shards = {}
        self.bucketForKey = {}
        self.cluster = Cluster(
            "couchbase://" + self.clusterName,
            ClusterOptions(
                PasswordAuthenticator(self.cbadminuser, self.cbadminpassword)
            ),
        )
        self.shardbucket = self.cluster.bucket(self.shardbucketname)
        self.peopleCache = {}

    def getShardNames(self):
        return list(self.shards.keys())

    def recreateBuckets(self):

        self.shards = {}
        self.bucketForKey = {}
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

    def setPieceOfTheAction(self, shardName, keyList):
        shard = self.shards[shardName]
        shard.pieceOfTheAction = keyList
        for key in keyList:
            self.bucketForKey[key] = shard.bucket

    def getShardsFromDB(self):
        collection = self.shardbucket.default_collection()
        result = collection.get(self.shardbucketname)
        self.shards = {}
        self.bucketForKey = {}
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
        bucket = self.bucketForKey[id[0]]
        collection = bucket.default_collection()
        res = collection.get(id)
        x = res.content
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

