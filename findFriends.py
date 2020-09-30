import random
import json
import os
import sys
import uliConfig
import time

from shardAdmin import Shard, ShardAdmin, Person


class Walker:
    def __init__(self, sAdmin, startHere):
        self.sAdmin = sAdmin
        self.startHere = startHere
        self.visited = {startHere}
        self.ToVisit = []
        self.ToVisit.extend(self.sAdmin.getFriendsByID(startHere))
        self.walked = 0

    def walk(self):
        if len(self.ToVisit) > 0:
            nextPersonID = self.ToVisit.pop()

            self.visited.add(nextPersonID)
            for friend in self.sAdmin.getFriendsByID(nextPersonID):
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


uc = uliConfig.UliConfig("config.sourceMe")

testReferenceData = uc.get("TEST_REFDATA")

testResults = {}


sAdmin = ShardAdmin(uc)
sAdmin.getShardsFromDB()

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

recordingMode = False
if os.path.exists(testReferenceData):
    with open(testReferenceData, "r") as f:
        while True:
            line = f.readline().strip()
            if not line == "":
                key, value = line.split(":")
                testResults[key.strip()] = value.strip()
            if line is None or line == "":
                break
else:
    print("Recording Mode")
    recordingMode = True


while nTimes > 0:
    one, two = numbers[nTimes]

    firstID = sAdmin.getNthID(one)
    secondID = sAdmin.getNthID(two)

    firstWalker = Walker(sAdmin, firstID)
    secondWalker = Walker(sAdmin, secondID)
    nTimes -= 1
    sys.stdout.write("\r" + str(nTimes))

    #
    while True:
        # irstWalker.walk()
        # secondWalker.walk()
        time.sleep(2)
        inter = firstWalker.visited.intersection(secondWalker.visited)

        if len(inter) > 0:
            key = (one + "-" + two).strip()

            value = (
                str(inter)
                + "-"
                + str(firstWalker.walked)
                + "-"
                + str(secondWalker.walked)
            ).strip()

            if recordingMode == False:
                if testResults[key] == value:
                    pass
                else:
                    print(key, " Booo!!!!!!!")
                    print(key, " : ", value, testResults[key])
            else:
                testResults[key] = value

            break


if recordingMode == True:
    with open(testReferenceData, "w") as f:
        for key in testResults:
            f.write(key + " : " + testResults[key] + "\n")

    # print(firstWalker)
    # print(secondWalker)
