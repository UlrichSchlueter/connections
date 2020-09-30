#!/usr/bin/env python

import random
import os
import json
import uliConfig


FIRSTNAMES = "random-name/first-names.txt"
MIDDLENAMES = "random-name/middle-names.txt"
LASTNAMES = "random-name/names.txt"


uc = uliConfig.UliConfig("config.sourceMe")

OUTFILE = uc.get("MIL_TESTNAMESFILE")
TESTNUMBERSFILES = uc.get("MIL_TESTNUMBERSFILES")
NONAMES = uc.getInt("MIL_NUMBER_OF_NAMES")
FRIENDSFILE = uc.get("MIL_FRIENDSFILE")
MIN_FRIENDS = uc.getInt("MIL_MIN_FRIENDS")
MAX_FRIENDS = uc.getInt("MIL_MAX_FRIENDS")


firstnameStore = []
middlenameStore = []
lastnameStore = []


def slurpFileIntoList(name, list):
    with open(name, "r") as f:
        line = f.readline()
        while line:
            list.append(line.strip())
            line = f.readline()


slurpFileIntoList(FIRSTNAMES, firstnameStore)
slurpFileIntoList(MIDDLENAMES, middlenameStore)
slurpFileIntoList(LASTNAMES, lastnameStore)
# print(firstnameStore, middlenameStore, lastnameStore)
print("Names:")
names = []
with open(OUTFILE, "w") as f:
    for _ in range(NONAMES):
        fRandom = random.randint(0, len(firstnameStore) - 1)
        firstName = firstnameStore[fRandom]

        mRandom = random.randint(0, len(middlenameStore) - 1)
        middleName = middlenameStore[mRandom]

        nRandom = random.randint(0, len(lastnameStore) - 1)
        lastName = lastnameStore[nRandom]

        nameString = firstName + " " + middleName + " " + lastName
        # print (nameString)
        f.write(nameString + os.linesep)
        names.append(nameString)


print("Friends:")
friends = {}
for name in names:
    noFriends = random.randint(MIN_FRIENDS, MAX_FRIENDS)
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

with open(FRIENDSFILE, "w") as fp:
    json.dump(friends, fp)

print("Random Pairs:" + str(NONAMES))
with open(TESTNUMBERSFILES, "w") as f:
    for _ in range(1000):
        one = random.randint(0, NONAMES - 1)
        two = one
        while two == one:
            two = random.randint(0, NONAMES - 1)
        f.write(str(one) + "," + str(two) + "\n")
    if one >= NONAMES:
        print("Erf", one, NONAMES)
    if two >= NONAMES:
        print("Erf", two, NONAMES)

