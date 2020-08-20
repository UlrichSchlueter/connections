import random
import os
import uliConfig


FIRSTNAMES = "random-name/first-names.txt"
MIDDLENAMES = "random-name/middle-names.txt"
LASTNAMES = "random-name/names.txt"


uc = uliConfig.UliConfig("config.sourceMe")

OUTFILE = namesFile = uc.get("MIL_TESTNAMESFILE")
NONAMES = int(uc.get("MIL_NUMBER_OF_NAMES"))

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
print(firstnameStore, middlenameStore, lastnameStore)


with open(OUTFILE, "w") as f:
    for _ in range(NONAMES):
        fRandom = random.randint(0, len(firstnameStore) - 1)
        firstName = firstnameStore[fRandom]

        mRandom = random.randint(0, len(middlenameStore) - 1)
        middleName = middlenameStore[mRandom]

        nRandom = random.randint(0, len(lastnameStore) - 1)
        lastName = lastnameStore[nRandom]

        nameString = firstName + " " + middleName + " " + lastName + os.linesep
        # print (nameString)
        f.write(nameString)

