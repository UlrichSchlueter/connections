import pprint
import os, subprocess as sp, json


class UliConfig:
    def __init__(self, file):
        self.conf = {}
        self.file = file
        self.readConfig()

    def get(self, key):
        return self.conf[key]

    def getInt(self, key):
        return int(self.conf[key])

    def getAsList(self, key):
        return self.conf[key].split(",")

    def readConfig(self):
        self.conf = {}
        with open(self.file, "r") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if line.startswith("#"):
                    pass
                if "=" in line:
                    k, v = line.split("=")
                    if k != "":
                        self.conf[k] = v
                line = f.readline()

    def addConfigToEnv(self):
        for k, v in self.conf.items():
            os.environ[k] = v


#
