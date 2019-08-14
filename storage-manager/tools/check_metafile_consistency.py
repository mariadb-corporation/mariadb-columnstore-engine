import io
import sys
import argparse
import json
from pathlib import Path
import os
import configparser
import re
import traceback


cloudPath = None
metaPath = None
journalPath = None
cachePath = None
bigObjectSet = set()

def get_envvar(match):
    return os.environ[match.group(1)]

def resolve_envvars(setting):
    result = str(setting)
    pattern = ("\$\{(.*)\}")
    result = re.sub(pattern, get_envvar, setting)
    return result

def parseArgs():
    global cloudPath
    global metaPath
    global journalPath
    global cachePath

    parser = argparse.ArgumentParser(description="Verifies that the fake-cloud and cache contain what the metadata files say")
    parser.add_argument("config_file", type=str, help="The storagemanager.cnf file")
    args = parser.parse_args()
    config = configparser.ConfigParser()
    try:
        config.read(args.config_file)
        cloudPath = Path(resolve_envvars(config["LocalStorage"]["path"]))
        metaPath = Path(resolve_envvars(config["ObjectStorage"]["metadata_path"]))
        cachePath = Path(resolve_envvars(config["Cache"]["path"]))
        journalPath = Path(resolve_envvars(config["ObjectStorage"]["journal_path"]))
        #print("{}\n{}\n{}\n{}".format(cloudPath, metaPath, cachePath, journalPath))       

    except Exception as e:
        parser.error("Failed to parse the config file.  Got '{}'".format(e))

    if not Path(cloudPath).is_dir() or not Path(metaPath).is_dir() or not Path(journalPath).is_dir() or not Path(cachePath).is_dir():
        parser.error("cloudpath, metapath, and journalpath need to be directories.")

def key_breakout(key):
    return key.split("_", 3)

def validateMetadata(metafile):
    try:
        metadata = json.load(open(metafile))

        for obj in metadata["objects"]:
            bigObjectSet.add(obj["key"])
            fields = key_breakout(obj["key"])
            cPath = cachePath / obj["key"]
            l_cloudPath = cloudPath / obj["key"]
            #if fields[2] != obj["length"]:
            #    print("object {}: in metadata length is {}, key says {}".format(obj["key"], obj["length"], fields[2]))
            if fields[1] != obj["offset"]:
                print("object {}: in metadata offset is {}, key says {}".format(obj["key"], obj["offset"], fields[1]))

            realSize = -1
            if cPath.exists():
                inCache = True
                realSize = cPath.stat().st_size
            else:
                inCache = False
            if l_cloudPath.exists():
                inCloud = True
                realSize = l_cloudPath.stat().st_size
            else:
                inCloud = False
            if not inCache and not inCloud:
                print("{} does not exist in cache or the cloud".format(obj["key"]))
                continue        

            # There are a couple cases where the length field and actual file size legitmately 
            # don't match.
            # 1) IOC::truncate() currently doesn't rename the object on truncate for
            # performance reasons.
            # 2) IOC::write() currently does the same on modifying an existing object.  
            # In that case, we can validate the length by parsing the journal file as well.
            #if int(obj["length"]) != realSize:
            #    print("{} has the wrong length in its key.  Actual length is {}.".format(obj["key"], realSize))
        
    except Exception as e:
        print("Failed to parse {}, got {}".format(metafile, e))
        traceback.print_exc() 


def walkMetaDir(basepath):
    for p in basepath.iterdir():
        if p.is_dir():
            #print("Recursing on {}".format(p))
            walkMetaDir(p)
        elif p.is_file():
            if p.suffix == ".meta": 
                validateMetadata(p)
            else:
                print("{} is not a metadata file".format(p))
        else:
            print("{} is not a metadata file".format(p))

# Verifies that everything in journalPath has a corresponding object in cloud/cache
def verifyValidJournalFiles():
    for p in journalPath.iterdir():
        l_cachePath = cachePath/(p.stem);
        l_cloudPath = cloudPath/(p.stem);
        if not l_cachePath.is_file() and not l_cloudPath.is_file():
            print("Journal file {} has no corresponding object in cache or cloud storage".format(p))

def verifyNoOrphans():
    for path in cloudPath.iterdir():
        if path.name not in bigObjectSet:
            print("{} is in cloud storage but not referenced by any metadata file".format(path.name))


    for path in cachePath.iterdir():
        if path.name not in bigObjectSet:
            print("{} is in the cache but not referenced by any metadata file".format(path.name))

def main():
    parseArgs()

    print("Verifying that all objects in metadata exist in cloud storage or the cache")
    walkMetaDir(metaPath)
    print("Verifying that all journal files have a corresponding object")
    verifyValidJournalFiles()
    print("Verifying that all objects in cloud & cache are referenced by metadata")
    verifyNoOrphans()
    print("Done")
    sys.exit(0)


if sys.version_info < (3, 5):
    print("Please use python version 3.5 or greater")
    sys.exit(1)

if __name__ == "__main__":
    main()

