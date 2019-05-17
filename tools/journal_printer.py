import io
import os
import argparse
import sys
import json
import struct

journal_filename = ""
journal_size = 0

def parse_args():
    global journal_filename
    parser = argparse.ArgumentParser(description="Print journal entries")
    parser.add_argument('journal_file', type=str, help="The journal file");
    args = parser.parse_args()
    journal_filename = args.journal_file

def print_header(f):
    # Argh.  Load up to \0 into a string, then json.loads() that.
    s = ""
    c = chr(f.read(1)[0])
    while c != '\0':
        s = s + c
        c = chr(f.read(1)[0])

    header = json.loads(s)
    print("Header:")
    for (k, v) in header.items():
        print("  {} : {}".format(k, int(v)))

def print_journal_entries(f):
    i = 1
    pos = f.tell()
    while pos < journal_size:
        b = f.read(16)
        (offset, length) = struct.unpack("QQ", b)
        print("{}:  offset = {}, length = {}".format(i, offset, length))
        pos = f.seek(length, io.SEEK_CUR)
        i = i + 1

def main():
    global journal_size
    parse_args()
    journal_size = os.stat(journal_filename).st_size

    with open(journal_filename, "rb") as f:
        print_header(f)
        print_journal_entries(f)

if sys.version_info < (3, 0):
    print("This requires Python 3.0+ b/c I'm lazy")
    sys.exit(1)

if __name__ == "__main__":
    main()

