#!/usr/bin/env python3

import json
import sys

branch_name = sys.argv[1]
data_branch = []
data_develop = []
time_spent = 30

with open(sys.argv[2],'r') as jf1:
    data_branch = json.load(jf1)

with open(sys.argv[3],'r') as jf2:
    data_develop = json.load(jf2)

time = '''Time spent: {time}'''.format(time = time_spent)

comparison = '''Queries made:
for {name}
  -- {reads1} reads ({reads1ps} per second)
  -- {writes1} writes ({writes1ps} per second)
  -- {other1} other ({other1ps} per second)
  -- {total1} in total ({total1ps} per second)
for develop
  -- {reads2} reads ({reads2ps} per second)
  -- {writes2} writes ({writes2ps} per second)
  -- {other2} other ({other2ps} per second)
  -- {total2} in total ({total2ps} per second)
'''.format(name = branch_name,
           reads1 = data_branch[0]["queries"]["reads"],
           writes1 = data_branch[0]["queries"]["writes"],
           other1 = data_branch[0]["queries"]["other"],
           total1 = data_branch[0]["queries"]["total"],
           reads2 = data_develop[0]["queries"]["reads"],
           writes2 = data_develop[0]["queries"]["writes"],
           other2 = data_develop[0]["queries"]["other"],
           total2 = data_develop[0]["queries"]["total"],
           reads1ps = data_branch[0]["qps"]["reads"],
           writes1ps = data_branch[0]["qps"]["writes"],
           other1ps = data_branch[0]["qps"]["other"],
           total1ps = data_branch[0]["qps"]["total"],
           reads2ps = data_develop[0]["qps"]["reads"],
           writes2ps = data_develop[0]["qps"]["writes"],
           other2ps = data_develop[0]["qps"]["other"],
           total2ps = data_develop[0]["qps"]["total"],)

relation = ""
if data_branch[0]["qps"]["total"] >= data_develop[0]["qps"]["total"]:
    relation = '''The speed increase (total / total) is {inc}
               '''.format(inc = data_branch[0]["qps"]["total"]/data_develop[0]["qps"]["total"])
else:
    relation = '''The speed decrease (total / total) is {inc}
               '''.format(inc = data_develop[0]["qps"]["total"]/data_branch[0]["qps"]["total"])

print(time)
print(comparison)
print(relation)
