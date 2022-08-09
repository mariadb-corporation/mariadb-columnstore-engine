#!/usr/bin/env python3

import json
import sys

branch1_name = sys.argv[1]
branch2_name = sys.argv[2]
data_branch = json.loads(sys.argv[3])
data_develop = json.loads(sys.argv[4])
time_spent = 120

time = '''Time spent: {time}'''.format(time = sys.argv[5])

comparison = '''Queries made:
for {name1}
  -- {reads1} reads ({reads1ps} per second)
  -- {writes1} writes ({writes1ps} per second)
  -- {other1} other ({other1ps} per second)
  -- {total1} in total ({total1ps} per second)
for {name2}
  -- {reads2} reads ({reads2ps} per second)
  -- {writes2} writes ({writes2ps} per second)
  -- {other2} other ({other2ps} per second)
  -- {total2} in total ({total2ps} per second)
'''.format(name1 = branch1_name,
           name2 = branch2_name,
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
