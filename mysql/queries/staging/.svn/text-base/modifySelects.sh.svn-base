#!/bin/bash

for fl in *.sql; do
	cat $fl | awk '{ gsub(/empsalary3/, "empsalary"); print }' > tmp.txt
	mv tmp.txt $fl
done
