cat $1/q*log | grep -v "sec)" | grep -v "CacheI/O" | grep -v "201" | grep -v "calgetstats" | grep -v "+---" > diff1.txt
cat baseline/q*log | grep -v "sec)" | grep -v "CacheI/O" | grep -v "201" | grep -v "calgetstats" | grep -v "+---" > diff2.txt

diff diff1.txt diff2.txt > diff.txt
lines=`cat diff.txt | wc -l`
if [ $lines -eq 0 ]
then
        echo "Query Validation   :  Success"
        rm -rf diff1.txt
        rm -rf diff2.txt
        rm -rf diff.txt
else
        echo "Query Validation   :  Failed"
fi

