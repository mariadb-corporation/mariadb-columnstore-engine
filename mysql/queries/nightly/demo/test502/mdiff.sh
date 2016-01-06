cat $1/m*log | grep -v sec > mdiff1.txt
cat baseline/m*log | grep -v sec > mdiff2.txt

diff mdiff1.txt mdiff2.txt > mdiff.txt
lines=`cat mdiff.txt | wc -l`
if [ $lines -eq 0 ]
then
        echo "DML/DDL Validation :  Success"
        rm -f mdiff1.txt
        rm -f mdiff2.txt
        rm -f mdiff.txt
else
        echo "DML/DDL Validation :  Results did not match!  Check mdiff.txt for differences."
fi

