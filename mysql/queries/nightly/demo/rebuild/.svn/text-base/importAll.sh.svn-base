#!/bin/sh

importSsb100c() {
	rm -f importSsb100c.log
	DB=ssb100c
	for table in customer dateinfo part supplier; do
		echo "Importing $DB.$table."
		/usr/local/Calpont/bin/cpimport $DB $table /mnt/data/ssb/100G/$table.tbl >> importSsb100c.log 2>&1
	done

	for i in /mnt/data/ssb/100G/19*.tbl; do
        	echo "Importing $i at `date`."
	        /usr/local/Calpont/bin/cpimport $DB lineorder $i >> importSsb100c.log 2>&1
        	echo ""
	done
	echo "All done with ssb100 import."
}

importTpch1tcOthers() {
	DB=tpch1tc
	rm -f tpch1tc.others.log
	for table in region nation customer part supplier partsupp; do
        	echo "Import /mnt/data/tpch/1t/$table."
	        /usr/local/Calpont/bin/cpimport $DB $table /mnt/data/tpch/1t/$table.tbl >> tpch1tc.others.log 2>&1
	done

	for i in /mnt/data/tpch/1t/orders*tbl; do
        	echo "Importing $i into $DB.orders at `date`."
        	/usr/local/Calpont/bin/cpimport $DB orders $i >> tpch1tc.others.log 2>&1
	done
}

importTpch1tcLineitem() {
	rm -f tpch1tc.lineitem.log
	DB=tpch1tc
	for i in /mnt/data/tpch/1t/lineitem*tbl*; do
        	echo "Importing $i into $DB.lineitem at `date`."
        	/usr/local/Calpont/bin/cpimport $DB lineitem $i >> tpch1tc.lineitem.log
	done
}

importNightly100() {
	rm -f nightly100.log
	DB=nightly100
	echo "Importing nightly100 tables."
        /usr/local/Calpont/bin/cpimport $DB orders /mnt/data/tpch/100g/orders.tbl >> nightly100.log 2>&1
	echo "All done with nightly100 tables." 
}

importSsb100c &
importTpch1tcOthers &
importTpch1tcLineitem &
importNightly100 &

wait

echo "All done with imports."
