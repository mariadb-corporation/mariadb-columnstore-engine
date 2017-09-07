.. _udafmap:

UDAFMap
=======

The UDAFMap is where we tell the system about our function. For Columnstore 1.1, you must manually place your function into this map.

* open mcsv1_udaf.cpp
* add your header to the #include list
* add a new line to the UDAFMap::getMap() function

::

 #include "allnull.h"
 #include "ssq.h"
 #include "median.h"
 #include "avg_mode.h"
 UDAF_MAP& UDAFMap::getMap()
 {
    if (fm.size() > 0)
    {
        return fm;
	}
    // first: function name
    // second: Function pointer
    // please use lower case for the function name. Because the names might be 
    // case-insensitive in MySQL depending on the setting. In such case, 
    // the function names passed to the interface is always in lower case.
    fm["allnull"] = new allnull();
    fm["ssq"] = new ssq();
    fm["median"] = new median();
    fm["avg_mode"] = new avg_mode();
	
    return fm;
 }

