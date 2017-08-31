UDAFMap
-------

UDAFMap holds a mapping from the function name to its implementation. The engine uses the map when a UDA(n)F is called by a SQL statement.

You must enter your function into the map. This means you must:

* #include your header file
* add an entry into UDAFMap::getMap().

The map is fully populated the first time it is called, i.e., the first time any UDA(n)F is called by a SQL statement.

The need for you to manually enter your function into this map may be alleviated by future enhancements.

