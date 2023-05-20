This method runs ORDER BY using built-in sort. Not great in case of ORDER BY + LIMIT but not the end of the world. Remove this. New approach is to save all ORDER BY key columns in output RG.

Retain those ReturnedColumns that are not in WFS columns list but in orderByColsVec.

!!! Remove ORDER BY duplication in WFS. 