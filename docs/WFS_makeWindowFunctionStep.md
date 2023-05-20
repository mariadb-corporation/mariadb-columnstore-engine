WFS adds extra columns into RG that must be removed after WFS. There is a block here that restores the original deliveredCols only there is no DISTINCT or GROUP BY b/c DISTINCT/GB does a similar filtering modifying jobInfo.distinctColVec.
[[WFS_initialize]]
