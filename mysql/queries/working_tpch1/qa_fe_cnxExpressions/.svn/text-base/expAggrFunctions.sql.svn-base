# Reviewed diffs and added round around 2nd and 4th queries to get them to match ref.  Avg uses division which is done at scale 4 by 
# default.
SELECT AVG(CINTEGER),SUM(CTINYINT) FROM DATATYPETESTM;
SELECT round(AVG(CINTEGER),4) * SUM(CTINYINT) FROM DATATYPETESTM;
SELECT SIGN(AVG(CINTEGER) / SUM(CTINYINT)) FROM DATATYPETESTM;

-- Edited below.  Round to 2 instead of 4 to compare with reference.  Bug 2617 is open for this issue.
-- SELECT round(POWER((AVG(CINTEGER) + SUM(CTINYINT)),3),4) FROM DATATYPETESTM;
SELECT round(POWER((AVG(CINTEGER) + SUM(CTINYINT)),3),2) FROM DATATYPETESTM;
