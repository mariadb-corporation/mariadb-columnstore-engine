# WWW.  Edited.  Changed "5" to "4" to avoid known empty string as null comparison diffs between CNX and ref.
select n_name, SUBSTR(n_name from 4) from nation;
select n_name, SUBSTRING(n_name from 4) from nation;
select n_name, SUBSTRING(n_name from 4 for 3) from nation;
select n_name, substr(n_name, 4, 3) from nation;
