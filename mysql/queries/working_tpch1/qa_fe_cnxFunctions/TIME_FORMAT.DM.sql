select cidx, CDATETIME, TIME_FORMAT(CDATETIME,'%W %M %Y') from datatypetestm;
select cidx, CDATETIME, TIME_FORMAT(CDATETIME,'%H:%i:%s') from datatypetestm;
select cidx, CDATETIME, TIME_FORMAT(CDATETIME,'%f %H %h %I %i %k %l %p %r %S %s %T') from datatypetestm;
select cidx, CDATETIME from datatypetestm where TIME_FORMAT(CDATETIME,'%W %M %Y') <> CDATETIME;
select cidx, CDATETIME from datatypetestm where TIME_FORMAT(CDATETIME,'%H:%i:%s') <> CDATETIME;
select cidx, CDATETIME from datatypetestm where TIME_FORMAT(CDATETIME,'%f %H %h %I %i %k %l %p %r %S %s %T') <> CDATETIME;
