select cidx, TIME_FORMAT('2009-10-04 22:23:00','%W %M %Y %w %l') from datatypetestm;
select cidx, TIME_FORMAT('2009-10-04 22:23:00','%D %y %a %d %m %b %j') from datatypetestm;
select cidx, TIME_FORMAT('22:23:00','%f %H %h %I %i %k %l %p %r %S %s %T') from datatypetestm;
select cidx, TIME_FORMAT('01:02:00','%f %H %h %I %i %k %l %p %r %S %s %T') from datatypetestm;

