select char_2 c_1, varchar_9 c_2, char_8 c_3, varchar_255 c_4, varchar_8 c_5, varchar_4 c_6, varchar_9 c_7, right( char_2  , 1 ) c_9 from table100_char where varchar_8 is null order by c_1;
select varchar_9 c_1, varchar_4 c_2, varchar_1 c_4, char_2 c_5,upper( char_8 ) c_6 from table10_char where char_255 = 'x' order by c_1;
select char_9 c_1, char_2 c_2, char_4 c_3, varchar_1 c_4, varchar_4 c_5, replace( char_9  , 'b'  ,'aa' )  from table10_char where char_2 not in ( 'x' ,'w' , 'v' , 'y' , 'h' , 'y' , 'he\'s', 'as', 'that', 'how' ) order by c_1;
select varchar_1 c_1, varchar_255 c_2, varchar_4 c_3, ltrim( varchar_255 ) from table100_char where varchar_1 not in ( 'p' , 'c' , 'w' , 'b' , 'i' , 'v' ,'don\'t', 'with', 'ok', 'good' ) order by c_1;
select char_255 c_1, rtrim( char_255 ) c_2 from table10_char where varchar_2 between 'd' and 'w' order by c_1;
select varchar_1 c_1, char_255 c_3, ltrim( char_255 ) from table100_char where varchar_2 in ( 'a' , 'a' , 'n' , 'k' , 'f' , 'l' , 'do','but', 'but', 'did' ) order by c_1;
