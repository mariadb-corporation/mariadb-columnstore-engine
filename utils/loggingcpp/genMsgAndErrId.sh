#!/bin/sh

perl ./genMsgId.pl > messageids-temp.h
diff -abBq messageids-temp.h messageids.h >/dev/null 2>&1; if [ $? -ne 0 ]; then mv -f messageids-temp.h messageids.h; else touch -a messageids.h; fi;
rm -f messageids-temp.h
perl ./genErrId.pl > errorids-temp.h
diff -abBq errorids-temp.h errorids.h >/dev/null 2>&1; if [ $? -ne 0 ]; then mv -f errorids-temp.h errorids.h; else touch -a errorids.h; fi;
rm -f errorids-temp.h
