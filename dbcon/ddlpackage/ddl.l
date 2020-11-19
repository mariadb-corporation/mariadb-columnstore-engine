/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/* $Id: ddl.l 9341 2013-03-27 14:10:35Z chao $ */
%{
#include <string.h>
#include <iostream>
#include <vector>
#include <stdio.h>
#include "sqlparser.h"
#include "ddlpkg.h"

#ifdef _MSC_VER
#include "ddl-gram-win.h"
#else
#include "ddl-gram.h"
#endif

using namespace ddlpackage;
typedef enum { NOOP, STRIP_QUOTES } copy_action_t;
#if YYDEBUG == 0
int ddldebug = 0;
#endif
int lineno = 1;
void ddlerror(struct pass_to_bison* x, char const *s);

static char* scanner_copy(char *str, yyscan_t yyscanner, copy_action_t action = NOOP );

%}

%option reentrant
%option bison-bridge
%option noyywrap
%option nounput
/* %option header-file="ddl-scan.h" */

%x check1
%x check2
%x inquote
%x endquote
%x c_comment

space [ \t\n\r\f]
horiz_space [ \t\f]
newline [\n\r]
non_newline [^\n\r]

quote '
double_quote \"
grave_accent `

comment ("--"{non_newline}*)
extended_ident_cont [ A-Za-z\200-\377_0-9\$#,()\[\].;\:\+\-\*\/\%\^\<\>\=!&|@\\]
self			[,()\[\].;\:\+\-\*\/\%\^\<\>\=]
whitespace ({space}+|{comment})

digit [0-9]
ident_start [A-Za-z\200-\377_0-9#]
ident_cont [A-Za-z\200-\377_0-9\$]
identifier {ident_start}{ident_cont}*
extended_identifier {ident_start}{extended_ident_cont}*
/* fully qualified names regexes */
identifier_quoted {grave_accent}{extended_identifier}{grave_accent}
identifier_double_quoted {double_quote}{extended_identifier}{double_quote}

integer			[-+]?{digit}+
decimal			([-+]?({digit}*\.{digit}+)|({digit}+\.{digit}*))
real			({integer}|{decimal})[Ee][-+]?{digit}+
realfail1		({integer}|{decimal})[Ee]
realfail2		({integer}|{decimal})[Ee][-+]



%%


{identifier_quoted} { ddlget_lval(yyscanner)->str = scanner_copy( ddlget_text(yyscanner), yyscanner, STRIP_QUOTES ); return IDENT; }
{identifier_double_quoted} { ddlget_lval(yyscanner)->str = scanner_copy( ddlget_text(yyscanner), yyscanner, STRIP_QUOTES ); return DQ_IDENT; }

ACTION {return ACTION;}
ADD {return ADD;}
ALTER {return ALTER;}
AUTO_INCREMENT {return AUTO_INCREMENT;}
CASCADE {return CASCADE;}
CHAR {return IDB_CHAR;}
CHARACTER {return IDB_CHAR;}
BIGINT {return BIGINT;}
CHECK {BEGIN(check1);return CHECK;}
<check1>\( {BEGIN(check2); return '(';}
<check2>[^)]*/\) {BEGIN(check1); ddlget_lval(yyscanner)->str = scanner_copy(ddlget_text(yyscanner), yyscanner); return CP_SEARCH_CONDITION_TEXT;}
<check1>\) {BEGIN(0); return ')';}

{quote} {BEGIN(inquote);return yytext[0];}
<inquote>[^']*/' {BEGIN(endquote); ddlget_lval(yyscanner)->str = scanner_copy(ddlget_text(yyscanner), yyscanner); return SCONST;}
<endquote>' {BEGIN(0); return yytext[0];}

{integer} {ddlget_lval(yyscanner)->str = scanner_copy(ddlget_text(yyscanner), yyscanner); return ICONST;}
{decimal} {ddlget_lval(yyscanner)->str = scanner_copy(ddlget_text(yyscanner), yyscanner); return FCONST;}
{real} {ddlget_lval(yyscanner)->str = scanner_copy(ddlget_text(yyscanner), yyscanner); return FCONST;}

COMMENT {return COMMENT;}
COLUMN {return COLUMN;}
COLUMNS {return COLUMNS;}
CONSTRAINT {return CONSTRAINT;}
CONSTRAINTS {return CONSTRAINTS;}
CREATE {return CREATE;}
CURRENT_USER {return CURRENT_USER;}
DATE {ddlget_lval(yyscanner)->str=strdup("date"); return DATE;}
DATETIME {return DATETIME;}
TIME {ddlget_lval(yyscanner)->str=strdup("time"); return TIME;}
TIMESTAMP {return TIMESTAMP;}
DECIMAL {return DECIMAL;}
DEC {return DECIMAL;}
DEFAULT {return DEFAULT;}
DEFERRABLE {return DEFERRABLE;}
DEFERRED {return DEFERRED;}
DELETE {return IDB_DELETE;}
DROP {return DROP;}
ENGINE {return ENGINE;}
FOREIGN {return FOREIGN;}
FULL {return FULL;}
IMMEDIATE {return IMMEDIATE;}
INDEX {return INDEX;}
INITIALLY {return INITIALLY;}
INT {return IDB_INT;}
INTEGER {return INTEGER;}
KEY {return KEY;}
MATCH {return MATCH;}
MAX_ROWS {return MAX_ROWS;}
MIN_ROWS {return MIN_ROWS;}
MODIFY {return MODIFY;}
NO {return NO;}
NOT {return NOT;}
NULL {return NULL_TOK;}
NUMERIC {return NUMERIC;}
ON {return ON;}
PARTIAL {return PARTIAL;}
PRECISION {return PRECISION;}
PRIMARY {return PRIMARY;}
REFERENCES {return REFERENCES;}
RENAME {return RENAME;}
RESTRICT {return RESTRICT;}
SESSION_USER {return SESSION_USER;}
SIGNED {return SIGNED;}
SYSTEM_USER {return SYSTEM_USER;}
SET {return SET;}
SMALLINT {return SMALLINT;}
TABLE {return TABLE;}
TINYINT {return TINYINT;}
TO {return TO;}
UNIQUE {return UNIQUE;}
UNSIGNED {return UNSIGNED;}
UPDATE {return UPDATE;}
USER {return USER;}
VARCHAR {return VARCHAR;}
FLOAT {return IDB_FLOAT;}
DOUBLE {return DOUBLE;}
REAL {return REAL;}
CHARSET {return CHARSET;}
COLLATE {return COLLATE;}
IF {return IDB_IF;}
EXISTS {return EXISTS;}
CHANGE {return CHANGE;}
TRUNCATE {return TRUNCATE;}
VARBINARY {return VARBINARY;}
TINYBLOB {return TINYBLOB;}
BLOB {return BLOB;}
MEDIUMBLOB {return MEDIUMBLOB;}
LONGBLOB {return LONGBLOB;}
TINYTEXT {return TINYTEXT;}
TEXT {return TEXT;}
MEDIUMTEXT {return MEDIUMTEXT;}
LONGTEXT {return LONGTEXT;}
BOOL {return BOOL;}
BOOLEAN {return BOOLEAN;}
MEDIUMINT {return MEDIUMINT;}
ZEROFILL {return ZEROFILL;}

\n { lineno++;}

{whitespace} {
    /* ignore */
}

{identifier} { ddlget_lval(yyscanner)->str = scanner_copy(ddlget_text(yyscanner), yyscanner); return DQ_IDENT;}

{self} {
    return ddlget_text(yyscanner)[0];
}

{grave_accent} {
    return ddlget_text(yyscanner)[0];
}

"/*"            { BEGIN(c_comment); }
<c_comment>"*/" { BEGIN(0); }
<c_comment>\n   { }
<c_comment>.    { }
%%

void ddlerror(struct pass_to_bison* x, char const *s)
{
	printf("yyerror: %d: %s at %s\n", lineno, s, ddlget_text(x->scanner));
}

typedef std::vector<char*> valbuf_t;

#include <pthread.h>
using namespace ddlpackage;

/*
 * Called before any actual parsing is done
 */
void scanner_init(const char* str, yyscan_t yyscanner)
{
#ifdef YYDEBUG
    extern int ddldebug;
    ddldebug = 1;
#endif

	size_t slen = strlen(str);
   scan_data* pScanData = (scan_data*)ddlget_extra(yyscanner);

	/*
	 * Might be left over after ereport()
	 */
   struct yyguts_t * yyg = (struct yyguts_t*)yyscanner; // needed for macro YY_CURRENT_BUFFER
	if (YY_CURRENT_BUFFER)
      yy_delete_buffer(YY_CURRENT_BUFFER, yyscanner);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
   pScanData->scanbuf =  (char *)malloc(slen + 2);
   memcpy(pScanData->scanbuf, str, slen);
	pScanData->scanbuf[slen] = pScanData->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	pScanData->scanbufhandle = (void*)yy_scan_buffer(pScanData->scanbuf, slen + 2, yyscanner);
	//std::cout << "scanner_init " << (uint64_t)pScanData->scanbufhandle << std::endl;

	BEGIN(INITIAL);

   pScanData->valbuf.clear();
}


/*
 * Called after parsing is done to clean up after scanner_init()
 */


void scanner_finish(yyscan_t yyscanner)
{
   char* str;
   scan_data* pScanData = (scan_data*)ddlget_extra(yyscanner);
	//std::cout << "scanner_finish " << (uint64_t)pScanData->scanbufhandle << std::endl;
   yy_delete_buffer((YY_BUFFER_STATE)pScanData->scanbufhandle, yyscanner);
   free(pScanData->scanbuf);
   unsigned int i;
   for(i=0; i<pScanData->valbuf.size(); i++) {
      str = pScanData->valbuf[i];
      if(str) {
//        std::cout << "valbuf:(" << str << ")" << std::endl;
         free(pScanData->valbuf[i]);
      }
   }
   pScanData->valbuf.clear();
}

char* scanner_copy (char *str, yyscan_t yyscanner, copy_action_t action)
{
    char* result;
    char* nv = strdup(str);
    result = nv;
    // free strduped memory later to prevent possible memory leak
    if(nv)
        ((scan_data*)ddlget_extra(yyscanner))->valbuf.push_back(nv);

    if(action == STRIP_QUOTES)
    {
        nv[strlen(str) - 1] = '\0';
        result = nv + 1;
    }

    return result;
}
