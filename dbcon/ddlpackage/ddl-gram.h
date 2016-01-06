
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton interface for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     ACTION = 258,
     ADD = 259,
     ALTER = 260,
     AUTO_INCREMENT = 261,
     BIGINT = 262,
     BIT = 263,
     IDB_BLOB = 264,
     CASCADE = 265,
     IDB_CHAR = 266,
     CHARACTER = 267,
     CHECK = 268,
     CLOB = 269,
     COLUMN = 270,
     COLUMNS = 271,
     COMMENT = 272,
     CONSTRAINT = 273,
     CONSTRAINTS = 274,
     CREATE = 275,
     CURRENT_USER = 276,
     DATETIME = 277,
     DEC = 278,
     DECIMAL = 279,
     DEFAULT = 280,
     DEFERRABLE = 281,
     DEFERRED = 282,
     IDB_DELETE = 283,
     DROP = 284,
     ENGINE = 285,
     FOREIGN = 286,
     FULL = 287,
     IMMEDIATE = 288,
     INDEX = 289,
     INITIALLY = 290,
     IDB_INT = 291,
     INTEGER = 292,
     KEY = 293,
     MATCH = 294,
     MAX_ROWS = 295,
     MIN_ROWS = 296,
     MODIFY = 297,
     NO = 298,
     NOT = 299,
     NULL_TOK = 300,
     NUMBER = 301,
     NUMERIC = 302,
     ON = 303,
     PARTIAL = 304,
     PRECISION = 305,
     PRIMARY = 306,
     REFERENCES = 307,
     RENAME = 308,
     RESTRICT = 309,
     SET = 310,
     SMALLINT = 311,
     TABLE = 312,
     TIME = 313,
     TINYINT = 314,
     TO = 315,
     UNIQUE = 316,
     UNSIGNED = 317,
     UPDATE = 318,
     USER = 319,
     SESSION_USER = 320,
     SYSTEM_USER = 321,
     VARCHAR = 322,
     VARBINARY = 323,
     VARYING = 324,
     WITH = 325,
     ZONE = 326,
     DOUBLE = 327,
     IDB_FLOAT = 328,
     REAL = 329,
     CHARSET = 330,
     IDB_IF = 331,
     EXISTS = 332,
     CHANGE = 333,
     TRUNCATE = 334,
     IDENT = 335,
     FCONST = 336,
     SCONST = 337,
     CP_SEARCH_CONDITION_TEXT = 338,
     ICONST = 339,
     DATE = 340
   };
#endif
/* Tokens.  */
#define ACTION 258
#define ADD 259
#define ALTER 260
#define AUTO_INCREMENT 261
#define BIGINT 262
#define BIT 263
#define IDB_BLOB 264
#define CASCADE 265
#define IDB_CHAR 266
#define CHARACTER 267
#define CHECK 268
#define CLOB 269
#define COLUMN 270
#define COLUMNS 271
#define COMMENT 272
#define CONSTRAINT 273
#define CONSTRAINTS 274
#define CREATE 275
#define CURRENT_USER 276
#define DATETIME 277
#define DEC 278
#define DECIMAL 279
#define DEFAULT 280
#define DEFERRABLE 281
#define DEFERRED 282
#define IDB_DELETE 283
#define DROP 284
#define ENGINE 285
#define FOREIGN 286
#define FULL 287
#define IMMEDIATE 288
#define INDEX 289
#define INITIALLY 290
#define IDB_INT 291
#define INTEGER 292
#define KEY 293
#define MATCH 294
#define MAX_ROWS 295
#define MIN_ROWS 296
#define MODIFY 297
#define NO 298
#define NOT 299
#define NULL_TOK 300
#define NUMBER 301
#define NUMERIC 302
#define ON 303
#define PARTIAL 304
#define PRECISION 305
#define PRIMARY 306
#define REFERENCES 307
#define RENAME 308
#define RESTRICT 309
#define SET 310
#define SMALLINT 311
#define TABLE 312
#define TIME 313
#define TINYINT 314
#define TO 315
#define UNIQUE 316
#define UNSIGNED 317
#define UPDATE 318
#define USER 319
#define SESSION_USER 320
#define SYSTEM_USER 321
#define VARCHAR 322
#define VARBINARY 323
#define VARYING 324
#define WITH 325
#define ZONE 326
#define DOUBLE 327
#define IDB_FLOAT 328
#define REAL 329
#define CHARSET 330
#define IDB_IF 331
#define EXISTS 332
#define CHANGE 333
#define TRUNCATE 334
#define IDENT 335
#define FCONST 336
#define SCONST 337
#define CP_SEARCH_CONDITION_TEXT 338
#define ICONST 339
#define DATE 340




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  ddlpackage::AlterTableStatement *alterTableStmt;
  ddlpackage::AlterTableAction *ata;
  ddlpackage::AlterTableActionList *ataList;
  ddlpackage::DDL_CONSTRAINT_ATTRIBUTES cattr;
  std::pair<std::string, std::string> *tableOption;
  const char *columnOption;
  ddlpackage::ColumnConstraintDef *columnConstraintDef;
  ddlpackage::ColumnNameList *columnNameList;
  ddlpackage::ColumnType* columnType;
  ddlpackage::ConstraintAttributes *constraintAttributes;
  ddlpackage::ColumnConstraintList *constraintList;
  ddlpackage::DDL_CONSTRAINTS constraintType;
  double dval;
  bool flag;
  int ival;
  ddlpackage::QualifiedName *qualifiedName;
  ddlpackage::SchemaObject *schemaObject;
  ddlpackage::SqlStatement *sqlStmt;
  ddlpackage::SqlStatementList *sqlStmtList;
  const char *str;
  ddlpackage::TableConstraintDef *tableConstraint;
  ddlpackage::TableElementList *tableElementList;
  ddlpackage::TableOptionMap *tableOptionMap;
  ddlpackage::ColumnDefaultValue *colDefault;
  ddlpackage::DDL_MATCH_TYPE matchType;
  ddlpackage::DDL_REFERENTIAL_ACTION refActionCode;
  ddlpackage::ReferentialAction *refAction;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE ddllval;


