/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

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

#ifndef YY_HSQL_BISON_PARSER_TAB_H_INCLUDED
# define YY_HSQL_BISON_PARSER_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef HSQL_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define HSQL_DEBUG 1
#  else
#   define HSQL_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define HSQL_DEBUG 1
# endif /* ! defined YYDEBUG */
#endif  /* ! defined HSQL_DEBUG */
#if HSQL_DEBUG
extern int hsql_debug;
#endif
/* "%code requires" blocks.  */
#line 38 "bison_parser.y" /* yacc.c:1909  */

  // clang-format on
  // %code requires block

#include "../SQLParserResult.h"
#include "../sql/statements.h"
#include "parser_typedef.h"

// Auto update column and line number
#define YY_USER_ACTION                        \
  yylloc->first_line = yylloc->last_line;     \
  yylloc->first_column = yylloc->last_column; \
  for (int i = 0; yytext[i] != '\0'; i++) {   \
    yylloc->total_column++;                   \
    yylloc->string_length++;                  \
    if (yytext[i] == '\n') {                  \
      yylloc->last_line++;                    \
      yylloc->last_column = 0;                \
    } else {                                  \
      yylloc->last_column++;                  \
    }                                         \
  }

#line 76 "bison_parser.tab.h" /* yacc.c:1909  */

/* Token type.  */
#ifndef HSQL_TOKENTYPE
# define HSQL_TOKENTYPE
  enum hsql_tokentype
  {
    SQL_IDENTIFIER = 258,
    SQL_STRING = 259,
    SQL_FLOATVAL = 260,
    SQL_INTVAL = 261,
    SQL_DEALLOCATE = 262,
    SQL_PARAMETERS = 263,
    SQL_INTERSECT = 264,
    SQL_TEMPORARY = 265,
    SQL_TIMESTAMP = 266,
    SQL_DISTINCT = 267,
    SQL_NVARCHAR = 268,
    SQL_RESTRICT = 269,
    SQL_TRUNCATE = 270,
    SQL_ANALYZE = 271,
    SQL_BETWEEN = 272,
    SQL_CASCADE = 273,
    SQL_COLUMNS = 274,
    SQL_CONTROL = 275,
    SQL_DEFAULT = 276,
    SQL_EXECUTE = 277,
    SQL_EXPLAIN = 278,
    SQL_INTEGER = 279,
    SQL_NATURAL = 280,
    SQL_PREPARE = 281,
    SQL_PRIMARY = 282,
    SQL_SCHEMAS = 283,
    SQL_CHARACTER_VARYING = 284,
    SQL_REAL = 285,
    SQL_DECIMAL = 286,
    SQL_SMALLINT = 287,
    SQL_BIGINT = 288,
    SQL_SPATIAL = 289,
    SQL_VARCHAR = 290,
    SQL_VIRTUAL = 291,
    SQL_DESCRIBE = 292,
    SQL_BEFORE = 293,
    SQL_COLUMN = 294,
    SQL_CREATE = 295,
    SQL_DELETE = 296,
    SQL_DIRECT = 297,
    SQL_DOUBLE = 298,
    SQL_ESCAPE = 299,
    SQL_EXCEPT = 300,
    SQL_EXISTS = 301,
    SQL_EXTRACT = 302,
    SQL_CAST = 303,
    SQL_FORMAT = 304,
    SQL_GLOBAL = 305,
    SQL_HAVING = 306,
    SQL_IMPORT = 307,
    SQL_INSERT = 308,
    SQL_ISNULL = 309,
    SQL_OFFSET = 310,
    SQL_RENAME = 311,
    SQL_SCHEMA = 312,
    SQL_SELECT = 313,
    SQL_SORTED = 314,
    SQL_TABLES = 315,
    SQL_UNIQUE = 316,
    SQL_UNLOAD = 317,
    SQL_UPDATE = 318,
    SQL_VALUES = 319,
    SQL_AFTER = 320,
    SQL_ALTER = 321,
    SQL_CROSS = 322,
    SQL_DATABASES = 323,
    SQL_DATABASE = 324,
    SQL_DELTA = 325,
    SQL_FLOAT = 326,
    SQL_GROUP = 327,
    SQL_INDEX = 328,
    SQL_INNER = 329,
    SQL_LIMIT = 330,
    SQL_LOCAL = 331,
    SQL_MERGE = 332,
    SQL_MINUS = 333,
    SQL_ORDER = 334,
    SQL_OUTER = 335,
    SQL_RIGHT = 336,
    SQL_TABLE = 337,
    SQL_UNION = 338,
    SQL_USING = 339,
    SQL_WHERE = 340,
    SQL_CALL = 341,
    SQL_CASE = 342,
    SQL_CHAR = 343,
    SQL_COPY = 344,
    SQL_DATE = 345,
    SQL_DATETIME = 346,
    SQL_DESC = 347,
    SQL_DROP = 348,
    SQL_ELSE = 349,
    SQL_FILE = 350,
    SQL_FROM = 351,
    SQL_FULL = 352,
    SQL_HASH = 353,
    SQL_HINT = 354,
    SQL_INTO = 355,
    SQL_JOIN = 356,
    SQL_LEFT = 357,
    SQL_LIKE = 358,
    SQL_LOAD = 359,
    SQL_LONG = 360,
    SQL_NULL = 361,
    SQL_PLAN = 362,
    SQL_SHOW = 363,
    SQL_TEXT = 364,
    SQL_THEN = 365,
    SQL_TIME = 366,
    SQL_VIEW = 367,
    SQL_WHEN = 368,
    SQL_WITH = 369,
    SQL_ADD = 370,
    SQL_ALL = 371,
    SQL_AND = 372,
    SQL_ASC = 373,
    SQL_END = 374,
    SQL_FOR = 375,
    SQL_INT = 376,
    SQL_KEY = 377,
    SQL_NOT = 378,
    SQL_OFF = 379,
    SQL_SET = 380,
    SQL_TOP = 381,
    SQL_AS = 382,
    SQL_BY = 383,
    SQL_IF = 384,
    SQL_IN = 385,
    SQL_IS = 386,
    SQL_OF = 387,
    SQL_ON = 388,
    SQL_OR = 389,
    SQL_TO = 390,
    SQL_NO = 391,
    SQL_ARRAY = 392,
    SQL_CONCAT = 393,
    SQL_ILIKE = 394,
    SQL_SECOND = 395,
    SQL_MINUTE = 396,
    SQL_HOUR = 397,
    SQL_DAY = 398,
    SQL_MONTH = 399,
    SQL_YEAR = 400,
    SQL_SECONDS = 401,
    SQL_MINUTES = 402,
    SQL_HOURS = 403,
    SQL_DAYS = 404,
    SQL_MONTHS = 405,
    SQL_YEARS = 406,
    SQL_INTERVAL = 407,
    SQL_TRUE = 408,
    SQL_FALSE = 409,
    SQL_BOOLEAN = 410,
    SQL_TRANSACTION = 411,
    SQL_BEGIN = 412,
    SQL_COMMIT = 413,
    SQL_ROLLBACK = 414,
    SQL_NOWAIT = 415,
    SQL_SKIP = 416,
    SQL_LOCKED = 417,
    SQL_SHARE = 418,
    SQL_EQUALS = 419,
    SQL_NOTEQUALS = 420,
    SQL_LESS = 421,
    SQL_GREATER = 422,
    SQL_LESSEQ = 423,
    SQL_GREATEREQ = 424,
    SQL_NOTNULL = 425,
    SQL_UMINUS = 426
  };
#endif
/* Tokens.  */
#define SQL_IDENTIFIER 258
#define SQL_STRING 259
#define SQL_FLOATVAL 260
#define SQL_INTVAL 261
#define SQL_DEALLOCATE 262
#define SQL_PARAMETERS 263
#define SQL_INTERSECT 264
#define SQL_TEMPORARY 265
#define SQL_TIMESTAMP 266
#define SQL_DISTINCT 267
#define SQL_NVARCHAR 268
#define SQL_RESTRICT 269
#define SQL_TRUNCATE 270
#define SQL_ANALYZE 271
#define SQL_BETWEEN 272
#define SQL_CASCADE 273
#define SQL_COLUMNS 274
#define SQL_CONTROL 275
#define SQL_DEFAULT 276
#define SQL_EXECUTE 277
#define SQL_EXPLAIN 278
#define SQL_INTEGER 279
#define SQL_NATURAL 280
#define SQL_PREPARE 281
#define SQL_PRIMARY 282
#define SQL_SCHEMAS 283
#define SQL_CHARACTER_VARYING 284
#define SQL_REAL 285
#define SQL_DECIMAL 286
#define SQL_SMALLINT 287
#define SQL_BIGINT 288
#define SQL_SPATIAL 289
#define SQL_VARCHAR 290
#define SQL_VIRTUAL 291
#define SQL_DESCRIBE 292
#define SQL_BEFORE 293
#define SQL_COLUMN 294
#define SQL_CREATE 295
#define SQL_DELETE 296
#define SQL_DIRECT 297
#define SQL_DOUBLE 298
#define SQL_ESCAPE 299
#define SQL_EXCEPT 300
#define SQL_EXISTS 301
#define SQL_EXTRACT 302
#define SQL_CAST 303
#define SQL_FORMAT 304
#define SQL_GLOBAL 305
#define SQL_HAVING 306
#define SQL_IMPORT 307
#define SQL_INSERT 308
#define SQL_ISNULL 309
#define SQL_OFFSET 310
#define SQL_RENAME 311
#define SQL_SCHEMA 312
#define SQL_SELECT 313
#define SQL_SORTED 314
#define SQL_TABLES 315
#define SQL_UNIQUE 316
#define SQL_UNLOAD 317
#define SQL_UPDATE 318
#define SQL_VALUES 319
#define SQL_AFTER 320
#define SQL_ALTER 321
#define SQL_CROSS 322
#define SQL_DATABASES 323
#define SQL_DATABASE 324
#define SQL_DELTA 325
#define SQL_FLOAT 326
#define SQL_GROUP 327
#define SQL_INDEX 328
#define SQL_INNER 329
#define SQL_LIMIT 330
#define SQL_LOCAL 331
#define SQL_MERGE 332
#define SQL_MINUS 333
#define SQL_ORDER 334
#define SQL_OUTER 335
#define SQL_RIGHT 336
#define SQL_TABLE 337
#define SQL_UNION 338
#define SQL_USING 339
#define SQL_WHERE 340
#define SQL_CALL 341
#define SQL_CASE 342
#define SQL_CHAR 343
#define SQL_COPY 344
#define SQL_DATE 345
#define SQL_DATETIME 346
#define SQL_DESC 347
#define SQL_DROP 348
#define SQL_ELSE 349
#define SQL_FILE 350
#define SQL_FROM 351
#define SQL_FULL 352
#define SQL_HASH 353
#define SQL_HINT 354
#define SQL_INTO 355
#define SQL_JOIN 356
#define SQL_LEFT 357
#define SQL_LIKE 358
#define SQL_LOAD 359
#define SQL_LONG 360
#define SQL_NULL 361
#define SQL_PLAN 362
#define SQL_SHOW 363
#define SQL_TEXT 364
#define SQL_THEN 365
#define SQL_TIME 366
#define SQL_VIEW 367
#define SQL_WHEN 368
#define SQL_WITH 369
#define SQL_ADD 370
#define SQL_ALL 371
#define SQL_AND 372
#define SQL_ASC 373
#define SQL_END 374
#define SQL_FOR 375
#define SQL_INT 376
#define SQL_KEY 377
#define SQL_NOT 378
#define SQL_OFF 379
#define SQL_SET 380
#define SQL_TOP 381
#define SQL_AS 382
#define SQL_BY 383
#define SQL_IF 384
#define SQL_IN 385
#define SQL_IS 386
#define SQL_OF 387
#define SQL_ON 388
#define SQL_OR 389
#define SQL_TO 390
#define SQL_NO 391
#define SQL_ARRAY 392
#define SQL_CONCAT 393
#define SQL_ILIKE 394
#define SQL_SECOND 395
#define SQL_MINUTE 396
#define SQL_HOUR 397
#define SQL_DAY 398
#define SQL_MONTH 399
#define SQL_YEAR 400
#define SQL_SECONDS 401
#define SQL_MINUTES 402
#define SQL_HOURS 403
#define SQL_DAYS 404
#define SQL_MONTHS 405
#define SQL_YEARS 406
#define SQL_INTERVAL 407
#define SQL_TRUE 408
#define SQL_FALSE 409
#define SQL_BOOLEAN 410
#define SQL_TRANSACTION 411
#define SQL_BEGIN 412
#define SQL_COMMIT 413
#define SQL_ROLLBACK 414
#define SQL_NOWAIT 415
#define SQL_SKIP 416
#define SQL_LOCKED 417
#define SQL_SHARE 418
#define SQL_EQUALS 419
#define SQL_NOTEQUALS 420
#define SQL_LESS 421
#define SQL_GREATER 422
#define SQL_LESSEQ 423
#define SQL_GREATEREQ 424
#define SQL_NOTNULL 425
#define SQL_UMINUS 426

/* Value type.  */
#if ! defined HSQL_STYPE && ! defined HSQL_STYPE_IS_DECLARED

union HSQL_STYPE
{
#line 98 "bison_parser.y" /* yacc.c:1909  */

  // clang-format on
  bool bval;
  char* sval;
  double fval;
  int64_t ival;
  uintmax_t uval;

  // statements
  hsql::AlterStatement* alter_stmt;
  hsql::CreateStatement* create_stmt;
  hsql::DeleteStatement* delete_stmt;
  hsql::DropStatement* drop_stmt;
  hsql::ExecuteStatement* exec_stmt;
  hsql::ExportStatement* export_stmt;
  hsql::ImportStatement* import_stmt;
  hsql::InsertStatement* insert_stmt;
  hsql::PrepareStatement* prep_stmt;
  hsql::SelectStatement* select_stmt;
  hsql::ShowStatement* show_stmt;
  hsql::SQLStatement* statement;
  hsql::TransactionStatement* transaction_stmt;
  hsql::UpdateStatement* update_stmt;

  hsql::Alias* alias_t;
  hsql::AlterAction* alter_action_t;
  hsql::ColumnDefinition* column_t;
  hsql::ColumnType column_type_t;
  hsql::ConstraintType column_constraint_t;
  hsql::DatetimeField datetime_field;
  hsql::DropColumnAction* drop_action_t;
  hsql::Expr* expr;
  hsql::GroupByDescription* group_t;
  hsql::ImportType import_type_t;
  hsql::JoinType join_type;
  hsql::LimitDescription* limit;
  hsql::OrderDescription* order;
  hsql::OrderType order_type;
  hsql::SetOperation* set_operator_t;
  hsql::TableConstraint* table_constraint_t;
  hsql::TableElement* table_element_t;
  hsql::TableName table_name;
  hsql::TableRef* table;
  hsql::UpdateClause* update_t;
  hsql::WithDescription* with_description_t;
  hsql::LockingClause* locking_t;

  std::vector<char*>* str_vec;
  std::set<hsql::ConstraintType>* column_constraint_set;
  std::vector<hsql::Expr*>* expr_vec;
  std::vector<hsql::OrderDescription*>* order_vec;
  std::vector<hsql::SQLStatement*>* stmt_vec;
  std::vector<hsql::TableElement*>* table_element_vec;
  std::vector<hsql::TableRef*>* table_vec;
  std::vector<hsql::UpdateClause*>* update_vec;
  std::vector<hsql::WithDescription*>* with_description_vec;
  std::vector<hsql::LockingClause*>* locking_clause_vec;

  std::pair<int64_t, int64_t>* ival_pair;

  hsql::RowLockMode lock_mode_t;
  hsql::RowLockWaitPolicy lock_wait_policy_t;

#line 494 "bison_parser.tab.h" /* yacc.c:1909  */
};

typedef union HSQL_STYPE HSQL_STYPE;
# define HSQL_STYPE_IS_TRIVIAL 1
# define HSQL_STYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined HSQL_LTYPE && ! defined HSQL_LTYPE_IS_DECLARED
typedef struct HSQL_LTYPE HSQL_LTYPE;
struct HSQL_LTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define HSQL_LTYPE_IS_DECLARED 1
# define HSQL_LTYPE_IS_TRIVIAL 1
#endif



int hsql_parse (hsql::SQLParserResult* result, yyscan_t scanner);

#endif /* !YY_HSQL_BISON_PARSER_TAB_H_INCLUDED  */
