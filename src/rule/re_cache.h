#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"

bool re_is_cache_sql(hsql::SQLParserResult* sql_ast, std::string key);
std::string get_table_name(hsql::SQLParserResult* sql_ast);
std::string get_schema(hsql::SQLParserResult* sql_ast);