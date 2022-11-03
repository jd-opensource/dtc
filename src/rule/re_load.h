#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include <string>
#include <vector>

using namespace std;

int get_rule_condition_num(hsql::Expr* rule);
int re_load_rule(std::string buf, hsql::SQLParserResult* rule_ast, vector<vector<hsql::Expr*> >* expr_rules);
std::string load_dtc_yaml_buffer(int mid);