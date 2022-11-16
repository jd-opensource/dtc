#include <vector>
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"

using namespace hsql;

std::string fun_date_add(std::vector<Expr*>* elist);
std::string fun_date_sub(std::vector<Expr*>* elist);