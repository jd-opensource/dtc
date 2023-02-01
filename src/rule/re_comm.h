#ifndef _H_RE_COMM_
#define _H_RE_COMM_

#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"

typedef struct _expr_properity{
    hsql::Expr* rule;
    int condition_num;
}expr_properity;

#define YAML_DTC_BUFFER "YAML_DTC_BUFFER"
#define YAML_DTC_RULES "YAML_DTC_RULES"
#define YAML_DTC_KEY_TYPE "YAML_DTC_KEY_TYPE"
#define YAML_DTC_KEY_STRING "YAML_DTC_KEY_STRING"

#endif