#ifndef _H_RE_COMM_
#define _H_RE_COMM_

#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"

typedef struct _expr_properity{
    hsql::Expr* rule;
    int condition_num;
}expr_properity;

#endif