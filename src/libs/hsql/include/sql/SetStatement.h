#ifndef SQLPARSER_SET_STATEMENT_H
#define SQLPARSER_SET_STATEMENT_H

#include "SQLStatement.h"
#include "Expr.h"

namespace hsql {

struct SetStatement : SQLStatement {
    SetStatement();
    ~SetStatement();

    Expr* equal_expr;
};

}// namespace hsql

#endif