#ifndef SQLPARSER_UPDATE_STATEMENT_H
#define SQLPARSER_UPDATE_STATEMENT_H

#include "SQLStatement.h"
#include "SelectStatement.h"

namespace hsql {

// Represents "column = value" expressions.
struct UpdateClause {
  char* column;
  Expr* value;
};

// Represents SQL Update statements.
struct UpdateStatement : SQLStatement {
  UpdateStatement();
  ~UpdateStatement() override;

  // TODO: switch to char* instead of TableRef
  TableRef* table;
  std::vector<UpdateClause*>* updates;
  Expr* where;
  LimitDescription* limit;
};

}  // namespace hsql

#endif
