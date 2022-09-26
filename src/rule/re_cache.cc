#include "re_cache.h"
#include "re_load.h"
#include "log.h"

using namespace hsql;

#define SPECIFIC_L1_SCHEMA "L1"
#define SPECIFIC_L2_SCHEMA "L2"
#define SPECIFIC_L3_SCHEMA "L3"

int is_single_talbe()
{
    return 0;
}

bool is_select(SQLParserResult* sql_ast)
{
    if(sql_ast->getStatement(0)->type() == kStmtSelect)
        return true;

    return false;
}

int get_select_fields_num(SQLParserResult* sql_ast)
{
    const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
    return stmt->selectList->size();
}

int is_select_count(const SelectStatement* stmt, int index)
{
    //TODO
    return 0;
}

int get_where_condition_num(SQLParserResult* sql_ast)
{
    const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
    Expr* where = stmt->whereClause;
    if(!where) 
        return 0;

    get_rule_condition_num(where);
    return 0;
}

bool is_where_key(SQLParserResult* sql_ast, std::string keyname)
{
    const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
    Expr* where = stmt->whereClause;
    if(!where) 
        return false;

    if(where->isType(kExprOperator) && where->opType == kOpEquals)
    {
        if(where->expr->getName() == keyname)
            return true;
    }
        
    return false;
}

bool is_where_and(SQLParserResult* sql_ast)
{
    const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
    Expr* where = stmt->whereClause;
    if(!where) 
        return false;

   return false;
}

bool is_complex_keyword(SQLParserResult* sql_ast)
{
    const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
    const TableRef* table = stmt->fromTable;

    if(stmt->order || stmt->groupBy || table->join || table->type != kTableName)
        return true;
    
    return false;
}

bool is_complex_sql(SQLParserResult* sql_ast)
{
    const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
    Expr* where = stmt->whereClause;
    if(!where) 
        return false;

    Expr* cond1 = where->expr;
    if(cond1)
    {
        if((cond1->opType >= kOpNot && cond1->opType <= kOpExists) || 
        (cond1->opType >= kOpCase && cond1->opType <= kOpCaret))
            return true;
    }

    Expr* cond2 = where->expr2;
    if(cond2)
    {
        if((cond2->opType >= kOpNot && cond2->opType <= kOpExists) || 
            (cond2->opType >= kOpCase && cond2->opType <= kOpCaret))
            return true;
    }

    return false;
}

std::string get_schema(SQLParserResult* sql_ast)
{
    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtSelect)
    {
        const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
        TableRef* table = stmt->fromTable;
        if(table)
        {
            log4cplus_debug("type: %d", table->type);
            if(table->type == kTableName && table->schema)
            {
                return std::string(table->schema);
            }
        }
    }
    else if(t == kStmtInsert)
    {
        const InsertStatement* stmt = (const InsertStatement*)(sql_ast->getStatement(0));
        if(stmt && stmt->schema)
            return std::string(stmt->schema);
    }
    else if(t == kStmtUpdate)
    {
        const UpdateStatement* stmt = (const UpdateStatement*)(sql_ast->getStatement(0));
        TableRef* table = stmt->table;
        if(table)
        {
            if(table->type == kTableName && table->schema)
                return std::string(table->schema);
        }
    }
    else if(t == kStmtDelete)
    {
        const DeleteStatement* stmt = (const DeleteStatement*)(sql_ast->getStatement(0));
        if(stmt && stmt->schema)
        {
            return std::string(stmt->schema);
        }
    }
  
    return "";
}

std::string get_table_name(SQLParserResult* sql_ast)
{
    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtSelect)
    {
        const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
        TableRef* table = stmt->fromTable;
        if(table)
        {
            log4cplus_debug("type: %d", table->type);
            if(table->type == kTableName && table->name)
            {
                return std::string(table->name);
            }
        }
    }
    else if(t == kStmtInsert)
    {
        const InsertStatement* stmt = (const InsertStatement*)(sql_ast->getStatement(0));
        if(stmt && stmt->tableName)
            return std::string(stmt->tableName);
    }
    else if(t == kStmtUpdate)
    {
        const UpdateStatement* stmt = (const UpdateStatement*)(sql_ast->getStatement(0));
        TableRef* table = stmt->table;
        if(table)
        {
            if(table->type == kTableName && table->name)
                return std::string(table->name);
        }
    }
    else if(t == kStmtDelete)
    {
        const DeleteStatement* stmt = (const DeleteStatement*)(sql_ast->getStatement(0));
        if(stmt && stmt->tableName)
        {
            return std::string(stmt->tableName);
        }
    }
  
    return "";
}

bool is_dtc_adapt_type(SQLParserResult* sql_ast)
{
    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtSelect || t == kStmtInsert || t == kStmtUpdate || t == kStmtDelete)
        return true;
    else
        return false;
}

int check_dtc_key(hsql::Expr* rule, std::string key)
{
    int count = 0;
    if(!rule)
        return 0;

    if(rule->isType(kExprOperator) && rule->opType == kOpAnd)
    {
        count += check_dtc_key(rule->expr, key);
        count += check_dtc_key(rule->expr2, key);
        return count;
    }
    else if(rule->isType(kExprOperator) && rule->opType == kOpEquals)
    {
        if(rule->expr->getName() == key)
            return 1;
    }

    return 0;
}

bool re_is_cache_sql(SQLParserResult* sql_ast, std::string key)
{
    if(sql_ast->size() > 1)
        return false;

    if(!is_dtc_adapt_type(sql_ast))
        return false;

    StatementType type = sql_ast->getStatement(0)->type();

    if(type == kStmtSelect)
    {
        const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
        Expr* where = stmt->whereClause;
        if(!where) 
            return false;
        if(check_dtc_key(where, key) == 1)
            return true;
    }
    else if(type == kStmtDelete)
    {
        const DeleteStatement* stmt = (const DeleteStatement*)(sql_ast->getStatement(0));
        Expr* where = stmt->expr;
        if(!where) 
            return false;

        if(check_dtc_key(where, key) == 1)
            return true;
    }
    else if(type == kStmtUpdate)
    {
        const UpdateStatement* stmt = (const UpdateStatement*)(sql_ast->getStatement(0));
        Expr* where = stmt->where;
        if(!where) 
            return false;

        if(check_dtc_key(where, key) == 1)
            return true;
    }
    else if(type == kStmtInsert)
    {
        const InsertStatement* stmt = (const InsertStatement*)(sql_ast->getStatement(0));
        if(stmt->type != kInsertValues)
            return false;
        
        if(stmt->columns == NULL && stmt->values->size() > 0)
        {
            return true;
        }

        for(int i = 0; i < stmt->columns->size(); i++)
        {
            if(std::string(stmt->columns->at(i)) == key)
                return true;
        }
    }
    else
    {
        return false;
    }

    return false;
}