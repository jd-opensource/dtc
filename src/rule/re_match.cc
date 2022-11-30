#include "re_match.h"
#include "re_comm.h"
#include "log.h"
#include "re_function.h"
#include "re_cache.h"

using namespace hsql;
using namespace std;
hsql::SQLParserResult sql_ast;

#define SPECIFIC_L3_SCHEMA "L3"
#define SPECIFIC_L1_SCHEMA "L1"
#define SPECIFIC_L2_SCHEMA "L2"

//legitimacy check.
int do_check_sql()
{
    return 0;
}

// parse sql to AST.
int re_parse_sql(std::string sql, hsql::SQLParserResult* sql_ast)
{
    log4cplus_debug("input sql parse start..");
    bool r = hsql::SQLParser::parse(sql, sql_ast);
    if(r == false)
    {
        log4cplus_debug("parse failed, sql: %s", sql.c_str());
        return -1; 
    }
    if (!sql_ast->isValid() || sql_ast->size() <= 0)
    {
        log4cplus_debug("valid:%d, size:%d, %s", sql_ast->isValid(), sql_ast->size(), sql.c_str());
        return -1; 
    }
    log4cplus_debug("input sql parse end");
    return 0;
}

bool hsql_convert_value_int(hsql::Expr* input, int* out)
{
    if(input->isType(kExprLiteralInt))
    {
        *out = input->ival;
        return true;
    }
    else if(input->isType(kExprLiteralString))
    {
        char *endptr = NULL;
        long result = strtol(input->name, &endptr, 10);
        if(endptr == input->name)
            return false;
        if(strlen(endptr) > 0)
            return false;
        *out = result;
        return true;    
    }
    else if(input->isType(kExprLiteralFloat))
    {
        *out = static_cast<int>(input->fval);
        return true;
    }

    return false;
}

std::string hsql_convert_value_string(hsql::Expr* input)
{
    if(input->isType(kExprLiteralInt))
    {
        return to_string(input->ival);
    }
    else if(input->isType(kExprLiteralString))
    {
        return input->name;
    }
    else if(input->isType(kExprLiteralFloat))
    {
        return to_string(input->fval);
    }

    return "";
}

bool hsql_convert_value_float(hsql::Expr* input, double* out)
{
    if(input->isType(kExprLiteralFloat))
    {
        *out = input->fval;
        return true;
    }
    else if(input->isType(kExprLiteralString))
    {
        char *endptr = NULL;
        double result = strtod(input->name, &endptr);
        if(endptr == input->name)
            return false;
        if(strlen(endptr) > 0)
            return false;
        *out = result;
        return true;    
    }
    else if(input->isType(kExprLiteralInt))
    {
        *out = static_cast<double>(input->ival);
        return true;
    }

    return false;
}

unsigned int hsql_convert_functionref_int(hsql::Expr* rule)
{
    if(strcasecmp(rule->name, "unix_timestamp") == 0)
    {
        std::vector<Expr*>* elist = rule->exprList;
        if(elist->size() != 1)
        {
            log4cplus_error("elist size(%d) error", elist->size());
            return 0;
        }

        Expr* expr = (*elist)[0];
        std::string rv;
        if(expr->type == kExprFunctionRef)
        {
            ExprType rule_run_converted_type = convert_rule_function_type(expr);
            if(rule_run_converted_type == kExprLiteralString)
            {
                rv = hsql_convert_functionref_string(expr);
            }
        }
        else if(expr->type == kExprLiteralString)
        {
            rv = expr->name;
        }

        tm tminfo = {0};
        strptime(rv.c_str(), "%Y-%m-%d %H:%M:%S", &tminfo);

        time_t t = mktime(&tminfo);
        log4cplus_debug("timestamp: %ld, %d-%d-%d %d:%d:%d", t, tminfo.tm_year, tminfo.tm_mon, tminfo.tm_mday, tminfo.tm_hour, tminfo.tm_min, tminfo.tm_sec);
        return t;
    }

    return 0;
}

std::string hsql_convert_functionref_string(hsql::Expr* rule)
{
    if(strcasecmp(rule->name, "date_sub") == 0)
    {
        std::vector<Expr*>* elist = rule->exprList;
        if(elist->size() != 2)
        {
            log4cplus_error("elist size(%d) error", elist->size());
            return "";
        }
        std::string r = fun_date_sub(elist);
        log4cplus_debug("new date:%s", r.c_str());
        return r;
    }
    else if(strcasecmp(rule->name, "date_add") == 0)
    {
        std::vector<Expr*>* elist = rule->exprList;
        if(elist->size() != 2)
        {
            log4cplus_error("elist size(%d) error", elist->size());
            return "";
        }
        std::string r = fun_date_add(elist);
        log4cplus_debug("new date:%s", r.c_str());
        return r;
    }
    else
    {
        log4cplus_error("function: %s unsupported right now.", rule->name);
        return "";
    }

    return "";
}

ExprType convert_rule_function_type(hsql::Expr* rule)
{
    if(strcasecmp(rule->name, "date_sub") == 0)
    {
        return kExprLiteralString;
    }
    else if(strcasecmp(rule->name, "date_add") == 0)
    {
        return kExprLiteralString;
    }
    else if(strcasecmp(rule->name, "unix_timestamp") == 0)
    {
        return kExprLiteralInt;
    }

    return kExprFunctionRef;
}

bool cmp_expr_value(hsql::Expr* input, hsql::Expr* rule, OperatorType input_type, OperatorType rule_type)
{
    if(input->type != rule->type)
    {
        if(input->type > kExprLiteralInt || (rule->type > kExprLiteralInt && rule->type != kExprFunctionRef))
            return false;
    }

    if(input_type == kOpEquals && rule_type == kOpEquals)
    {
        if(rule->isType(kExprLiteralFloat))
        {
            double v;
            bool valid = hsql_convert_value_float(input, &v);
            if(!valid)
                return false;
            if(v == rule->fval)
                return true;
            else
                return false;
        }
        else if(rule->isType(kExprLiteralInt))
        {
            int v;
            bool valid = hsql_convert_value_int(input, &v);
            if(!valid)
                return false;
            if(v == rule->ival)
                return true;
            else
                return false;
        }
        else if(rule->isType(kExprLiteralString))
        {
            std::string v = hsql_convert_value_string(input);
            if(v == std::string(rule->name))
                return true;
            else
                return false;
        }
        else if(rule->isType(kExprFunctionRef))     
        {
            ExprType rule_run_converted_type = convert_rule_function_type(rule);
            if(rule_run_converted_type == kExprLiteralString)
            {
                std::string rv = hsql_convert_functionref_string(rule);
                std::string v = hsql_convert_value_string(input);
                if(rv == v)
                    return true;
                else
                    return false;
            }
            else if(rule_run_converted_type == kExprLiteralInt)
            {
                unsigned int rv = hsql_convert_functionref_int(rule);
                int v = 0;
                bool valid = hsql_convert_value_int(input, &v);
                if(rv == v)
                    return true;
                else
                    return false;
            }            
        }   
    }
    else if(input_type == kOpNotEquals && rule_type == kOpNotEquals)
    {
        if(rule->isType(kExprLiteralFloat))
        {
            double v;
            bool valid = hsql_convert_value_float(input, &v);
            if(!valid)
                return false;
            if(v != rule->fval)
                return true;
            else
                return false;
        }
        else if(rule->isType(kExprLiteralInt))
        {
            int v;
            bool valid = hsql_convert_value_int(input, &v);
            if(!valid)
                return false;
            if(v != rule->ival)
                return true;
            else
                return false;
        }
        else if(rule->isType(kExprLiteralString))
        {
            std::string v = hsql_convert_value_string(input);
            if(v == std::string(rule->name))
                return false;
            else
                return true;
        }
        else if(rule->isType(kExprFunctionRef))     
        {
            ExprType rule_run_converted_type = convert_rule_function_type(rule);
            if(rule_run_converted_type == kExprLiteralString)
            {
                std::string rv = hsql_convert_functionref_string(rule);
                std::string v = hsql_convert_value_string(input);
                if(rv == v)
                    return false;
                else
                    return true;
            }
            else if(rule_run_converted_type == kExprLiteralInt)
            {
                unsigned int rv = hsql_convert_functionref_int(rule);
                int v = 0;
                bool valid = hsql_convert_value_int(input, &v);          
                if(rv == v)
                    return false;
                else
                    return true;                      
            }
        }   
    }
    else if(rule_type == kOpLess)
    {
        if(rule->isType(kExprLiteralFloat))
        {
            double v;
            bool valid = hsql_convert_value_float(input, &v);
            if(!valid)
                return false;
            if(input_type == kOpLess)
            {
                if(v <= rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpLessEq)
            {
                if(v < rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(v < rule->fval)
                    return true;
                else
                    return false;
            }            
        }
        else if(rule->isType(kExprLiteralInt))
        {
            int v;
            bool valid = hsql_convert_value_int(input, &v);
            if(!valid)
                return false;

            if(input_type == kOpLess)
            {
                if(v <= rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpLessEq)
            {
                if(v < rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(v < rule->ival)
                    return true;
                else
                    return false;
            }            
        }
        else if(rule->isType(kExprFunctionRef))     
        {
            ExprType rule_run_converted_type = convert_rule_function_type(rule);
            if(rule_run_converted_type == kExprLiteralString)
            {
                std::string rv = hsql_convert_functionref_string(rule);
                std::string v = hsql_convert_value_string(input);
                if(input_type == kOpLess)
                {
                    if(v.length() == rv.length() && v <= rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpLessEq)
                {
                    if(v.length() == rv.length() && v < rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpEquals)
                {
                    if(v.length() == rv.length() && v < rv)
                        return true;
                    else
                        return false;
                }            
            }
            else if(rule_run_converted_type == kExprLiteralInt)
            {
                unsigned int rv = hsql_convert_functionref_int(rule);
                int v = 0;
                bool valid = hsql_convert_value_int(input, &v);
                if(input_type == kOpLess)
                {
                    if(v <= rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpLessEq)
                {
                    if(v < rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpEquals)
                {
                    if(v < rv)
                        return true;
                    else
                        return false;
                }                            
            }
        }   
    }
    else if(rule_type == kOpLessEq)
    {
        if(rule->isType(kExprLiteralFloat))
        {
            if(input_type == kOpLess || input_type == kOpLessEq || input_type == kOpEquals)
            {
                double v;
                bool valid = hsql_convert_value_float(input, &v);
                if(!valid)
                    return false;
                if(v <= rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(rule->isType(kExprLiteralInt))
        {
            int v;
            bool valid = hsql_convert_value_int(input, &v);
            if(!valid)
                return false;
            if(input_type == kOpLess || input_type == kOpLessEq || input_type == kOpEquals)
            {
                if(v <= rule->ival)
                    return true;
                else
                    return false;
            }
        }
        else if(rule->isType(kExprFunctionRef))     
        {
            ExprType rule_run_converted_type = convert_rule_function_type(rule);
            if(rule_run_converted_type == kExprLiteralString)
            {
                std::string rv = hsql_convert_functionref_string(rule);
                std::string v = hsql_convert_value_string(input);
                if(input_type == kOpLess || input_type == kOpLessEq || input_type == kOpEquals)
                {
                    if(v.length() == rv.length() && v <= rv)
                        return true;
                    else
                        return false;
                }
            }
            else if(rule_run_converted_type == kExprLiteralInt)
            {
                unsigned int rv = hsql_convert_functionref_int(rule);
                int v = 0;
                bool valid = hsql_convert_value_int(input, &v);
                if(input_type == kOpLess || input_type == kOpLessEq || input_type == kOpEquals)
                {
                    if(v <= rv)
                        return true;
                    else
                        return false;
                }
            }
        }           
    }
    else if(rule_type == kOpGreater)
    {
        if(rule->isType(kExprLiteralFloat))
        {
            double v;
            bool valid = hsql_convert_value_float(input, &v);
            if(!valid)
                return false;
            if(input_type == kOpGreater)
            {
                if(v >= rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpGreaterEq)
            {
                if(v > rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(v > rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(rule->isType(kExprLiteralInt))
        {
            int v;
            bool valid = hsql_convert_value_int(input, &v);
            if(!valid)
                return false;

            if(input_type == kOpGreater)
            {
                if(v >= rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpGreaterEq)
            {
                if(v > rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(v > rule->ival)
                    return true;
                else
                    return false;
            }
        }
        else if(rule->isType(kExprFunctionRef))    
        {
            ExprType rule_run_converted_type = convert_rule_function_type(rule);
            if(rule_run_converted_type == kExprLiteralString)
            {
                std::string rv = hsql_convert_functionref_string(rule);
                std::string v = hsql_convert_value_string(input);
                if(input_type == kOpGreater)
                {
                    if(v.length() == rv.length() && v >= rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpGreaterEq)
                {
                    if(v.length() == rv.length() && v > rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpEquals)
                {
                    if(v.length() == rv.length() && v > rv)
                        return true;
                    else
                        return false;
                }
            }            
            else if(rule_run_converted_type == kExprLiteralInt)
            {
                unsigned int rv = hsql_convert_functionref_int(rule);
                int v = 0;
                bool valid = hsql_convert_value_int(input, &v);

                if(input_type == kOpGreater)
                {
                    if(v >= rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpGreaterEq)
                {
                    if(v > rv)
                        return true;
                    else
                        return false;
                }
                else if(input_type == kOpEquals)
                {
                    if(v > rv)
                        return true;
                    else
                        return false;
                }
            }
        }
    }
    else if(rule_type == kOpGreaterEq)
    {
        if(rule->isType(kExprLiteralFloat))
        {
            if(input_type == kOpGreater || input_type == kOpGreaterEq || input_type == kOpEquals)
            {
                double v;
                bool valid = hsql_convert_value_float(input, &v);
                if(!valid)
                    return false;
                if(v >= rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(rule->isType(kExprLiteralInt))
        {
            if(input_type == kOpGreater || input_type == kOpGreaterEq || input_type == kOpEquals)
            {
                int v;
                bool valid = hsql_convert_value_int(input, &v);
                if(!valid)
                    return false;                
                if(v >= rule->ival)
                    return true;
                else
                    return false;
            }
        }
        else if(rule->isType(kExprFunctionRef))    
        {
            ExprType rule_run_converted_type = convert_rule_function_type(rule);
            if(rule_run_converted_type == kExprLiteralString)
            {
                std::string rv = hsql_convert_functionref_string(rule);
                std::string v = hsql_convert_value_string(input);

                if(input_type == kOpGreater || input_type == kOpGreaterEq || input_type == kOpEquals)
                {
                    if(v.length() == rv.length() && v >= rv)
                        return true;
                    else
                        return false;
                }
            }            
            else if(rule_run_converted_type == kExprLiteralInt)
            {
                unsigned int rv = hsql_convert_functionref_int(rule);
                int v = 0;
                bool valid = hsql_convert_value_int(input, &v);
                if(input_type == kOpGreater || input_type == kOpGreaterEq || input_type == kOpEquals)
                {
                    if(v >= rv)
                        return true;
                    else
                        return false;
                }
            }
        }
    }

    return false;
}


bool do_match_expr(hsql::Expr* input, hsql::Expr* rule)
{
    if(!input->isType(kExprOperator) || !rule->isType(kExprOperator))
        return false;

    if(input->opType != rule->opType && (input->opType < kOpEquals || input->opType > kOpGreaterEq))
        return false;

    log4cplus_debug("do_MATCH_EXPR:%s %s", input->expr->getName(), rule->expr->getName());
    if(strcasecmp(input->expr->getName(), rule->expr->getName()) != 0)
        return false;

    bool result = cmp_expr_value(input->expr2, rule->expr2, input->opType, rule->opType);
    log4cplus_debug("match result: %d, name: %s", result, input->expr->getName());
    return result;
}

bool is_write_type(SQLParserResult* sql_ast)
{
    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtInsert)
        return true;
    else if(t == kStmtUpdate)
        return true;
    else if(t == kStmtDelete)
        return true;        
    else
        return false;
}

bool is_update_delete_type(SQLParserResult* sql_ast)
{
    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtUpdate)
        return true;
    else if(t == kStmtDelete)
        return true;        
    else
        return false;
}


Expr* get_expr(SQLParserResult* sql_ast)
{
    if(!sql_ast)
        return NULL;

    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtSelect)
    {
        const SelectStatement* stmt = (const SelectStatement*)(sql_ast->getStatement(0));
        if(stmt)
        {
            return stmt->whereClause;
        }
    }
    else if(t == kStmtUpdate)
    {
        const UpdateStatement* stmt = (const UpdateStatement*)(sql_ast->getStatement(0));
        if(stmt)
        {
            return stmt->where;
        }
    }
    else if(t == kStmtDelete)
    {
        const DeleteStatement* stmt = (const DeleteStatement*)(sql_ast->getStatement(0));
        if(stmt)
        {
            return stmt->expr;
        }
    }
  
    return NULL;
}

bool traverse_input_sql(hsql::Expr* input, vector<hsql::Expr*> rules)
{
    bool left = false;
    bool right = false;

    if(!input)
        return false;

    if(!input->expr || !input->expr2)
        return false;

    if(input->opType >= kOpEquals && input->opType <= kOpGreaterEq)
    {
        for(int i = 0; i < rules.size(); i++)
        {
            if(do_match_expr(input, rules[i]))
            {
                return true;
            }
        }
    }

    if(input->expr->opType >= kOpEquals && input->expr->opType <= kOpGreaterEq)
    {
        for(int i = 0; i < rules.size(); i++)
        {
            if(do_match_expr(input->expr, rules[i]))
            {
                left = true;
                break;
            }
        }
    }
    else if(input->expr->opType == kOpAnd || input->expr->opType == kOpOr)
    {
        left = traverse_input_sql(input->expr, rules);
    }

    if(input->expr2->opType >= kOpEquals && input->expr2->opType <= kOpGreaterEq)
    {
        for(int i = 0; i < rules.size(); i++)
        {
            if(do_match_expr(input->expr2, rules[i]))
            {
                right = true;
                break;
            }
        }

    }
    else if(input->expr2->opType == kOpAnd || input->expr2->opType == kOpOr)
    {
        right = traverse_input_sql(input->expr2, rules);
    }

    if(input->opType == kOpAnd)
    {
        if(left || right)
            return true;
    }
    else if(input->opType == kOpOr)
    {
        if(left && right)
            return true;
    }

    return false;
}

int re_match_sql(hsql::SQLParserResult* sql_ast, vector<vector<hsql::Expr*> > expr_rules, hsql::SQLParserResult* ast)
{
    bool b_match = false;
    hsql::Expr* input_expr = NULL;
    int ret = -1;
    bool is_write = false;
    int statment_num = 0;

    log4cplus_debug("sql match start..");
    if(!sql_ast)
    {
        log4cplus_debug("sql_ast is null");
        ret = -1;
        goto RESULT;
    }
    statment_num = sql_ast->size();
    if(statment_num > 1)
    {
        ret = -2;
        goto RESULT;
    }

    if(is_write_type(sql_ast))
    {
        is_write = true;
        input_expr = get_expr(ast);            
    }
    else
    {
        input_expr = get_expr(sql_ast);
    }

    if(!input_expr)
    {
        if(is_write)
            return -100;
        ret = -1;
        goto RESULT;
    }

    log4cplus_debug("expr rule count: %d", expr_rules.size());
    for(int i = 0; i < expr_rules.size(); i++)
    {
        if(traverse_input_sql(input_expr, expr_rules[i]))
        {
            ret = 0;
            goto RESULT;
        }
    }

    if(is_write)
    {
        ret = -100;
        goto RESULT;
    }

    ret = -2;

RESULT:
    log4cplus_debug("sql match end: %d", ret);
    return ret;
}