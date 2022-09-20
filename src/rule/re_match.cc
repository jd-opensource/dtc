#include "re_match.h"
#include "re_comm.h"
#include "log.h"
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
    hsql::SQLParser::parse(sql, sql_ast);
    if (!sql_ast->isValid() || sql_ast->size() <= 0)
    {
        log4cplus_debug("valid:%d, size:%d, %s", sql_ast->isValid(), sql_ast->size(), sql.c_str());
        return -1; 
    }
    log4cplus_debug("input sql parse end");
    return 0;
}

bool cmp_expr_value(hsql::Expr* input, hsql::Expr* rule, OperatorType input_type, OperatorType rule_type)
{
    if(input->type != rule->type)
        return false;

    if(input_type >= kOpLess && input_type <= kOpGreaterEq && input->isType(kExprLiteralString))
        return false;

    if(input_type == kOpEquals && rule_type == kOpEquals)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input->fval == rule->fval)
                return true;
            else
                return false;
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input->ival == rule->ival)
                return true;
            else
                return false;
        }
        else if(input->isType(kExprLiteralString))
        {
            if(strcasecmp(input->name, rule->name) == 0)
                return true;
            else
                return false;
        }
    }
    else if(input_type == kOpNotEquals && rule_type == kOpNotEquals)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input->fval != rule->fval)
                return true;
            else
                return false;
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input->ival != rule->ival)
                return true;
            else
                return false;
        }
        else if(input->isType(kExprLiteralString))
        {
            if(strcasecmp(input->name, rule->name) == 0)
                return false;
            else
                return true;
        }
    }
    else if(rule_type == kOpLess)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input_type == kOpLess)
            {
                if(input->fval <= rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpLessEq)
            {
                if(input->fval < rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(input->fval < rule->fval)
                    return true;
                else
                    return false;
            }            
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input_type == kOpLess)
            {
                if(input->ival <= rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpLessEq)
            {
                if(input->ival < rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(input->ival < rule->ival)
                    return true;
                else
                    return false;
            }            
        }
    }
    else if(rule_type == kOpLessEq)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input_type == kOpLess || input_type == kOpLessEq || input_type == kOpEquals)
            {
                if(input->fval <= rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input_type == kOpLess || input_type == kOpLessEq || input_type == kOpEquals)
            {
                if(input->ival <= rule->ival)
                    return true;
                else
                    return false;
            }
        }
    }
    else if(rule_type == kOpGreater)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input_type == kOpGreater)
            {
                if(input->fval >= rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpGreaterEq)
            {
                if(input->fval > rule->fval)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(input->fval > rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input_type == kOpGreater)
            {
                if(input->ival >= rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpGreaterEq)
            {
                if(input->ival > rule->ival)
                    return true;
                else
                    return false;
            }
            else if(input_type == kOpEquals)
            {
                if(input->ival > rule->ival)
                    return true;
                else
                    return false;
            }
        }
    }
    else if(rule_type == kOpGreaterEq)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input_type == kOpGreater || input_type == kOpGreaterEq)
            {
                if(input->fval >= rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input_type == kOpGreater || input_type == kOpGreaterEq)
            {
                if(input->ival >= rule->ival)
                    return true;
                else
                    return false;
            }
        }
        else if(input_type == kOpEquals)
        {
            if(input->ival >= rule->ival)
                return true;
            else
                return false;
        }
    }

    return false;
}


bool do_match_expr(hsql::Expr* input, hsql::Expr* rule)
{
    log4cplus_debug("bbbbbbbbbbb");
    if(!input->isType(kExprOperator) || !rule->isType(kExprOperator))
        return false;
log4cplus_debug("aaaaaaaaaaaa:%d %d", input->opType, rule->opType);
    if(input->opType != rule->opType && (input->opType < kOpEquals || input->opType > kOpGreaterEq))
        return false;

    log4cplus_debug("do_MATCH_EXPR:%s %s", input->expr->getName(), rule->expr->getName());
    if(strcasecmp(input->expr->getName(), rule->expr->getName()) != 0)
        return false;

    return cmp_expr_value(input->expr2, rule->expr2, input->opType, rule->opType);
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
log4cplus_debug("111111111111");
    if(!input)
        return false;
log4cplus_debug("22222222222");
    if(!input->expr || !input->expr2)
        return false;
log4cplus_debug("3333333333333: %d", input->opType);
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
    log4cplus_debug("444444444444");
    if(input->expr->opType >= kOpEquals && input->expr->opType <= kOpGreaterEq)
    {
        log4cplus_debug("5555555555");
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
        log4cplus_debug("66666666666666");
        left = traverse_input_sql(input->expr, rules);
    }

    if(input->expr2->opType >= kOpEquals && input->expr2->opType <= kOpGreaterEq)
    {
        log4cplus_debug("777777777777");
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
        log4cplus_debug("8888888888888");
        right = traverse_input_sql(input->expr2, rules);
    }
log4cplus_debug("99999999999999");
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
log4cplus_debug("000000000000000000");
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

log4cplus_debug("3333333333");
    if(is_write_type(sql_ast))
    {
        log4cplus_debug("33333333");
        is_write = true;
        input_expr = get_expr(ast);            
    }
    else
    {
        input_expr = get_expr(sql_ast);
    }
log4cplus_debug("444444444");
    if(!input_expr)
    {
        if(is_write)
            return -100;
        log4cplus_debug("11111111111");
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

#if 0 
    for(int i = 0; i < expr_rules.size(); i++)
    {
        expr_properity ep = expr_rules[i];

        hsql::Expr* rule = ep.rule;

        if(ep.condition_num == 1 && input_expr->opType >=kOpEquals && input_expr->opType <= kOpGreaterEq)
        {
            if(do_match_expr(input_expr, ep.rule))
            {
                ret = 0;
                goto RESULT;
            }
        }
        else
        {
            if(input_expr->isType(kExprOperator) && input_expr->opType == kOpAnd)
            {
                if(ep.condition_num == 1)
                {
                    if(do_match_expr(input_expr->expr, ep.rule) || do_match_expr(input_expr->expr2, ep.rule))
                    {
                        ret = 0;
                        goto RESULT;
                    }
                }
                else if(ep.condition_num > 1)
                {
                    if(ep.rule->isType(kExprOperator) && ep.rule->opType == kOpAnd)
                    {
                        if((do_match_expr(input_expr->expr, ep.rule->expr) && do_match_expr(input_expr->expr2, ep.rule->expr2)) ||
                        (do_match_expr(input_expr->expr, ep.rule->expr2) && do_match_expr(input_expr->expr2, ep.rule->expr)))
                        {
                            ret = 0;
                            goto RESULT;
                        }
                    }
                }
                else
                {
                    continue;
                }
            }
            else if(input_expr->isType(kExprOperator) && input_expr->opType == kOpOr)
            {
                if((do_match_expr(input_expr->expr, ep.rule->expr) && do_match_expr(input_expr->expr2, ep.rule->expr2)) ||
                        (do_match_expr(input_expr->expr, ep.rule->expr2) && do_match_expr(input_expr->expr2, ep.rule->expr)))
                    {
                        ret = 0;
                        goto RESULT;
                    }
            }
        }

        
    }
#endif

    ret = -2;

RESULT:
    log4cplus_debug("sql match end: %d", ret);
    return ret;
}