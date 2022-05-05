#include "re_match.h"
#include "re_comm.h"
#include "log.h"

using namespace hsql;
using namespace std;
hsql::SQLParserResult sql_ast;

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
        }
    }
    else if(rule_type == kOpLessEq)
    {
        if(input->isType(kExprLiteralFloat))
        {
            if(input_type == kOpLess || input_type == kOpLessEq)
            {
                if(input->fval <= rule->fval)
                    return true;
                else
                    return false;
            }
        }
        else if(input->isType(kExprLiteralInt))
        {
            if(input_type == kOpLess || input_type == kOpLessEq)
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
    }

    return false;
}


bool do_match_expr(hsql::Expr* input, hsql::Expr* rule)
{
    if(!input->isType(kExprOperator) || !rule->isType(kExprOperator))
        return false;

    if(input->opType != rule->opType && (input->opType < kOpLess || input->opType > kOpGreaterEq))
        return false;

    if(strcasecmp(input->expr->getName(), rule->expr->getName()) != 0)
        return false;

    return cmp_expr_value(input->expr2, rule->expr2, input->opType, rule->opType);
}

bool is_insert_type(SQLParserResult* sql_ast)
{
    StatementType t = sql_ast->getStatement(0)->type();
    if(t == kStmtInsert)
        return true;
    else
        return false;
}


Expr* get_expr(SQLParserResult* sql_ast)
{
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

int re_match_sql(hsql::SQLParserResult* sql_ast, vector<vector<hsql::Expr*> > expr_rules)
{
    bool b_match = false;
    hsql::Expr* input_expr = NULL;
    int ret = -1;
    int statment_num = 0;

    log4cplus_debug("sql match start..");
    if(!sql_ast)
    {
        ret = -1;
        goto RESULT;
    }

    statment_num = sql_ast->size();
    if(statment_num > 1)
    {
        ret = -2;
        goto RESULT;
    }

    if(is_insert_type(sql_ast))
    {
        ret = 0;
        goto RESULT;
    }

    input_expr = get_expr(sql_ast);
    if(!input_expr)
    {
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
                log4cplus_debug("1111111111, %d", ep.condition_num);
                if(ep.condition_num == 1)
                {
                    log4cplus_debug("2222222222");
                    if(do_match_expr(input_expr->expr, ep.rule) || do_match_expr(input_expr->expr2, ep.rule))
                    {
                        ret = 0;
                        goto RESULT;
                    }
                }
                else if(ep.condition_num > 1)
                {
                    log4cplus_debug("3333333333333");
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
    log4cplus_debug("sql match end.");
    return ret;
}