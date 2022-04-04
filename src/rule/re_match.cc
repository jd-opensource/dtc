#include "re_match.h"
#include "../libs/hsql/include/SQLParser.h"
#include "../libs/hsql/include/util/sqlhelper.h"
#include "re_comm.h"

using namespace hsql;
using namespace std;
hsql::SQLParserResult sql_ast;

//legitimacy check.
int do_check_sql()
{
    return 0;
}

// parse sql to AST.
int do_parse_sql(std::string sql)
{
    hsql::SQLParser::parse(sql, &sql_ast);
    if (!sql_ast.isValid() || sql_ast.size() <= 0)
    {
        return -1; 
    }

    return 0;
}

bool do_match_expr(hsql::Expr* input, hsql::Expr* rule)
{
    if(!input->isType(kExprOperator) || !rule->isType(kExprOperator))
        return false;

    if(input->opType != rule->opType)
        return false;

    if(input->expr->getName() != rule->expr->getName())
        return false;

    if(input->opType == kOpEquals || input->opType == kOpNotEquals)
    {
        if(input->expr2 == rule->expr2)
            return true;
    }
    else if(input->opType == kOpLess || input->opType == kOpLessEq)
    {
        if(input->expr2 <= rule->expr2)
            return true;
    }
    else if(input->opType == kOpGreater || input->opType == kOpGreaterEq)
    {
        if(input->expr2 >= rule->expr2)
            return true;
    }

    return false;
}

int re_match_sql(std::string sql, vector<expr_properity> expr_rules)
{
    if(do_parse_sql(sql) != 0)
        return -1;

    hsql::Expr* expr_sql = NULL;
    int expr_sql_condition_num = 0;
    bool b_match = false;

    for(int i = 0; i < expr_rules.size(); i++)
    {
        expr_properity ep = expr_rules[i];

        hsql::Expr* rule = ep.rule;

        if(expr_sql->isType(kExprOperator) && expr_sql->opType == kOpAnd)
        {
            if(ep.condition_num == 1)
            {
                if(do_match_expr(expr_sql->expr, ep.rule) || do_match_expr(expr_sql->expr2, ep.rule))
                    return 0;
            }
            else if(ep.condition_num > 1)
            {
                if(ep.rule->isType(kExprOperator) && ep.rule->opType == kOpAnd)
                {
                    if((do_match_expr(expr_sql->expr, ep.rule->expr) && do_match_expr(expr_sql->expr2, ep.rule->expr2)) ||
                    (do_match_expr(expr_sql->expr, ep.rule->expr2) && do_match_expr(expr_sql->expr2, ep.rule->expr)))
                        return 0;
                }
            }
            else
            {
                continue;
            }
        }
        else if(expr_sql->isType(kExprOperator) && expr_sql->opType == kOpOr)
        {
             if((do_match_expr(expr_sql->expr, ep.rule->expr) && do_match_expr(expr_sql->expr2, ep.rule->expr2)) ||
                    (do_match_expr(expr_sql->expr, ep.rule->expr2) && do_match_expr(expr_sql->expr2, ep.rule->expr)))
                return 0;
        }
        
    }

    return -2;
}