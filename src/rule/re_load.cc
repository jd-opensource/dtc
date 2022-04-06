#include "re_load.h"
#include "yaml-cpp/yaml.h"
#include "log.h"
#include "re_comm.h"

#define TABLE_CONF_NAME "/etc/dtc/table.yaml"
#define CACHE_CONF_NAME "/etc/dtc/dtc.yaml"
#define ALARM_CONF_FILE "/etc/dtc/dtcalarm.conf"

using namespace hsql;
hsql::SQLParserResult rule_ast;

vector<expr_properity> expr_rules;

// load rule from dtc.yaml
std::string do_get_rule()
{
    YAML::Node config;
    try {
        config = YAML::LoadFile(TABLE_CONF_NAME);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s\n", e.what());
		return "";
	}

    if(config["match"])
    {
        if(config["match"]["rule"])
        {
            std::string rules = config["match"]["rule"].as<string>();
            return rules;
        }
    }

    return "";
}

int get_rule_condition_num(hsql::Expr* rule)
{
    int num = 0;
    if(rule->isType(kExprOperator) && rule->opType == kOpAnd)
    {
        num += get_rule_condition_num(rule->expr);
        num += get_rule_condition_num(rule->expr2);
        return num;
    }
    else if(rule->isType(kExprOperator) && 
        (rule->opType >= kOpEquals && rule->opType <= kOpGreaterEq))
    {
        return 1;
    }

    return 0;
}

// parse rule txt to AST.
int do_parse_rule(std::string rules)
{
    std::string sql = "select * from rules where ";
    sql += rules;
    sql += ";";

    hsql::SQLParser::parse(sql, &rule_ast);
    if (rule_ast.isValid() && rule_ast.size() > 0)
    {
        return 0; 
    }

    return -1;
}

int traverse_ast(hsql::Expr* where)
{
    if(where->isType(kExprOperator) &&  where->opType == kOpOr)
    {
        traverse_ast(where->expr);
        traverse_ast(where->exprList->at(0));
    }
    else
    {
        expr_properity ep;
        ep.rule = where;
        ep.condition_num = get_rule_condition_num(where);
        expr_rules.push_back(ep);
    }

    return 0;
}

//legitimacy check.
int do_check_rule()
{
    hsql::Expr *where = NULL;

    if(rule_ast.size() != 1)
        return -1;

    if(rule_ast.getStatement(0)->type() != hsql::kStmtSelect)
        return -2;

    hsql::SelectStatement* stmt = (SelectStatement*)rule_ast.getStatement(0);

    where = stmt->whereClause;
    if(!where)
        return -3;

    traverse_ast(where);
    
    return 0;
}

// make `OR` top.
int do_optimize_rule()
{
    return 0;
}

// split whole rule to sub rules.
int do_split_rules()
{
    return 0;
}

int re_load_rule()
{
    std::string rules = do_get_rule();
    if(rules.length() <= 0)
        return -1;

    int ret = do_parse_rule(rules);
    if(ret != 0)
    {
        log4cplus_error("match rules parsed failed, %d", ret);
        return -2;
    }

    ret = do_check_rule();
    if(ret != 0)
    {
        log4cplus_error("match rules check failed, %d", ret);
        return -3;
    }

    return 0;
}