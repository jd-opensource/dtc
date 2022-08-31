#include "re_load.h"
#include "yaml-cpp/yaml.h"
#include "log.h"
#include <string>
#include <iostream>
#include "re_comm.h"

std::string conf_file = "/etc/dtc/dtc.yaml";

using namespace hsql;
hsql::SQLParserResult rule_ast;

vector<vector<hsql::Expr*> > expr_rules;

// load rule from dtc.yaml
std::string do_get_rule()
{
    YAML::Node config;
    try {
        config = YAML::LoadFile(conf_file);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s\n", e.what());
		return "";
	}

    YAML::Node node = config["primary"]["layered.rule"];
    if(node)
    {
        std::string rules = node.as<string>();
        return rules;
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
    log4cplus_debug("rule sql: %s", sql.c_str());
    hsql::SQLParser::parse(sql, &rule_ast);
    if (rule_ast.isValid() && rule_ast.size() > 0)
    {
        return 0; 
    }

    return -1;
}

int traverse_sub_ast(hsql::Expr* where, vector<hsql::Expr*>* v_expr)
{
    if(where->isType(kExprOperator) &&  where->opType == kOpAnd)
    {
        traverse_sub_ast(where->expr, v_expr);
        traverse_sub_ast(where->expr2, v_expr);
    }
    else
    {
        v_expr->push_back(where);
    }
}

int traverse_ast(hsql::Expr* where)
{
    if(where->isType(kExprOperator) &&  where->opType == kOpOr)
    {
        traverse_ast(where->expr);
        traverse_ast(where->expr2);
    }
    else
    {
        vector<hsql::Expr*> v_expr;
        traverse_sub_ast(where, &v_expr);

        expr_rules.push_back(v_expr);
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
    log4cplus_debug("load rule start...");

    if(rule_ast.isValid() && rule_ast.size() > 0)
        return 0;

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

    log4cplus_debug("load rule end.");
    return 0;
}

extern "C" int re_load_table_key(char* key)
{
    YAML::Node config;
    try {
        config = YAML::LoadFile(conf_file);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s\n", e.what());
		return -1;
	}

    YAML::Node node = config["primary"]["cache"]["field"][0]["name"];
    if(node)
    {
        if(node.as<string>().length() >= 50)
        {
            return -1;
        }
        strcpy(key, node.as<string>().c_str());
        return 0;
    }

    return -1;
}

std::string re_load_table_name()
{
    YAML::Node config;
    try {
        config = YAML::LoadFile(conf_file);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s\n", e.what());
		return "";
	}

    YAML::Node node = config["primary"]["table"];
    if(node)
    {
        if(node.as<string>().length() >= 50)
        {
            return "";
        }
        std::string res = node.as<string>();
        transform(res.begin(),res.end(),res.begin(),::toupper);
        return res;
    }

    return "";
}