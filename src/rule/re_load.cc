#include "re_load.h"
#include "yaml-cpp/yaml.h"
#include "log.h"
#include <string>
#include <iostream>
#include <fcntl.h>
#include "re_comm.h"

using namespace hsql;

vector<vector<hsql::Expr*> > expr_rules;

std::map<std::string, std::string> g_map_dtc_yaml;

// load rule from dtc.yaml
std::string do_get_rule(std::string buf)
{
    YAML::Node config;
    try {
        config = YAML::Load(buf);
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
int do_parse_rule(std::string rules, hsql::SQLParserResult* rule_ast)
{
    std::string sql = "select * from rules where ";
    sql += rules;
    sql += ";";
    log4cplus_debug("rule sql: %s", sql.c_str());
    bool r = hsql::SQLParser::parse(sql, rule_ast);
    if (r && rule_ast->isValid() && rule_ast->size() > 0)
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
        log4cplus_debug("type: %d, %d, %s", where->type, where->opType, where->expr->name);
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
int do_check_rule(hsql::SQLParserResult* rule_ast)
{
    hsql::Expr *where = NULL;
    if(rule_ast->size() != 1)
        return -1;

    if(rule_ast->getStatement(0)->type() != hsql::kStmtSelect)
        return -2;

    hsql::SelectStatement* stmt = (SelectStatement*)rule_ast->getStatement(0);

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

std::string load_dtc_yaml_buffer(int mid)
{
    char path[260];
    int i_length = 0;
    char* file = NULL;
    
    sprintf(path, "../conf/dtc-conf-%d.yaml", mid);

    int fd = -1;

	if ((fd = open(path, O_RDONLY)) < 0) 
    {
		log4cplus_error("open config file error");
		return "";
	}

	printf("open file:%s\n", path);
	lseek(fd, 0L, SEEK_SET);
	i_length = lseek(fd, 0L, SEEK_END);
	lseek(fd, 0L, SEEK_SET);
	// Attention: memory init here ,need release outside
	file = (char *)malloc(i_length + 1);
	int readlen = read(fd, file, i_length);
	if (readlen < 0 || readlen == 0)
		return "";
	file[i_length] = '\0';
	close(fd);
	i_length++; // add finish flag length
    std::string res = file;
    delete file;
    return res;
}

int re_load_rule(std::string buf, hsql::SQLParserResult* rule_ast)
{
    log4cplus_debug("load rule start...");

    if(rule_ast->isValid() && rule_ast->size() > 0)
        return 0;

    std::string rules = do_get_rule(buf);
    if(rules.length() <= 0)
        return -1;

    int ret = do_parse_rule(rules, rule_ast);
    if(ret != 0)
    {
        log4cplus_error("match rules parsed failed, %d", ret);
        return -2;
    }

    ret = do_check_rule(rule_ast);
    if(ret != 0)
    {
        log4cplus_error("match rules check failed, %d", ret);
        return -3;
    }

    log4cplus_debug("load rule end.");
    return 0;
}
