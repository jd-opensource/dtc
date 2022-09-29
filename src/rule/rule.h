
#ifdef __cplusplus
extern "C"{
#endif

    int rule_sql_match(const char* szsql, const char* osql, const char* dbname, const char* conf);
    int re_load_table_key(char* key);
    int sql_parse_table(const char* szsql, char* out);
    int rule_get_key_type(const char* conf);
    const char* rule_get_key(const char* conf);
    bool is_show_db(const char* szsql);

#ifdef __cplusplus    
}
#endif