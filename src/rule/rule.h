
#ifdef __cplusplus
extern "C"{
#endif

    int rule_sql_match(const char* szsql, const char* osql, const char* dbname, const char* conf);
    int re_load_table_key(char* key);
    int sql_parse_table(const char* szsql, char* out);
    int rule_get_key_type(const char* conf);
    int rule_get_key(const char* conf, char* out);
    int get_table_with_db(const char* sessiondb, const char* sql, char* result);    

#ifdef __cplusplus    
}
#endif

bool exist_session_db(const char* dbname);