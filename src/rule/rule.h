
#ifdef __cplusplus
extern "C"{
#endif

    int rule_sql_match(const char* szsql, const char* osql, const char* dbsession, char* out_dtckey, int* out_keytype);
    int sql_parse_table(const char* szsql, char* out);
    int get_table_with_db(const char* sessiondb, const char* sql, char* result);    
    int re_load_all_rules();
#ifdef __cplusplus    
}
#endif

bool exist_session_db(const char* dbname);