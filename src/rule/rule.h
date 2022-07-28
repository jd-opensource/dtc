
#ifdef __cplusplus
extern "C"{
#endif

    int rule_sql_match(const char* szsql, const char* szkey, const char* dbname);
    int re_load_table_key(char* key);

#ifdef __cplusplus    
}
#endif