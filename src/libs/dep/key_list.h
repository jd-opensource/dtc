#ifndef __CHC_KEYLIST_H
#define __CHC_KEYLIST_H

#include <string.h>
#include <map>
#include <value.h>

class NCKeyInfo 
{
private:
    struct nocase
	{
		bool operator()(const char * const & a, const char * const & b) const
		{
			return strcasecmp(a, b) < 0; 
			}
	};
	typedef std::map<const char *, int, nocase> namemap_t;
private:
	namemap_t key_map_;
	char *key_name_[8];
	uint8_t key_type_[8];
	int key_count_;
public:
	NCKeyInfo() 
	{
		key_count_ = 0;
		memset(key_type_, 0, sizeof(key_type_));
		memset(key_name_, 0, sizeof(key_name_));
	}
	NCKeyInfo(const NCKeyInfo&that) 
	{
		key_count_ = that.key_count_;
		memcpy(key_type_, that.key_type_, sizeof(key_type_));
		memcpy(key_name_, that.key_name_, sizeof(key_name_));
		for(int i=0; i<key_count_; i++)
			key_map_[key_name_[i]] = i;
	}
	/* modified by neolv to fix char* name */
	~NCKeyInfo();

	/* zero is KeyField::None */
	void clear_key();
	int add_key(const char *name, int type);
	int equal_key_name(const NCKeyInfo &other) const;
	int get_key_index(const char *n) const;
	const char *get_key_name(int n) const { return key_name_[n]; }
	int get_key_type(int id) const { return key_type_[id]; }
	int get_key_fields_count(void) const { return key_count_; }
};

class NCKeyValueList 
{
public:
	static int key_value_max_;
	NCKeyInfo *keyinfo_;
	DTCValue *val;
	int fcount[8];
	int key_count_;
public:
	NCKeyValueList() : keyinfo_(NULL), val(NULL), key_count_(0) 
	{
		memset(fcount, 0, sizeof(fcount));
	}
	~NCKeyValueList() 
	{
		FREE_CLEAR(val);
	}
	int get_key_fields_count() const { return keyinfo_->get_key_fields_count(); }
	int get_key_type(int id) const { return keyinfo_->get_key_type(id); }
	int KeyCount() const { return key_count_; }
	const char * get_key_name(int id) const { return keyinfo_->get_key_name(id); }

	void unset_key();
	int add_value(const char *, const DTCValue &, int);
	DTCValue &Value(int row, int col) { return val[row*keyinfo_->get_key_fields_count()+col]; }
	const DTCValue &Value(int row, int col) const { return val[row*keyinfo_->get_key_fields_count()+col]; }
	DTCValue & operator()(int row, int col) { return Value(row, col); }
	const DTCValue & operator()(int row, int col) const { return Value(row, col); }
	int is_key_flat() const 
	{
		for(int i=1; i<keyinfo_->get_key_fields_count(); i++)
			if(fcount[0] != fcount[i])
				return 0;
		return 1;
	}
};

#endif

