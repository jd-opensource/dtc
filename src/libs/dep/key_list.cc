#include <errno.h>

#include "mem_check.h"
#include "protocol.h"
#include "key_list.h"
#include "dtc_error_code.h"

int NCKeyValueList::key_value_max_ = 32;

NCKeyInfo::~NCKeyInfo()
{
	for(int i = 0; i < 8; ++i) {
		if(key_name_[i]) {
			free(key_name_[i]);
			key_name_[i] = NULL;
		}
	}
}

void NCKeyInfo::clear_key() 
{
	key_count_ = 0;
	memset(key_type_, 0, sizeof(key_type_));
	memset(key_name_, 0, sizeof(key_name_));
	key_map_.clear();
}
int NCKeyInfo::get_key_index(const char *n) const
{
	namemap_t::const_iterator i = key_map_.find(n);
	return i == key_map_.end() ? -1 : i->second;
}

int NCKeyInfo::add_key(const char* name, int type)
{
	switch(type) {
		case DField::Signed:
		case DField::String:
			break;
		default:
			return -EC_BAD_KEY_TYPE;
	}
	if(get_key_index(name) >= 0) {
		/**
		 * return -EC_DUPLICATE_FIELD;
		 * ignore duplicate key field name
		 * because NCKeyInfo may be initialized by check_internal_service()
		 * add again must be allowed for code compatibility
		 */
		return 0;
	}
	int cnt = get_key_fields_count();
	if(cnt >= (int)(sizeof(key_type_)/sizeof(key_type_[0]))) return -EC_BAD_MULTIKEY;
	char* localName = (char*)malloc(strlen(name)+1);
	strcpy(localName, name);
	key_name_[cnt] = localName;
	key_map_[localName] = cnt;
	key_type_[cnt] = type;
	key_count_++;
	return 0;
}

int NCKeyInfo::equal_key_name(const NCKeyInfo &other) const 
{
	int n = get_key_fields_count();
	/* key field count mis-match */
	if(other.get_key_fields_count() != n)
		return 0;
	/* key type mis-match */
	if(memcmp(key_type_, other.key_type_, n)!=0)
		return 0;
	for(int i=0; i<n; i++) {
		/* key name mis-match */
		if(strcasecmp(key_name_[i], other.key_name_[i]) != 0)
			return 0;
	}
	return 1;
}

void NCKeyValueList::unset_key(void) 
{
	if(key_count_) {
		const int kn = get_key_fields_count();
		for(int i=0; i<key_count_; i++) {
			for(int j=0; j<kn; j++)
				if(get_key_type(j) == DField::String || get_key_type(j) == DField::Binary)
					FREE(Value(i, j).bin.ptr);
	    }
		FREE_CLEAR(val);
		memset(fcount, 0, sizeof(fcount));
		key_count_ = 0;
	}
}

int NCKeyValueList::add_value(const char* name, const DTCValue &v, int type)
{
	const int kn = get_key_fields_count();

	int col = keyinfo_->get_key_index(name);
	if(col < 0 || col >= kn)
		return -EC_BAD_KEY_NAME;

	switch(get_key_type(col)) {
		case DField::Signed:
		case DField::Unsigned:
			if(type != DField::Signed && type != DField::Unsigned)
				return -EC_BAD_VALUE_TYPE;
			break;
		case DField::String:
		case DField::Binary:
			if(type != DField::String && type != DField::Binary)
				return -EC_BAD_VALUE_TYPE;
			if(v.bin.len > 255)
				return -EC_KEY_OVERFLOW;
			break;
		default:
			return -EC_BAD_KEY_TYPE;
	}

	int row = fcount[col];
	if(row >= key_value_max_)
	    /* Too many key values */
		return -EC_TOO_MANY_KEY_VALUE; 
	if(row >= key_count_) {
		if(REALLOC(val, (key_count_+1)*kn*sizeof(DTCValue)) == NULL)
			throw std::bad_alloc();
		memset(&Value(row, 0), 0, kn*sizeof(DTCValue));
		key_count_++;
	}
	DTCValue &slot = Value(row, col);
	switch(get_key_type(col)) {
		case DField::Signed:
		case DField::Unsigned:
			slot = v;
			break;
		
		case DField::String:
		case DField::Binary:
			slot.bin.len = v.bin.len;
			slot.bin.ptr =  (char *)MALLOC(v.bin.len+1);
			if(slot.bin.ptr==NULL)
				throw std::bad_alloc();
			memcpy(slot.bin.ptr, v.bin.ptr, v.bin.len);
			slot.bin.ptr[v.bin.len] = '\0';
			break;
	}
	fcount[col]++;
	return 0;
}

