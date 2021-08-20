
#include <stdio.h>
#include <dlfcn.h>
#include "container.h"
#include "version.h"
#include "dtcint.h"

typedef IInternalService *(*QueryInternalServiceFunctionType)(const char *name, const char *instance);

IInternalService *query_internal_service(const char *name, const char *instance)
{
	QueryInternalServiceFunctionType entry = NULL;
	entry = (QueryInternalServiceFunctionType)dlsym(RTLD_DEFAULT, "_QueryInternalService");
	if(entry == NULL)
		return NULL;
	return entry(name, instance);
}

static inline int fieldtype_keytype(int t)
{
	switch(t) {
		case DField::Signed:
		case DField::Unsigned:
			return DField::Signed;

		case DField::String:
		case DField::Binary:
			return DField::String;
	}
	return DField::None;
}

void NCServer::check_internal_service(void)
{
	if(NCResultInternal::verify_class()==0)
		return;

	IInternalService *inst = query_internal_service("dtcd", this->tablename_);

	/* not inside dtcd or tablename not found */
	if(inst == NULL)
		return;

	/* version mismatch, internal service is unusable */
	const char *version = inst->query_version_string();
	if(version==NULL || strcasecmp(version_detail, version) != 0)
		return;

	/* cast to DTC service */
	IDTCService *inst1 = static_cast<IDTCService *>(inst);

	DTCTableDefinition *tdef = inst1->query_table_definition();

	/* verify tablename etc */
	if(tdef->is_same_table(tablename_)==0)
		return;

	/* verify and save key type */
	int kt = fieldtype_keytype(tdef->key_type());
	if(kt == DField::None)
		/* bad key type */
		return;

	if(keytype_ == DField::None) {
		keytype_ = kt;
	} else if(keytype_ != kt) {
		badkey_ = 1;
		error_str_ = "Key Type Mismatch";
		return;
	}
	if(keyinfo_.get_key_fields_count()!=0) {
		/* FIXME: checking keyinfo */

		/* ZAP key info, use ordered name from server */
		keyinfo_.clear_key();
	}
	/* add NCKeyInfo */
	for(int i=0; i<tdef->key_fields(); i++) {
		kt = fieldtype_keytype(tdef->field_type(i));
		if(kt == DField::None)
			// bad key type
			return;
		keyinfo_.add_key(tdef->field_name(i), kt);
	}

    /**
     * OK, save it.
     * OK, save it.
     */
	this->table_definition_ = tdef;
	this->admin_tdef = inst1->query_admin_table_definition();
	/* useless here, internal DTCTableDefinition don't maintent a usage count */
	tdef->increase();
	this->service_ = inst1;
	this->completed_ = 1;
	if(get_address() && service_->match_listening_ports(get_address(), NULL)) {
		executor_ = service_->query_task_executor();
	}
}

