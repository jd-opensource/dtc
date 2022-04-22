#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <stdio.h>
#include <string.h>
#include <string>
#include <map>

class CAutoConfig {
	public:
		CAutoConfig() {};
		virtual ~CAutoConfig() {};
		virtual int GetIntVal(const char *key, const char *inst, int def=0) = 0;
		virtual unsigned long long GetSizeVal(const char *key, const char *inst, unsigned long long def=0, char unit=0) = 0;
		virtual int GetIdxVal(const char *, const char *, const char * const *, int=0) = 0;
		virtual const char *GetStrVal (const char *key, const char *inst) = 0;
};

class CConfig {
public:
	CConfig() {};
	~CConfig() {};

	CAutoConfig *GetAutoConfigInstance(const char *);
	int GetIntVal(const char *sec, const char *key, int def=0);
	unsigned long long GetSizeVal(const char *sec, const char *key, unsigned long long def=0, char unit=0);
	int GetIdxVal(const char *, const char *, const char * const *, int=0);
	const char *GetStrVal (const char *sec, const char *key);
	bool HasSection(const char *sec);
	bool HasKey(const char *sec, const char *key);
	void Dump (FILE *fp, bool dec=false);
	int Dump (const char *fn, bool dec=false);
	int ParseConfig(const char *f=0, const char *s=0,bool bakconfig = false);
	int ParseBufferedConfig(char *buf, const char *fn = 0, const char *s = 0, bool bakconfig = false);
	
private:
	struct nocase
	{
		bool operator()(const std::string &a, const std::string &b) const
		{ return strcasecmp(a.c_str(), b.c_str()) < 0; }
	};

	typedef std::map<std::string, std::string, nocase> keymap_t;
	typedef std::map<std::string, keymap_t, nocase> secmap_t;

private:
	std::string filename;
	secmap_t smap;
};

#endif
