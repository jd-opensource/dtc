#ifndef __SPLIT_TOOL_H__
#define __SPLIT_TOOL_H__
#include <set>
#include <string>
#include <vector>
#include <map>
#include <stdint.h>
#include "utf8_str.h"
#include "trainCorpus.h"
//#include "dtcapi.h"
using namespace std;

typedef struct
{
	string szIpadrr;
	unsigned uBid;
	unsigned uPort;
	unsigned uWeight;
	unsigned uStatus;
}SDTCroute;

typedef struct
{
	string szTablename;
	string szAccesskey;
	unsigned  uTimeout;
	unsigned  uKeytype;
	std::vector<SDTCroute> vecRoute;
}SDTCHost;

struct WordInfo {
	WordInfo() {
		word_id = 0;
		word_freq = 0;
		appid = 0;
	}
	uint32_t word_id;
	uint32_t word_freq;
	uint32_t appid;
};

struct RouteValue {
	double max_route;
	uint32_t idx;
	RouteValue() {
		max_route = 0;
		idx = 0;
	}
};

class FBSegment {
public:
	FBSegment();
	~FBSegment(){}
	bool Init();
	bool Init2(string train_path);
	bool Init3(string train_path, string word_path);

	vector<string> segment(iutf8string &phrase, uint32_t appid);
	void segment2(iutf8string &phrase, uint32_t appid, vector<string> &vec, string mode = "PrePostNGram", bool Hmm_flag = false);
	void cut_for_search(iutf8string &phrase, uint32_t appid, vector<vector<string> > &search_res_all, string mode = "PrePostNGram");
	void cut_ngram(iutf8string &phrase, vector<string> &search_res, uint32_t n);
	bool WordValid(string word, uint32_t appid);
	bool GetWordInfo(string word, uint32_t appid, WordInfo &word_info);
	bool GetWordInfoFromDictOnly(string word, uint32_t appid, WordInfo &word_info);

private:
	void __cut_DAG_NO_HMM(string senstece, uint32_t appid, vector<string> &vec);
	void get_DAG(string sentence, uint32_t appid, map<uint32_t, vector<uint32_t> > &DAG);
	void calc(string sentence, const map<uint32_t, vector<uint32_t> > &DAG, map<uint32_t, RouteValue> &route, uint32_t appid);
	vector<char> viterbi(string sentence);
	vector<string> HMM_split(string sentence);
	double CalSegProbability(const vector<string> &vec);
	void FMM2(iutf8string  &phrase, uint32_t appid, vector<string> &vec);
	void BMM2(iutf8string  &phrase, uint32_t appid, vector<string> &vec);
	void BMM(iutf8string  &phrase, uint32_t appid, vector<string> &vec);
	vector<string> segment_part(iutf8string &phrase, uint32_t appid);
	bool isAlphaOrDigit(string str);

	set<string> common_dict;
	map<uint32_t, set<string> > custom_dict;
	set<string> punct_set;
	set<string> alpha_set;
	map<string, map<string, int> > next_dict;
	uint32_t train_cnt;
	TrainCorpus train_corpus;
	map<string, map<uint32_t, WordInfo> > word_dict;
};
#endif