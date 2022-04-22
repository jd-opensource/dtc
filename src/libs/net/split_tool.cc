#include "split_tool.h"
#include <fstream>
#include <vector>
#include <stdio.h>
#include <stdint.h>
#include <math.h>
#include <stdlib.h>
#include <float.h>
#include "log.h"
using namespace std;
#define MAX_WORD_LEN 8
#define TOTAL 8000000
#define ALPHA_DIGIT "0123456789１２３４５６７８９０\
abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZａｂｃｄｅｆｇｈｉｇｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＵＶＷＸＹＺ"


bool isAllAlphaOrDigit(string str) {
	bool flag = true;
	size_t i = 0;
	for (; i < str.size(); i++) {
		if (!isupper(str[i]) && !islower(str[i]) && !isdigit(str[i])) {
			flag = false;
			break;
		}
	}
	return flag;
}

FBSegment::FBSegment() {
	punct_set.clear();
	train_cnt = 0;
}

bool FBSegment::isAlphaOrDigit(string str) {
	if (alpha_set.find(str) != alpha_set.end())
	{
		return true;
	}
	return false;
}


bool FBSegment::Init() {

	string en_punct = ",.!?/'\"<>\\:;\n";
	string punct = "，。！？、；：“”‘’（）《》 ";
	punct = punct.append(en_punct);
	iutf8string utf8_punct(punct);
	for (int i = 0; i < utf8_punct.length(); i++) {
		punct_set.insert(utf8_punct[i]);
	}

	string alphadigit = ALPHA_DIGIT;
	iutf8string utf8_alpha(alphadigit);
	for (int i = 0; i < utf8_alpha.length(); i++) {
		alpha_set.insert(utf8_alpha[i]);
	}

	return true;
}

bool FBSegment::Init2(string train_path) {
	bool ret = Init();
	if (ret == false) {
		return ret;
	}

	string str;
	ifstream train_infile;
	train_infile.open(train_path.c_str());
	if (train_infile.is_open() == false) {
		log_error("open file error: %s.\n", train_path.c_str());
		return false;
	}
	string beg_tag = "<BEG>";
	string end_tag = "<END>";
	while (getline(train_infile, str))
	{
		vector<string> str_vec = splitEx(str, " ");
		vector<string> line_list;
		vector<string>::iterator iter = str_vec.begin();
		for (; iter != str_vec.end(); iter++) {
			if (punct_set.find(*iter) == punct_set.end() && *iter != "") {
				line_list.push_back(*iter);
			}
		}
		train_cnt += line_list.size();
		for (int i = -1; i < (int)line_list.size(); i++) {
			string word1;
			string word2;
			if (i == -1) {
				word1 = beg_tag;
				word2 = line_list[i + 1];
			}
			else if (i == (int)line_list.size() - 1) {
				word1 = line_list[i];
				word2 = end_tag;
			}
			else {
				word1 = line_list[i];
				word2 = line_list[i + 1];
			}
			if (next_dict.find(word1) == next_dict.end()) {
				map<string, int> dict;
				next_dict[word1] = dict;
			}
			if (next_dict[word1].find(word2) == next_dict[word1].end()) {
				next_dict[word1][word2] = 1;
			}
			else {
				next_dict[word1][word2] += 1;
			}
		}

	}
	train_infile.close();

	ret = train_corpus.Init(train_path);
	if (ret == false) {
		log_error("train_corpus init error.");
		return ret;
	}
	log_info("total training words length is: %u, next_dict count: %d.", train_cnt, (int)next_dict.size());
	
	return true;
}

bool FBSegment::Init3(string train_path, string word_path) {
	bool ret = Init2(train_path);
	if (ret == false) {
		return ret;
	}

	string str;
	ifstream word_infile;
	word_infile.open(word_path.c_str());
	if (word_infile.is_open() == false) {
		log_error("open file error: %s.\n", word_path.c_str());
		return false;
	}

	uint32_t word_id = 0;
	uint32_t appid = 0;
	string word;
	uint32_t word_freq = 0;
	while (getline(word_infile, str))
	{
		vector<string> str_vec = splitEx(str, "\t");
		word_id = atoi(str_vec[0].c_str());
		word = str_vec[1];
		appid = atoi(str_vec[2].c_str());
		word_freq = atoi(str_vec[3].c_str());
		WordInfo word_info;
		word_info.appid = appid;
		word_info.word_freq = word_freq;
		word_info.word_id = word_id;
		word_dict[word][appid] = word_info;
	}
	log_info("word_dict count: %d", (int)word_dict.size());

	return true;
}

bool FBSegment::WordValid(string word, uint32_t appid) {
	if (word_dict.find(word) != word_dict.end()) {
		map<uint32_t, WordInfo> wordInfo = word_dict[word];
		if (wordInfo.find(0) != wordInfo.end() || wordInfo.find(appid) != wordInfo.end()) {
			return true;
		}
	}

	return false;
}

bool FBSegment::GetWordInfo(string word, uint32_t appid, WordInfo &word_info) {
	if (word_dict.find(word) != word_dict.end()) {
		map<uint32_t, WordInfo> wordInfo = word_dict[word];
		if (wordInfo.find(0) != wordInfo.end()) {
			word_info = wordInfo[0];
			return true;
		}
		if (wordInfo.find(appid) != wordInfo.end()) {
			word_info = wordInfo[appid];
			return true;
		}
	}

	return false;
}

bool FBSegment::GetWordInfoFromDictOnly(string word, uint32_t appid, WordInfo &word_info) {
	if (word_dict.find(word) != word_dict.end()) {
		map<uint32_t, WordInfo> wordInfo = word_dict[word];
		if (wordInfo.find(0) != wordInfo.end()) {
			word_info = wordInfo[0];
			return true;
		}
		if (wordInfo.find(appid) != wordInfo.end()) {
			word_info = wordInfo[appid];
			return true;
		}
	}
	return false;
}

void FBSegment::FMM2(iutf8string  &phrase, uint32_t appid, vector<string> &fmm_list) {
	int maxlen = MAX_WORD_LEN;
	int len_phrase = phrase.length();
	int i = 0, j = 0;

	while (i < len_phrase) {
		int end = i + maxlen;
		if (end >= len_phrase)
			end = len_phrase;
		iutf8string phrase_sub = phrase.utf8substr(i, end - i);
		for (j = phrase_sub.length(); j >= 0; j--) {
			if (j == 1)
				break;
			iutf8string key = phrase_sub.utf8substr(0, j);
			if (WordValid(key.stlstring(), appid) == true) {
				fmm_list.push_back(key.stlstring());
				i += key.length() - 1;
				break;
			}
		}
		if (j == 1) {
			fmm_list.push_back(phrase_sub[0]);
		}
		i += 1;
	}
	return;
}

// not query from DTC
void FBSegment::BMM(iutf8string  &phrase, uint32_t appid, vector<string> &bmm_list) {
	int maxlen = MAX_WORD_LEN - 2;
	int len_phrase = phrase.length();
	int i = len_phrase, j = 0;

	while (i > 0) {
		int start = i - maxlen;
		if (start < 0)
			start = 0;
		iutf8string phrase_sub = phrase.utf8substr(start, i - start);
		for (j = 0; j < phrase_sub.length(); j++) {
			if (j == phrase_sub.length() - 1)
				break;
			iutf8string key = phrase_sub.utf8substr(j, phrase_sub.length() - j);
			string word = key.stlstring();
			if (WordValid(word, appid) == true) {
				vector<string>::iterator iter = bmm_list.begin();
				bmm_list.insert(iter, key.stlstring());
				i -= key.length() - 1;
				break;
			}
		}
		if (j == phrase_sub.length() - 1) {
			vector<string>::iterator iter = bmm_list.begin();
			bmm_list.insert(iter, "" + phrase_sub[j]);
		}
		i -= 1;
	}
	return;
}

void FBSegment::BMM2(iutf8string  &phrase, uint32_t appid, vector<string> &bmm_list) {
	 int maxlen = MAX_WORD_LEN;
	 int len_phrase = phrase.length();
	 int i = len_phrase, j = 0;

	 while (i > 0) {
		 int start = i - maxlen;
		 if (start < 0)
			 start = 0;
		 iutf8string phrase_sub = phrase.utf8substr(start, i-start);
		 for (j = 0; j < phrase_sub.length(); j++) {
			 if (j == phrase_sub.length() - 1)
				 break;
			 iutf8string key = phrase_sub.utf8substr(j, phrase_sub.length()-j);
			 if (WordValid(key.stlstring(), appid) == true) {
				 vector<string>::iterator iter = bmm_list.begin();
				 bmm_list.insert(iter, key.stlstring());
				 i -= key.length() - 1;
				 break;
			 }
		 }
		 if (j == phrase_sub.length() - 1) {
			 vector<string>::iterator iter = bmm_list.begin();
			 bmm_list.insert(iter, "" + phrase_sub[j]);
		 }
		 i -= 1;
	 }
	 return;
 }

 vector<string> FBSegment::segment_part(iutf8string &phrase, uint32_t appid) {
	 vector<string> fmm_list;
	 FMM2(phrase, appid, fmm_list); // 正向最大匹配
	 vector<string> bmm_list;
	 BMM2(phrase, appid, bmm_list); // 反向最大匹配
	 //如果正反向分词结果词数不同，则取分词数量较少的那个  
	 if (fmm_list.size() != bmm_list.size()) {
		 if (fmm_list.size() > bmm_list.size())
			 return bmm_list;
		 else return fmm_list;
	 }
	 //如果分词结果词数相同  
	 else {
		 //如果正反向的分词结果相同，就说明没有歧义，可返回任意一个  
		 int i, FSingle = 0, BSingle = 0;
		 bool isSame = true;
		 for (i = 0; i < (int)fmm_list.size(); i++) {
			 if (fmm_list.at(i) != (bmm_list.at(i)))
				 isSame = false;
			 if (fmm_list.at(i).length() == 1)
				 FSingle += 1;
			 if (bmm_list.at(i).length() == 1)
				 BSingle += 1;
		 }
		 if (isSame)
			 return fmm_list;
		 else {
			 //分词结果不同，返回其中单字较少的那个  
			 if (BSingle > FSingle)
				 return fmm_list;
			 else return bmm_list;
		 }
	 }
 }

 void FBSegment::segment2(iutf8string &phrase, uint32_t appid, vector<string> &new_res_all, string mode, bool Hmm_flag) {
	 vector<string> sen_list;
	 set<string> special_set;  // 记录英文和数字字符串
	 string tmp_words = "";
	 bool flag = false; // 记录是否有英文或者数字的flag
	 for (int i = 0; i < phrase.length(); i++) {
		 if (isAlphaOrDigit(phrase[i])) {
			 if (tmp_words != "" and flag == false) {
				 sen_list.push_back(tmp_words);
				 tmp_words = "";
			 }
			 flag = true;
			 tmp_words += phrase[i];
		 }
		 else if(punct_set.find(phrase[i]) != punct_set.end()){
			 if (tmp_words != "") {
				 sen_list.push_back(tmp_words);
				 sen_list.push_back(phrase[i]);
				 if (flag == true) {
					 special_set.insert(tmp_words);
					 flag = false;
				 }
				 tmp_words = "";
			 }
		 }
		 else {
			 if (flag == true) {
				 sen_list.push_back(tmp_words);
				 special_set.insert(tmp_words);
				 flag = false;
				 tmp_words = phrase[i];
			 }
			 else {
				 tmp_words += phrase[i];
			 }
		 }
	 }
	 if (tmp_words != "") {
		 sen_list.push_back(tmp_words);
		 if (flag == true) {
			 special_set.insert(tmp_words);
		 }
	 }
	 tmp_words = "";
	 vector<string> res_all;
	 for (int i = 0; i < (int)sen_list.size(); i++) {
		 // special_set中保存了连续的字母数字串，不需要进行分词
		 if (special_set.find(sen_list[i]) == special_set.end() && punct_set.find(sen_list[i]) == punct_set.end()) {
			 iutf8string utf8_str(sen_list[i]);
			 vector<string> parse_list;
			 if (mode == "Pre") {
				 FMM2(utf8_str, appid, parse_list);
			 }
			 else if (mode == "Post") {
				 BMM2(utf8_str, appid, parse_list);
			 }
			 else if (mode == "DAG") {
				 __cut_DAG_NO_HMM(sen_list[i], appid, parse_list);
			 }
			 else if (mode == "Cache") {  // word dict in cache, not from DTC
				 BMM(utf8_str, appid, parse_list);
			 }
			 else { // PrePostNGram
				 vector<string> parse_list1;
				 vector<string> parse_list2;
				 FMM2(utf8_str, appid, parse_list1);
				 BMM2(utf8_str, appid, parse_list2);
				 parse_list1.insert(parse_list1.begin(), "<BEG>");
				 parse_list1.push_back("<END>");
				 parse_list2.insert(parse_list2.begin(), "<BEG>");
				 parse_list2.push_back("<END>");
				 
				 // CalList1和CalList2分别记录两个句子词序列不同的部分
				 vector<string> cal_list1;
				 vector<string> cal_list2;
				 // pos1和pos2记录两个句子的当前字的位置，cur1和cur2记录两个句子的第几个词
				 uint32_t pos1 = 0;
				 uint32_t pos2 = 0;
				 uint32_t cur1 = 0;
				 uint32_t cur2 = 0;
				 while (1) {
					 if (cur1 == parse_list1.size() && cur2 == parse_list2.size()) {
						 break;
					 }
					 // 如果当前位置一样
					 if (pos1 == pos2) {
						 // 当前位置一样，并且词也一样
						 if (parse_list1[cur1].size() == parse_list2[cur2].size()) {
							 pos1 += parse_list1[cur1].size();
							 pos2 += parse_list2[cur2].size();
							 // 说明此时得到两个不同的词序列，根据bigram选择概率大的
							 // 注意算不同的时候要考虑加上前面一个词和后面一个词，拼接的时候再去掉即可
							 if (cal_list1.size() > 0) {
								 cal_list1.insert(cal_list1.begin(), parse_list[parse_list.size() - 1]);
								 cal_list2.insert(cal_list2.begin(), parse_list[parse_list.size() - 1]);
								 if (cur1 < parse_list1.size()-1) {
									 cal_list1.push_back(parse_list1[cur1]);
									 cal_list2.push_back(parse_list2[cur2]);
								 }
								 double p1 = CalSegProbability(cal_list1);
								 double p2 = CalSegProbability(cal_list2);

								 vector<string> cal_list = (p1 > p2) ? cal_list1 : cal_list2;
								 cal_list.erase(cal_list.begin());
								 if (cur1 < parse_list1.size() - 1) {
									 cal_list.pop_back();
								 }
								 parse_list.insert(parse_list.end(), cal_list.begin(), cal_list.end());
								 cal_list1.clear();
								 cal_list2.clear();
							 }
							 parse_list.push_back(parse_list1[cur1]);
							 cur1++;
							 cur2++;
						 }
						 // pos相同，len(ParseList1[cur1])不同，向后滑动，不同的添加到list中
						 else if (parse_list1[cur1].size() > parse_list2[cur2].size()) {
							 cal_list2.push_back(parse_list2[cur2]);
							 pos2 += parse_list2[cur2].size();
							 cur2++;
						 }
						 else {
							 cal_list1.push_back(parse_list1[cur1]);
							 pos1 += parse_list1[cur1].size();
							 cur1++;
						 }
					 }
					 else { 
						 // pos不同，而结束的位置相同，两个同时向后滑动
						 if (pos1 + parse_list1[cur1].size() == pos2 + parse_list2[cur2].size()) {
							 cal_list1.push_back(parse_list1[cur1]);
							 cal_list2.push_back(parse_list2[cur2]);
							 pos1 += parse_list1[cur1].size();
							 pos2 += parse_list2[cur2].size();
							 cur1++;
							 cur2++;
						 }
						 else if (pos1 + parse_list1[cur1].size() > pos2 + parse_list2[cur2].size()) {
							 cal_list2.push_back(parse_list2[cur2]);
							 pos2 += parse_list2[cur2].size();
							 cur2++;
						 }
						 else {
							 cal_list1.push_back(parse_list1[cur1]);
							 pos1 += parse_list1[cur1].size();
							 cur1++;
						 }
					 }
				 }
				 parse_list.erase(parse_list.begin());
				 parse_list.pop_back();
			 }
			 res_all.insert(res_all.end(), parse_list.begin(), parse_list.end());
		 }
		 else {
			 res_all.push_back(sen_list[i]);
		 }
	 }

	 if (Hmm_flag == false) {
		 new_res_all.assign(res_all.begin(), res_all.end());
	 }
	 else {
		 // 使用HMM发现新词
		 string buf = "";
		 for (size_t i = 0; i < res_all.size(); i++) {
			 iutf8string utf8_str(res_all[i]);
			 if (utf8_str.length() == 1 && punct_set.find(res_all[i]) == punct_set.end() && res_all[i].length() > 1) { // 确保res_all[i]是汉字
				 buf += res_all[i];
			 }
			 else {
				 if (buf.length() > 0) {
					 iutf8string utf8_buf(buf);
					 if (utf8_buf.length() == 1) {
						 new_res_all.push_back(buf);
					 }
					 else if (WordValid(buf, appid) == false) { // 连续的单字组合起来，使用HMM算法进行分词
						 vector<string> vec = HMM_split(buf);
						 new_res_all.insert(new_res_all.end(), vec.begin(), vec.end());
					 }
					 else { // 是否有这种情况
						 new_res_all.push_back(buf);
					 }
				 }
				 buf = "";
				 new_res_all.push_back(res_all[i]);
			 }
		 }

		 if (buf.length() > 0) {
			 iutf8string utf8_buf(buf);
			 if (utf8_buf.length() == 1) {
				 new_res_all.push_back(buf);
			 }
			 else if (WordValid(buf, appid) == false) { // 连续的单字组合起来，使用HMM算法进行分词
				 vector<string> vec = HMM_split(buf);
				 new_res_all.insert(new_res_all.end(), vec.begin(), vec.end());
			 }
			 else { // 是否有这种情况
				 new_res_all.push_back(buf);
			 }
			 buf = "";
		 }
	 }
	 
	 return;
 }

void FBSegment::cut_for_search(iutf8string &phrase, uint32_t appid, vector<vector<string> > &search_res_all, string mode) {
	 // 搜索引擎模式
	vector<string> new_res_all;
	segment2(phrase, appid, new_res_all, mode);
	 for (size_t i = 0; i < new_res_all.size(); i++) {
		 vector<string> vec;
		 iutf8string utf8_str(new_res_all[i]);
		 if (utf8_str.length() > 2 && isAllAlphaOrDigit(new_res_all[i]) == false) {
			 for (int j = 0; j < utf8_str.length() - 1; j++) {
				 string tmp_str = utf8_str.substr(j, 2);
				 if (WordValid(tmp_str, appid) == true) {
					 vec.push_back(tmp_str);
				 }
			 }
		 }
		 if (utf8_str.length() > 3 && isAllAlphaOrDigit(new_res_all[i]) == false) {
			 for (int j = 0; j < utf8_str.length() - 2; j++) {
				 string tmp_str = utf8_str.substr(j, 3);
				 if (WordValid(tmp_str, appid) == true) {
					 vec.push_back(tmp_str);
				 }
			 }
		 }
		 vec.push_back(new_res_all[i]);
		 search_res_all.push_back(vec);
	 }

	 return;
 }

void FBSegment::cut_ngram(iutf8string &phrase, vector<string> &search_res, uint32_t n) {
	uint32_t N = (n > phrase.length()) ? phrase.length() : n;
	for (size_t i = 1; i <= N; i++) {
		for (size_t j = 0; j < phrase.length() - i + 1; j++) {
			string tmp_str = phrase.substr(j, i);
			search_res.push_back(tmp_str);
		}
	}
}

 vector<char> FBSegment::viterbi(string sentence) {
	 iutf8string utf8_str(sentence);
	 vector< map<char, double> > V;
	 map<char, vector<char> > path;
	 char states[4] = { 'B','M','E','S' };
	 map<char, double> prob_map;
	 for (size_t i = 0; i < sizeof(states); i++) {
		 char y = states[i];
		 double emit_value = train_corpus.MinEmit();
		 if (train_corpus.emit_dict[y].find(utf8_str[0]) != train_corpus.emit_dict[y].end()) {
			 emit_value = train_corpus.emit_dict[y].at(utf8_str[0]);
		 }
		 prob_map[y] = train_corpus.start_dict[y] * emit_value;  // 在位置0，以y状态为末尾的状态序列的最大概率
		 path[y].push_back(y);
	 }
	 V.push_back(prob_map);
	 for (int j = 1; j < utf8_str.length(); j++) {
		 map<char, vector<char> > new_path;
		 prob_map.clear();
		 for (size_t k = 0; k < sizeof(states); k++) {
			 char y = states[k];
			 double max_prob = 0.0;
			 char state = ' ';
			 for (size_t m = 0; m < sizeof(states); m++) {
				 char y0 = states[m];  // 从y0 -> y状态的递归
				 //cout << j << " " << y0 << " " << y << " " << V[j - 1][y0] << " " << train_corpus.trans_dict[y0][y] << " " << train_corpus.emit_dict[y].at(utf8_str[j]) << endl;
				 double emit_value = train_corpus.MinEmit();
				 if (train_corpus.emit_dict[y].find(utf8_str[j]) != train_corpus.emit_dict[y].end()) {
					 emit_value = train_corpus.emit_dict[y].at(utf8_str[j]);
				 }
				 double prob = V[j - 1][y0] * train_corpus.trans_dict[y0][y] * emit_value;
				 if (prob > max_prob) {
					 max_prob = prob;
					 state = y0;
				 }
			 }
			 prob_map[y] = max_prob;
			 new_path[y] = path[state];
			 new_path[y].push_back(y);
		 }
		 V.push_back(prob_map);
		 path = new_path;
	 }
	 double max_prob = 0.0;
	 char state = ' ';
	 for (size_t i = 0; i < sizeof(states); i++) {
		 char y = states[i];
		 if (V[utf8_str.length() - 1][y] > max_prob) {
			 max_prob = V[utf8_str.length() - 1][y];
			 state = y;
		 }
	 }
	 return path[state];
 }

 vector<string> FBSegment::HMM_split(string sentence) {

	 vector<char> pos_list = viterbi(sentence);
	 string result;
	 iutf8string utf8_str(sentence);
	 for (size_t i = 0; i < pos_list.size(); i++) {
		 result += utf8_str[i];
		 if (pos_list[i] == 'E') {
			 std::size_t found = result.find_last_of(" ");
			 string new_word = result.substr(found + 1);
			 //printf("new word: %s\n", new_word.c_str());
		 }
		 if (pos_list[i] == 'E' || pos_list[i] == 'S') {
			 result += ' ';
		 }
	 }
	 if (result[result.size()-1] == ' ') {
		 result = result.substr(0, result.size() - 1);
	 }

	 return splitEx(result, " ");
 }

 double FBSegment::CalSegProbability(const vector<string> &vec) {
	 double p = 0;
	 string word1;
	 string word2;
	 // 由于概率很小，对连乘做了取对数处理转化为加法
	 for (int pos = 0; pos < (int)vec.size(); pos++) {
		 if (pos != (int)vec.size() - 1) {
			 // 乘以后面词的条件概率
			 word1 = vec[pos];
			 word2 = vec[pos + 1];
			 if (next_dict.find(word1) == next_dict.end()) {
				 // 加1平滑
				 p += log(1.0 / train_cnt);
			 }
			 else {
				 double numerator = 1.0;
				 uint32_t denominator = train_cnt;
				 map<string, int>::iterator iter = next_dict[word1].begin();
				 for (; iter != next_dict[word1].end(); iter++) {
					 if (iter->first == word2) {
						 numerator += iter->second;
					 }
					 denominator += iter->second;
				 }
				 p += log(numerator / denominator);
			 }
		 }
		 // 乘以第一个词的概率
		 if ((pos == 0 && vec[pos] != "<BEG>") || (pos == 1 && vec[0] == "<BEG>")) {
			 uint32_t word_freq = 0;
			 WordInfo word_info;
			 if (GetWordInfo(vec[pos], 0, word_info)) {
				 word_freq = word_info.word_freq;
				 p += log(word_freq + 1.0 / next_dict.size() + train_cnt);
			 }
			 else {
				 p += log(1.0 / next_dict.size() + train_cnt);
			 }
		 }
	 }

	 return p;
 }
 
 vector<string> FBSegment::segment(iutf8string &phrase, uint32_t appid) {
	 vector<string> res;
	 int last = 0;
	 for (int i = 0; i < phrase.length(); i++) {
		 if (punct_set.find(phrase[i]) != punct_set.end()) {
			 iutf8string fragment = phrase.utf8substr(last, i - last);
			 for (int j = 0; j < fragment.length(); ) { // 继续拆分，将连续的中文或者连续的英文字母合并
				 if (fragment[j].length() == 1) {
					 string tmp = fragment[j];
					 int k = 1;
					 for (; k < fragment.length() - j; k++) {
						 if (fragment[j + k].size() > 1) { // 连续英文，遇到非英文字母停止
							 res.push_back(tmp);
							 break;
						 }
						 else { // 非英文或数字，断开
							 char frag_char = fragment[j + k][0];
							 if (!isupper(frag_char) && !islower(frag_char) && !isdigit(frag_char)) {
								 res.push_back(tmp);
								 break;
							 }
						 }
						 tmp += fragment[j + k];
					 }
					 if (k == fragment.length() - j) { // 如果循环完没有break，则插入到res
						 res.push_back(tmp);
					 }
					 j = j + k;
				 }
				 else {
					 string tmp = fragment[j];
					 int k = 1;
					 for (; k < fragment.length() - j; k++) {
						 if (fragment[j + k].size() == 1) { // 连续中文，遇到非中文字母停止
							 res.push_back(tmp);
							 break;
						 }
						 tmp += fragment[j + k];
					 }
					 if (k == fragment.length() - j) {
						 res.push_back(tmp);
					 }
					 j = j + k;
				 }
			 }
			 last = i + 1;
		 }
	 }
	 if (last < phrase.length()) {
		 string fragment = phrase.substr(last, phrase.length() - last);
		 res.push_back(fragment);
	 }
	 vector<string> res_all;
	 vector<string>::iterator iter = res.begin();
	 for (; iter != res.end(); iter++) {
		 vector<string> res;
		 string str = *iter;
		 if (isAllAlphaOrDigit(str)) { // 英文不进行分词
			 res_all.push_back(str);
		 }
		 else {
			 iutf8string utf8_str(*iter);
			 res = segment_part(utf8_str, appid);
			 res_all.insert(res_all.end(), res.begin(), res.end());
		 }
	 }
	 return res_all;
 }
 
void FBSegment::get_DAG(string sentence, uint32_t appid, map<uint32_t, vector<uint32_t> > &DAG) {
	 iutf8string utf8_str(sentence);
	 uint32_t N = utf8_str.length();
	 for (uint32_t k = 0; k < N; k++) {
		 uint32_t i = k;
		 vector<uint32_t> tmplist;
		 string frag = utf8_str[k];
		 while (i < N) {
			 if (WordValid(frag, appid) == true) {
				 tmplist.push_back(i);
			 }
			 i++;
			 frag = utf8_str.substr(k, i + 1 - k);
		 }
		 if (tmplist.empty()) {
			 tmplist.push_back(k);
		 }
		 DAG[k] = tmplist;
	 }
	 return;
 }

 void FBSegment::calc(string sentence, const map<uint32_t, vector<uint32_t> > &DAG, map<uint32_t, RouteValue> &route, uint32_t appid) {
	 iutf8string utf8_str(sentence);
	 uint32_t N = utf8_str.length();
	 RouteValue route_N;
	 route[N] = route_N;
	 double logtotal = log(TOTAL);
	 for (int i = N - 1; i > -1; i--) {
		 vector<uint32_t> vec = DAG.at(i);
		 double max_route = -DBL_MAX;
		 uint32_t max_idx = 0;
		 for (size_t t = 0; t < vec.size(); t++) {
			 string word = utf8_str.substr(i, vec[t] + 1 - i);
			 WordInfo word_info;
			 uint32_t word_freq = 1;
			 /* 不查DTC，改为从本地词库查询
			 if (word_manager.WordValid(word, appid, word_info) == true) {
				 word_freq = word_info.word_freq;
			 }
			 */
			 if (word_dict.find(word) != word_dict.end()) {
				 map<uint32_t, WordInfo> wordInfo = word_dict[word];
				 if (wordInfo.find(0) != wordInfo.end()) {
					 word_info = wordInfo[0];
					 word_freq = word_info.word_freq;
				 }
				 if (wordInfo.find(appid) != wordInfo.end()) {
					 word_info = wordInfo[appid];
					 word_freq = word_info.word_freq;
				 }
			 }
			 double route_value = log(word_freq) - logtotal + route[vec[t] + 1].max_route;
			 if (route_value > max_route) {
				 max_route = route_value;
				 max_idx = vec[t];
			 }
		 }
		 RouteValue route_value;
		 route_value.max_route = max_route;
		 route_value.idx = max_idx;
		 route[i] = route_value;
	 }
 }

void FBSegment::__cut_DAG_NO_HMM(string sentence, uint32_t appid, vector<string> &vec) {
	 map<uint32_t, vector<uint32_t> > DAG;
	 get_DAG(sentence, appid, DAG);
	 map<uint32_t, RouteValue> route;
	 calc(sentence, DAG, route, appid);
	 iutf8string utf8_str(sentence);
	 uint32_t N = utf8_str.length();
	 uint32_t i = 0;
	 string buf = "";
	 while (i < N) {
		 uint32_t j = route[i].idx + 1;
		 string l_word = utf8_str.substr(i, j - i);
		 if (isAllAlphaOrDigit(l_word)) {
			 buf += l_word;
			 i = j;
		 }
		 else {
			 if (!buf.empty()) {
				 vec.push_back(buf);
				 buf = "";
			 }
			 vec.push_back(l_word);
			 i = j;
		 }
	 }
	 if (!buf.empty()) {
		 vec.push_back(buf);
		 buf = "";
	 }

	 return;
 }
