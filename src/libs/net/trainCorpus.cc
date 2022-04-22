#include "trainCorpus.h"
#include "log.h"
#include "utf8_str.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <stdint.h>
using namespace std;

vector<char> getList(uint32_t str_len) {
	vector<char> output;
	if (str_len == 1) {
		output.push_back('S');
	}
	else if (str_len == 2) {
		output.push_back('B');
		output.push_back('E');
	}
	else {
		vector<char> M_list(str_len - 2, 'M');
		output.push_back('B');
		output.insert(output.end(), M_list.begin(), M_list.end());
		output.push_back('E');
	}
	return output;
}

TrainCorpus::TrainCorpus() {
	min_emit = 1.0;
	char state[4] = { 'B','M','E','S' };
	vector<char> state_list(state, state + 4);
	for (size_t i = 0; i < state_list.size(); i++) {
		map<char, double> trans_map;
		trans_dict.insert(make_pair(state_list[i], trans_map));
		for (size_t j = 0; j < state_list.size(); j++) {
			trans_dict[state_list[i]].insert(make_pair(state_list[j], 0.0));
		}
	}
	for (size_t i = 0; i < state_list.size(); i++) {
		map<string, double> emit_map;
		emit_dict.insert(make_pair(state_list[i], emit_map));
		start_dict.insert(make_pair(state_list[i],0.0));
		count_dict.insert(make_pair(state_list[i], 0));
	}
	line_num = 0;
}

bool TrainCorpus::Init(string train_path) {
	ifstream train_infile;  // 训练文件以空格为分隔符
	train_infile.open(train_path.c_str());
	if (train_infile.is_open() == false) {
		log_error("open file error: %s.\n", train_path.c_str());
		return false;
	}
	string str;
	while (getline(train_infile, str))
	{
		line_num++;
		vector<string> word_list; // 保存除空格以外的字符
		iutf8string utf8_str(str);
		for (int i = 0; i < utf8_str.length(); i++) {
			if (utf8_str[i] != " ") {
				word_list.push_back(utf8_str[i]);
			}
		}
		vector<char> line_state;
		vector<string> str_vec = splitEx(str, "  ");
		for (size_t i = 0; i < str_vec.size(); i++) {
			if (str_vec[i] == "") {
				continue;
			}
			iutf8string utf8_str_item(str_vec[i]);
			vector<char> item_state = getList(utf8_str_item.length());
			line_state.insert(line_state.end(), item_state.begin(), item_state.end());
		}
		if (word_list.size() != line_state.size()) {
			log_error("[line = %s]\n", str.c_str());
		}
		else {
			for (size_t i = 0; i < line_state.size(); i++) {
				if (i == 0) {
					start_dict[line_state[0]] += 1;  // 记录句子第一个字的状态，用于计算初始状态概率
					count_dict[line_state[0]] ++;  // 记录每个状态的出现次数
				}
				else {
					trans_dict[line_state[i - 1]][line_state[i]] += 1;
					count_dict[line_state[i]] ++;
					if (emit_dict[line_state[i]].find(word_list[i]) == emit_dict[line_state[i]].end()) {
						emit_dict[line_state[i]].insert(make_pair(word_list[i], 0.0));
					}
					else {
						emit_dict[line_state[i]][word_list[i]] += 1;
					}
				}
			}
		}
	}
	train_infile.close();

	map<char, double>::iterator start_iter = start_dict.begin();
	for (; start_iter != start_dict.end(); start_iter++) {  // 状态的初始概率
		start_dict[start_iter->first] = start_dict[start_iter->first] * 1.0 / line_num;
	}

	map<char, map<char, double> >::iterator trans_iter = trans_dict.begin();
	for (; trans_iter != trans_dict.end(); trans_iter++) {  // 状态转移概率
		map<char, double> trans_map = trans_iter->second;
		map<char, double>::iterator trans_iter2 = trans_map.begin();
		for (; trans_iter2 != trans_map.end(); trans_iter2++) {
			trans_dict[trans_iter->first][trans_iter2->first] = trans_dict[trans_iter->first][trans_iter2->first] / count_dict[trans_iter->first];
		}
	}

	map<char, map<string, double> >::iterator emit_iter = emit_dict.begin();
	for (; emit_iter != emit_dict.end(); emit_iter++) {  // 发射概率(状态->词语的条件概率)
		map<string, double> emit_map = emit_iter->second;
		map<string, double>::iterator emit_iter2 = emit_map.begin();
		for (; emit_iter2 != emit_map.end(); emit_iter2++) {
			double emit_value = emit_dict[emit_iter->first][emit_iter2->first] / count_dict[emit_iter->first];
			if (emit_value < min_emit && emit_value != 0.0) {
				min_emit = emit_value;
			}
			emit_dict[emit_iter->first][emit_iter2->first] = emit_value;
		}
	}

	return true;
}