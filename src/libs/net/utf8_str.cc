#include "utf8_str.h"
#include "log.h"

iutf8string::iutf8string(const string& str)
{
	data = str;
	refresh();
}

iutf8string::iutf8string(const char* str)
{
	data = string(str);
	refresh();
}

iutf8string::~iutf8string()
{
	delete[] offerset;
}

string iutf8string::stlstring()
{
	return data;
}

const char* iutf8string::c_str()
{
	return data.c_str();
}

iutf8string iutf8string::operator +(iutf8string& ustr)
{
	string temp = data + ustr.stlstring();

	return iutf8string(temp);
}

int iutf8string::length()
{

	return _length;
}

string iutf8string::get(int index)
{
	if (index >= _length) return "";
	string temp = data.substr(offerset[index], offerset[index + 1] - offerset[index]);

	return temp;
}

string iutf8string::operator [](int index)
{
	if (index >= _length) return "";
	string temp = data.substr(offerset[index], offerset[index + 1] - offerset[index]);

	return temp;
}

string iutf8string::substr(int u8_start_index, int u8_length)
{
	if (u8_start_index + u8_length > _length) return "";

	return data.substr(offerset[u8_start_index], offerset[u8_start_index + u8_length] - offerset[u8_start_index]);
}

iutf8string iutf8string::utf8substr(int u8_start_index, int u8_length)
{
	if (u8_start_index + u8_length > _length) return iutf8string("");
	string ret = data.substr(offerset[u8_start_index], offerset[u8_start_index + u8_length] - offerset[u8_start_index]);

	return iutf8string(ret);
}

void iutf8string::refresh()
{
	int *tmp = new int[data.length()+1];
	if (tmp == NULL) {
		log_error("alloc memeory failed!");
		return;
	}
	int i, tmpidx = 0;
	for (i = 0; i < (int)data.length(); i++)
	{
		if (((int)data[i] > 0) || (!(((int)data[i] & 0x00000040) == 0)))
		{
			tmp[tmpidx] = i;
			tmpidx++;
		}
	}

	tmp[tmpidx] = data.length();

	int *tmp2 = new int[tmpidx+1];
	if (tmp2 == NULL) {
		log_error("alloc memeory failed!");
		return;
	}
	for (i = 0; i < tmpidx; i++)
	{
		tmp2[i] = tmp[i];
	}
	tmp2[i] = data.length();

	delete[] tmp;
	offerset = tmp2;
	_length = tmpidx;
}

vector<string> splitEx(const string& src, string separate_character)
{
	vector<string> strs;

	//分割字符串的长度,这样就可以支持如“,,”多字符串的分隔符
	int separate_characterLen = separate_character.size();
	int lastPosition = 0, index = -1;
	while (-1 != (index = src.find(separate_character, lastPosition)))
	{
		if (src.substr(lastPosition, index - lastPosition) != " ") {
			strs.push_back(src.substr(lastPosition, index - lastPosition));
		}
		lastPosition = index + separate_characterLen;
	}
	string lastString = src.substr(lastPosition);//截取最后一个分隔符后的内容
	if (!lastString.empty() && lastString != " ")
		strs.push_back(lastString);//如果最后一个分隔符后还有内容就入队
	return strs;
}