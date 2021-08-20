/*
* Copyright [2021] JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
class DTCCompress {
    public:
	DTCCompress();
	virtual ~DTCCompress();

	void set_compress_level(int level);
	int set_buffer_len(unsigned long len);
	const char *error_message(void) const
	{
		return errmsg_;
	}

	//source 被压缩的缓冲区 sourcelen 被压缩缓冲区的原始长度
	//dest   压缩后的缓冲区 destlen   被压缩后的缓冲区长度
	//注意调用该函数时， destlen 首先要设置为dest缓冲区最大可以容纳的长度
	int compress(const char *source, unsigned long sourceLen);

	//source 待解压的缓冲区 sourcelen 待解压缓冲区的原始长度
	//dest   解压后的缓冲区 destlen   解缩后的缓冲区长度
	//注意调用该函数时， destlen 首先要设置为dest缓冲区最大可以容纳的长度
	int UnCompress(char **dest, int *destlen, const char *source,
		       unsigned long sourceLen);

	unsigned long get_len(void)
	{
		return _len;
	}
	char *get_buf(void)
	{
		return (char *)_buf;
	}
	char errmsg_[512];

    private:
	unsigned long _buflen;
	unsigned char *_buf;
	unsigned long _len;
	int _level;
};
