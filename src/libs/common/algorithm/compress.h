/*
* Copyright JD.com, Inc.
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

	//source: buffer to be compressed, sourcelen: original length of buffer to be compressed
	//dest: compressed buffer, destlen: length of compressed buffer
	//Note: when calling this function, destlen must first be set to the maximum capacity of dest buffer
	int compress(const char *source, unsigned long sourceLen);

	//source: buffer to be decompressed, sourcelen: original length of buffer to be decompressed
	//dest: decompressed buffer, destlen: length of decompressed buffer
	//Note: when calling this function, destlen must first be set to the maximum capacity of dest buffer
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
