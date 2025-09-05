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
#ifndef _H_MD5_H
#define _H_MD5_H

#include <stdint.h>
#include <sys/cdefs.h>
__BEGIN_DECLS

#define MD5Init _TX_MD5Init_
#define MD5Update _TX_MD5Update_
#define MD5Final _TX_MD5Final_
#define MD5Digest _TX_MD5Digest_
#define MD5DigestHex _TX_MD5DigestHex_
#define MD5HMAC _TX_MD5HMAC_
#define MD5HMAC2 _TX_MD5HMAC2_

struct MD5Context {
	uint32_t buf[4];
	uint32_t bits[2];
	uint8_t in[64];
};

typedef struct MD5Context md5_t;

extern void MD5Init(struct MD5Context *);
extern void MD5Update(struct MD5Context *, unsigned char const *, unsigned);
extern void MD5Final(unsigned char digest[16], struct MD5Context *ctx);
extern void MD5Digest(const unsigned char *msg, int len, unsigned char *digest);
extern void MD5DigestHex(const unsigned char *msg, int len,
			 unsigned char *digest);
extern void MD5HMAC(const unsigned char *password, unsigned pass_len,
		    const unsigned char *challenge, unsigned chal_len,
		    unsigned char response[16]);
extern void MD5HMAC2(const unsigned char *password, unsigned pass_len,
		     const unsigned char *challenge, unsigned chal_len,
		     const unsigned char *challenge2, unsigned chal_len2,
		     unsigned char response[16]);

__END_DECLS
#endif
