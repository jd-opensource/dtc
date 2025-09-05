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

#ifndef DA_ERRNO_H_
#define DA_ERRNO_H_

enum conn_errno {
	CONN_EPOLLCTL_ERR = 1,
	CONN_EPOLLADD_ERR = 2,
	CONN_EPOLL_DEL_ERR = 3,
	CONN_CONNECT_ERR = 4,
	CONN_SETNOBLOCK_ERR = 5,
	CONN_CREATSOCK_ERR = 6,
	CONN_RECV_ERR = 7,
	CONN_SEND_ERR = 8,
	CONN_MSG_GET_ERR = 9,
	CONN_BUF_GET_ERR = 10,
	CONN_MSG_PARSE_ERR = 11,
	CONN_NOT_VALID = 12,
};

enum msg_errno {
	MSG_FRAGMENT_ERR = 2,
	MSG_REQ_FORWARD_ERR = 3,
	MSG_BACKWORKER_ERR = 4,
	MSG_COALESCE_ERR = 5,
	MSG_NOKEY_ERR = 6,
	MSG_MYSQL_COMPATIBLE_ERR = 7,
};

static inline char *GetMsgErrorCodeStr(int errcode)
{
	enum msg_errno err;
	err = (enum msg_errno)errcode;
	switch (err) {
	case MSG_FRAGMENT_ERR:
		return "fragment request error!";
	case MSG_REQ_FORWARD_ERR:
		return "forward request to server error!";
	case MSG_BACKWORKER_ERR:
		return "server error!";
	case MSG_COALESCE_ERR:
		return "coalesce request error!";
	case MSG_NOKEY_ERR:
		return "request without key!";
  case MSG_MYSQL_COMPATIBLE_ERR:
    return "mysql protocol compatible version no fragment.";
	}
	return "unknow";
}

static inline char *GetConnErrorCodeStr(int errcode)
{
	enum conn_errno err;
	err = (enum conn_errno)errcode;
	switch (err) {
	case CONN_EPOLLCTL_ERR:
		return "conn epoll ctl error";
	case CONN_EPOLLADD_ERR:
		return "conn epoll add error";
	case CONN_EPOLL_DEL_ERR:
		return "conn epoll del error";
	case CONN_CONNECT_ERR:
		return "conn server error";
	case CONN_SETNOBLOCK_ERR:
		return "conn set noblock error";
	case CONN_CREATSOCK_ERR:
		return "conn create socket error";
	case CONN_RECV_ERR:
		return "conn recv error";
	case CONN_SEND_ERR:
		return "conn send error";
	case CONN_MSG_GET_ERR:
		return "conn get msg error";
	case CONN_BUF_GET_ERR:
		return "conn get buf error";
	case CONN_MSG_PARSE_ERR:
		return "conn parse msg error";
	case CONN_NOT_VALID:
		return "conn accesstoken invalid";
	}
	return "unknow";
}

#endif /* DA_ERRNO_H_ */
