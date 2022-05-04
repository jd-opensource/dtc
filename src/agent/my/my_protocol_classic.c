#include "my_protocol_classic.h"
#include "my_com_data.h"

static inline char *strend(char *s)
{
	while (*s++)
		;
	return s - 1;
}

bool parse_packet(uchar *input_raw_packet, int input_packet_length,
		  struct msg *r, enum enum_server_command cmd)
{
	union COM_DATA *data = &(r->data);
	log_debug("input_raw_packet:%p, input_packet_length:%d, cmd:%d",
		  input_raw_packet, input_packet_length, cmd);
	switch (cmd) {
	case COM_INIT_DB: {
		data->com_init_db.db_name = (const char *)(input_raw_packet);
		data->com_init_db.length = input_packet_length;
		break;
	}
	case COM_REFRESH: {
		if (input_packet_length < 1)
			goto malformed;
		data->com_refresh.options = input_raw_packet[0];
		break;
	}
	case COM_PROCESS_KILL: {
		if (input_packet_length < 4)
			goto malformed;
		data->com_kill.id = (ulong)uint4korr(input_raw_packet);
		break;
	}
	case COM_SET_OPTION: {
		if (input_packet_length < 2)
			goto malformed;
		data->com_set_option.opt_command = uint2korr(input_raw_packet);
		break;
	}
	case COM_STMT_EXECUTE: {
		if (input_packet_length < 9)
			goto malformed;

		break;
	}
	case COM_STMT_FETCH: {
		if (input_packet_length < 8)
			goto malformed;
		data->com_stmt_fetch.stmt_id = uint4korr(input_raw_packet);
		data->com_stmt_fetch.num_rows = uint4korr(input_raw_packet + 4);
		break;
	}
	case COM_STMT_SEND_LONG_DATA: {
		if (input_packet_length < MYSQL_LONG_DATA_HEADER)
			goto malformed;
		data->com_stmt_send_long_data.stmt_id =
			uint4korr(input_raw_packet);
		data->com_stmt_send_long_data.param_number =
			uint2korr(input_raw_packet + 4);
		data->com_stmt_send_long_data.longdata = input_raw_packet + 6;
		data->com_stmt_send_long_data.length = input_packet_length - 6;
		break;
	}
	case COM_STMT_PREPARE: {
		data->com_stmt_prepare.query = (const char *)(input_raw_packet);
		data->com_stmt_prepare.length = input_packet_length;
		break;
	}
	case COM_STMT_CLOSE: {
		if (input_packet_length < 4)
			goto malformed;

		data->com_stmt_close.stmt_id = uint4korr(input_raw_packet);
		break;
	}
	case COM_STMT_RESET: {
		if (input_packet_length < 4)
			goto malformed;

		data->com_stmt_reset.stmt_id = uint4korr(input_raw_packet);
		break;
	}
	case COM_QUERY: {
		int start_offset, end_offset;

		uint8_t *p = input_raw_packet;
		log_debug("len: %d", input_packet_length);

		if (*p == 0x0) {
			log_debug("len: %d", input_packet_length);
			p++;
			input_packet_length--;
		}

		if (*p == 0x1) {
			log_debug("len: %d", input_packet_length);
			p++;
			input_packet_length--;
		}

		log_debug("len: %d", input_packet_length);

		int layer = my_get_route_key(p, input_packet_length,
					   &start_offset, &end_offset);
		if (layer < 0) {
			log_error("my_get_route_key return value: %d", layer);
			r->keys[0].start = NULL;
			r->keys[0].end = NULL;
			return false;
		} else if (layer == 3) {	//forward to full database.
			log_debug("L3");
			r->keys[0].start = NULL;
			r->keys[0].end = NULL;
			r->layer = layer;
			r->admin = CMD_NOP;
			return true;
		} else if (layer == 2) {	//forward to sharding hot database.
			log_debug("L2");
			r->keys[0].start = NULL;
			r->keys[0].end = NULL;
			r->layer = layer;
			r->admin = CMD_NOP;
			return true;
		} else if (layer == 1) {	//forward to DTC.
			log_debug("L1");
			log_debug("my_get_route_key parse success. %d, %d",
			  start_offset, end_offset);
			r->keys[0].start = input_raw_packet + start_offset;
			r->keys[0].end = input_raw_packet + end_offset;
			r->layer = layer;
			r->admin = CMD_NOP;
			break;
		}
		else{
			log_error("layer error: %d", layer);
			r->keys[0].start = NULL;
			r->keys[0].end = NULL;
			return false;
		}
	}
	case COM_FIELD_LIST: {
		/*
        We have name + wildcard in packet, separated by endzero
      */
		ulong len = strend((char *)input_raw_packet) -
			    (char *)input_raw_packet;

		if (len >= input_packet_length || len > NAME_LEN)
			goto malformed;

		data->com_field_list.table_name = input_raw_packet;
		data->com_field_list.table_name_length = len;

		data->com_field_list.query = input_raw_packet + len + 1;
		data->com_field_list.query_length = input_packet_length - len;
		break;
	}
	default:
		break;
	}

	return true;

malformed:
	return false;
}