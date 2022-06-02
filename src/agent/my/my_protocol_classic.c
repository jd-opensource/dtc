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
		//if(layer <= 0 || layer > 3)
		//	layer = 3;

		if (layer < 0) {
			log_error("my_get_route_key return value: %d", layer);
			r->keys[0].start = NULL;
			r->keys[0].end = NULL;
			r->admin = CMD_SQL_PASS_OK;
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
			r->admin = CMD_SQL_PASS_OK;
			return false;
		}
	}
	default:
		break;
	}

	return true;

malformed:
	return false;
}