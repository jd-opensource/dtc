#include "my_command.h"
#include "my_comm.h"
#include "my_com_data.h"

bool parse_packet(uint8_t *input_raw_packet, int32_t input_packet_length,
		  union COM_DATA *data, enum enum_server_command cmd);