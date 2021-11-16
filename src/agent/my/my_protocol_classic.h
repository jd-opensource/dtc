#include "my_command.h"
#include "my_comm.h"
#include "../da_msg.h"

bool parse_packet(uchar* input_raw_packet, int input_packet_length, struct msg* r, enum enum_server_command cmd);