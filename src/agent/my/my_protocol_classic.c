#include "my_protocol_classic.h"

static inline char *strend(char *s) {
  while (*s++)
    ;
  return s - 1;
}

bool parse_packet(uchar* input_raw_packet, int input_packet_length, union COM_DATA *data, enum enum_server_command cmd) 
{
  switch (cmd) {
    case COM_INIT_DB: {
      data->com_init_db.db_name =
          (const char*)(input_raw_packet);
      data->com_init_db.length = input_packet_length;
      break;
    }
    case COM_REFRESH: {
      if (input_packet_length < 1) goto malformed;
      data->com_refresh.options = input_raw_packet[0];
      break;
    }
    case COM_PROCESS_KILL: {
      if (input_packet_length < 4) goto malformed;
      data->com_kill.id = (ulong)uint4korr(input_raw_packet);
      break;
    }
    case COM_SET_OPTION: {
      if (input_packet_length < 2) goto malformed;
      data->com_set_option.opt_command = uint2korr(input_raw_packet);
      break;
    }
    case COM_STMT_EXECUTE: {
      if (input_packet_length < 9) goto malformed;

      break;
    }
    case COM_STMT_FETCH: {
      if (input_packet_length < 8) goto malformed;
      data->com_stmt_fetch.stmt_id = uint4korr(input_raw_packet);
      data->com_stmt_fetch.num_rows = uint4korr(input_raw_packet + 4);
      break;
    }
    case COM_STMT_SEND_LONG_DATA: {
      if (input_packet_length < MYSQL_LONG_DATA_HEADER) goto malformed;
      data->com_stmt_send_long_data.stmt_id = uint4korr(input_raw_packet);
      data->com_stmt_send_long_data.param_number =
          uint2korr(input_raw_packet + 4);
      data->com_stmt_send_long_data.longdata = input_raw_packet + 6;
      data->com_stmt_send_long_data.length = input_packet_length - 6;
      break;
    }
    case COM_STMT_PREPARE: {
      data->com_stmt_prepare.query =
          (const char *)(input_raw_packet);
      data->com_stmt_prepare.length = input_packet_length;
      break;
    }
    case COM_STMT_CLOSE: {
      if (input_packet_length < 4) goto malformed;

      data->com_stmt_close.stmt_id = uint4korr(input_raw_packet);
      break;
    }
    case COM_STMT_RESET: {
      if (input_packet_length < 4) goto malformed;

      data->com_stmt_reset.stmt_id = uint4korr(input_raw_packet);
      break;
    }
    case COM_QUERY: {
      uchar *read_pos = input_raw_packet;
      size_t packet_left = input_packet_length;

      break;
    }
    case COM_FIELD_LIST: {
      /*
        We have name + wildcard in packet, separated by endzero
      */
      ulong len = strend((char *)input_raw_packet) - (char *)input_raw_packet;

      if (len >= input_packet_length || len > NAME_LEN) goto malformed;

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