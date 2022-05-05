#ifndef __CH_PROTOCOL_H_
#define __CH_PROTOCOL_H_
#include <stdint.h>


#define MAXFIELDS_PER_TABLE	255
#define MAXPACKETSIZE	(64<<20)

struct CPacketHeader {
	uint16_t magic;//0xFDFC
	uint16_t cmd;
	uint32_t len;
	uint32_t seq_num;
};

struct CTimeInfo {
	uint64_t time;
};

class DField {
public:
    enum {
	None=0,	// undefined
	Signed=1,	// Signed Integer
	Unsigned=2,	// Unsigned Integer
	Float=3,	// FloatPoint
	String=4,	// String, case insensitive, null ended
	Binary=5,	// opaque binary data
	TotalType
    };

    enum {
	Set = 0,
	Add = 1,
	SetBits = 2,
	OR = 3,
	TransactionSQL = 4,
	TotalOperation
    };

    enum {
	EQ = 0,
	NE = 1,
	LT = 2,
	LE = 3,
	GT = 4,
	GE = 5,
	TotalComparison
    };
};


#endif
