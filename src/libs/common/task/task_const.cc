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
#include "task_base.h"

// convert stage to result code
const DecodeResult DtcJob::stage2result[] = {
	/*DecodeStageFatalError */ DecodeFatalError,
	/*DecodeStageDataError  */ DecodeDataError,
	/*DecodeStageIdle       */ DecodeIdle,
	/*DecodeStageWaitHeader */ DecodeWaitData,
	/*DecodeStageWaitData   */ DecodeWaitData,
	/*DecodeStageDiscard    */ DecodeWaitData,
	/*DecodeStageDone       */ DecodeDone,
};

const uint32_t DtcJob::validcmds[] = {
	//00000001 NOP
	//00000002 RESULTCODE
	//00000004 RESULTSET
	//00000008 SVRADMIN
	//00000010 GET
	//00000020 PURGE
	//00000040 INSERT
	//00000080 UPDATE
	//00000100 DELETE
	//00000200 OBSOLETED
	//00000400 OBSOLETED
	//00000800 OBSOLETED
	//00001000 REPLACE
	//00002000 FLUSH
	//00004000 Invalidate
	//00008000 bit15
	0x000071F9 /* Client --> Server/Helper */,
	0x00000006 /* Server --> Client */,
	0x00000006 /* Helper --> Server */,
};

const uint16_t DtcJob::cmd2type[] = {
	TaskTypeAdmin, // NOP
	TaskTypeAdmin, // RESULTCODE
	TaskTypeAdmin, // RESULTSET
	TaskTypeAdmin, // SVRADMIN
	TaskTypeRead, // GET
	TaskTypeAdmin, // PURGE
	TaskTypeWrite, // INSERT
	TaskTypeWrite, // UPDATE
	TaskTypeWrite, // DELETE
	TaskTypeAdmin, // OBSOLETED
	TaskTypeAdmin, // OBSOLETED
	TaskTypeAdmin, // OBSOLETED
	//	TaskTypeCommit, // REPLACE
	TaskTypeWrite, // REPLACE
	TaskTypeWrite, // Flush
	TaskTypeWrite, // Invalidate
	TaskTypeAdmin, // Monitor,此条是为了占位
	TaskTypeHelperReloadConfig, //Reload
};

const uint16_t DtcJob::validsections[][2] = {
	//0001 VersionInfo
	//0002 DataDefinition
	//0004 RequestInfo
	//0008 ResultInfo
	//0010 UpdateInfo
	//0020 ConditionInfo
	//0040 FieldSet
	//0080 DTCResultSet
	{ 0x0000, 0x0005 }, // NOP
	{ 0x0008, 0x000B }, // RESULTCODE
	{ 0x0089, 0x008B }, // RESULTSET
	{ 0x0005, 0x0075 }, // SVRADMIN
	{ 0x0005, 0x0065 }, // GET
	{ 0x0005, 0x0025 }, // PURGE
	{ 0x0001, 0x0015 }, // INSERT
	{ 0x0015, 0x0035 }, // UPDATE
	{ 0x0005, 0x0025 }, // DELETE
	{ 0x0001, 0x0051 }, // OBSOLETED
	{ 0x0015, 0x0055 }, // OBSOLETED
	{ 0x0005, 0x0045 }, // OBSOLETED
	{ 0x0001, 0x0015 }, // REPLACE
	{ 0x0005, 0x0025 }, // Flush
	{ 0x0005, 0x0025 }, // Invalidate
};

/* [clientkey][serverkey] */
const uint8_t DtcJob::validktype[DField::TotalType][DField::TotalType] = {
	/* -, d, u, f, s, b */
	{ 0, 0, 0, 0, 0, 0 }, /* - */
	{ 0, 1, 1, 0, 0, 0 }, /* d */
	{ 0, 1, 1, 0, 0, 0 }, /* u */
	{ 0, 0, 0, 0, 0, 0 }, /* f */
	{ 0, 0, 0, 0, 1, 1 }, /* s */
	{ 0, 0, 0, 0, 1, 1 }, /* b */
};

/* [fieldtype][ valuetype] */
const uint8_t DtcJob::validxtype[DField::TotalOperation][DField::TotalType]
				[DField::TotalType] = {
					//set
					{
						/* -, d, u, f, s, b */
						{ 0, 0, 0, 0, 0, 0 }, /* - */
						{ 0, 1, 1, 0, 0, 0 }, /* d */
						{ 0, 1, 1, 0, 0, 0 }, /* u */
						{ 0, 1, 1, 1, 0, 0 }, /* f */
						{ 0, 0, 0, 0, 1, 0 }, /* s */
						{ 0, 0, 0, 0, 1, 1 }, /* b */
					},
					//add
					{
						/* -, d, u, f, s, b */
						{ 0, 0, 0, 0, 0, 0 }, /* - */
						{ 0, 1, 1, 0, 0, 0 }, /* d */
						{ 0, 1, 1, 0, 0, 0 }, /* u */
						{ 0, 1, 1, 1, 0, 0 }, /* f */
						{ 0, 0, 0, 0, 0, 0 }, /* s */
						{ 0, 0, 0, 0, 0, 0 }, /* b */
					},
					//setbits
					{
						/* -, d, u, f, s, b */
						{ 0, 0, 0, 0, 0, 0 }, /* - */
						{ 0, 1, 1, 0, 0, 0 }, /* d */
						{ 0, 1, 1, 0, 0, 0 }, /* u */
						{ 0, 0, 0, 0, 0, 0 }, /* f */
						{ 0, 1, 1, 0, 0, 0 }, /* s */
						{ 0, 1, 1, 0, 0, 0 }, /* b */
					},
					//OR
					{
						/* -, d, u, f, s, b */
						{ 0, 0, 0, 0, 0, 0 }, /* - */
						{ 0, 1, 1, 0, 0, 0 }, /* d */
						{ 0, 1, 1, 0, 0, 0 }, /* u */
						{ 0, 0, 0, 0, 0, 0 }, /* f */
						{ 0, 0, 0, 0, 0, 0 }, /* s */
						{ 0, 0, 0, 0, 0, 0 }, /* b */
					},
				};

/* [fieldtype][cmp] */
const uint8_t DtcJob::validcomps[DField::TotalType][DField::TotalComparison] = {
	/* EQ,NE,LT,LE,GT,GE */
	{ 0, 0, 0, 0, 0, 0 }, /* - */
	{ 1, 1, 1, 1, 1, 1 }, /* d */
	{ 1, 1, 1, 1, 1, 1 }, /* u */
	{ 0, 0, 0, 0, 0, 0 }, /* f */
	{ 1, 1, 0, 0, 0, 0 }, /* s */
	{ 1, 1, 0, 0, 0, 0 }, /* b */
};
