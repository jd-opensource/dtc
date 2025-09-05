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
#ifdef _CGIHOST_PROCESS
#include "../include/tsfnew.h"
#endif

#include "algorithm/markup_stl.h"
#include <stdio.h>
#include <stdint.h>
using namespace std;

#ifdef _DEBUG
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#define new DEBUG_NEW
#endif

//[CMARKUPDEV
// To add a version tag to new documents, define MARKUP_VERSIONTAG
#ifdef MARKUP_VERSIONTAG
#define x_VERSIONTAG "<?xml version=\"1.0\"?>\r\n"
#endif
//]CMARKUPDEV

void MarkupSTL::operator=(const MarkupSTL &markup)
{
	m_iPosParent = markup.m_iPosParent;
	m_iPos = markup.m_iPos;
	m_iPosChild = markup.m_iPosChild;
	m_iPosFree = markup.m_iPosFree;
	m_nNodeType = markup.m_nNodeType;
	//[CMARKUPDEV
	m_nNodeOffset = markup.m_nNodeOffset;
	m_nNodeLength = markup.m_nNodeLength;
	//]CMARKUPDEV
	m_aPos = markup.m_aPos;
	m_strDoc = markup.m_strDoc;
	MARKUP_SETDEBUGSTATE;
}

bool MarkupSTL::set_doc(const char *szDoc)
{
	// Reset indexes
	m_iPosFree = 1;
	reset_pos();
	m_mapSavedPos.clear();

	// Set document text
	if (szDoc)
		m_strDoc = szDoc;
	else
		m_strDoc.erase();

	// Starting size of position array: 1 element per 64 bytes of document
	// Tight fit when parsing small doc, only 0 to 2 reallocs when parsing large doc
	// Start at 8 when creating new document
	uint32_t nStartSize = m_strDoc.size() / 64 + 8;
	if (m_aPos.size() < nStartSize)
		m_aPos.resize(nStartSize);

	// Parse document
	bool bWellFormed = false;
	if (m_strDoc.size()) {
		m_aPos[0].Clear();
		int iPos = x_ParseElem(0);
		if (iPos > 0) {
			m_aPos[0].iElemChild = iPos;
			bWellFormed = true;
		}
	}

	// Clear indexes if parse failed or empty document
	if (!bWellFormed) {
		m_aPos[0].Clear();
		m_iPosFree = 1;
	}

	reset_pos();
	return bWellFormed;
};

bool MarkupSTL::is_well_formed()
{
	if (m_aPos.size() && m_aPos[0].iElemChild)
		return true;
	return false;
}

bool MarkupSTL::Load(const char *szFileName)
{
	// Load document from file
	bool bResult = false;
	FILE *fp = fopen(szFileName, "rb");
	if (fp) {
		// Determine file length
		fseek(fp, 0L, SEEK_END);
		int nFileLen = ftell(fp);
		fseek(fp, 0L, SEEK_SET);

		// Load string
		allocator<char> mem;
		allocator<char>::pointer pBuffer =
			mem.allocate(nFileLen + 1, NULL);
		if (fread(pBuffer, nFileLen, 1, fp) == 1) {
			pBuffer[nFileLen] = '\0';
			bResult = set_doc(pBuffer);
		}
		fclose(fp);
		mem.deallocate(pBuffer, 1);
	}
	if (!bResult) {
		set_doc(NULL);
	}
	MARKUP_SETDEBUGSTATE;
	return bResult;
}

bool MarkupSTL::Save(const char *szFileName)
{
	// Save document to file
	bool bResult = false;
	FILE *fp = fopen(szFileName, "wb");
	if (fp) {
		// Save string
		int nFileLen = m_strDoc.size();
		if (!nFileLen)
			bResult = true;
		else if (fwrite(m_strDoc.c_str(), nFileLen, 1, fp) == 1)
			bResult = true;
		fclose(fp);
	}
	return bResult;
}

bool MarkupSTL::find_elem(const char *szName)
{
	// Change current position only if found
	//
	if (m_aPos.size()) {
		int iPos = x_FindElem(m_iPosParent, m_iPos, szName);
		if (iPos) {
			// Assign new position
			x_SetPos(m_aPos[iPos].iElemParent, iPos, 0);
			return true;
		}
	}
	return false;
}

bool MarkupSTL::find_child_elem(const char *szName)
{
	// Change current child position only if found
	//
	// Shorthand: call this with no current main position
	// means find child under root element
	if (!m_iPos)
		find_elem();

	int iPosChild = x_FindElem(m_iPos, m_iPosChild, szName);
	if (iPosChild) {
		// Assign new position
		int iPos = m_aPos[iPosChild].iElemParent;
		x_SetPos(m_aPos[iPos].iElemParent, iPos, iPosChild);
		return true;
	}

	return false;
}

//[CMARKUPDEV
int MarkupSTL::find_node(int nType)
{
	// Change current node position only if a node is found
	// If nType is 0 find any node, otherwise find node of type nType
	// Return type of node or 0 if not found
	// If found node is an element, change m_iPos

	// Determine where in document to start scanning for node
	int nTypeFound = 0;
	int nNodeOffset = m_nNodeOffset;
	if (m_nNodeType > 1) {
		// By-pass current node
		nNodeOffset += m_nNodeLength;
	} else {
		// Set position to begin looking for node
		nNodeOffset = 0; // default to start of document
		if (m_iPos) {
			// After element
			nNodeOffset = m_aPos[m_iPos].nEndR + 1;
		} else {
			// Immediately after start tag of parent
			if (m_aPos[m_iPosParent].is_empty_element())
				return 0;
			if (m_iPosParent)
				nNodeOffset = m_aPos[m_iPosParent].nStartR + 1;
		}
	}

	// Get nodes until we find what we're looking for
	int iPosNew = m_iPos;
	TokenPos token(m_strDoc.c_str());
	token.nNext = nNodeOffset;
	do {
		nNodeOffset = token.nNext;
		nTypeFound = x_ParseNode(token);
		if (!nTypeFound)
			return 0;
		if (nTypeFound == MNT_ELEMENT) {
			if (iPosNew)
				iPosNew = m_aPos[iPosNew].iElemNext;
			else
				iPosNew = m_aPos[m_iPosParent].iElemChild;
			if (!iPosNew)
				return 0;
			if (!nType || (nType & nTypeFound)) {
				// Found node, move position to this element
				x_SetPos(m_iPosParent, iPosNew, 0);
				return m_nNodeType;
			}
			token.nNext = m_aPos[iPosNew].nEndR + 1;
		}
	} while (nType && !(nType & nTypeFound));

	m_iPos = iPosNew;
	m_iPosChild = 0;
	m_nNodeOffset = nNodeOffset;
	m_nNodeLength = token.nNext - nNodeOffset;
	m_nNodeType = nTypeFound;
	MARKUP_SETDEBUGSTATE;
	return m_nNodeType;
}

bool MarkupSTL::remove_node()
{
	if (m_iPos || m_nNodeLength) {
		x_RemoveNode(m_iPosParent, m_iPos, m_nNodeType, m_nNodeOffset,
			     m_nNodeLength);
		m_iPosChild = 0;
		MARKUP_SETDEBUGSTATE;
		return true;
	}
	return false;
}
//]CMARKUPDEV

string MarkupSTL::get_tag_name() const
{
	// Return the tag name at the current main position
	string strTagName;

	//[CMARKUPDEV
	// This method is primarily for elements, however
	// it does return something for certain other nodes
	if (m_nNodeLength) {
		switch (m_nNodeType) {
		case MNT_PROCESSING_INSTRUCTION: {
			// Processing instruction returns target
			// For the document version it returns "xml"
			TokenPos token(m_strDoc.c_str());
			token.nNext = m_nNodeOffset + 2;
			if (x_FindToken(token))
				strTagName = x_GetToken(token);
		} break;
		case MNT_COMMENT:
			strTagName = "#comment";
			break;
		case MNT_CDATA_SECTION:
			strTagName = "#cdata-section";
			break;
		case MNT_DOCUMENT_TYPE: {
			// Document type returns document type name
			TokenPos token(m_strDoc.c_str());
			token.nNext = m_nNodeOffset + 2;
			// Find second token
			if (x_FindToken(token) && x_FindToken(token))
				strTagName = x_GetToken(token);
		} break;
		case MNT_TEXT:
		case MNT_WHITESPACE:
			strTagName = "#text";
			break;
		}
		return strTagName;
	}
	//]CMARKUPDEV

	if (m_iPos)
		strTagName = x_GetTagName(m_iPos);
	return strTagName;
}

bool MarkupSTL::into_elem()
{
	// If there is no child position and into_elem is called it will succeed in release 6.3
	// (A subsequent call to find_elem will find the first element)
	// The following short-hand behavior was never part of EDOM and was misleading
	// It would find a child element if there was no current child element position and go into it
	// It is removed in release 6.3, this change is NOT backwards compatible!
	// if ( ! m_iPosChild )
	//	find_child_elem();

	if (m_iPos && m_nNodeType == MNT_ELEMENT) {
		x_SetPos(m_iPos, m_iPosChild, 0);
		return true;
	}
	return false;
}

bool MarkupSTL::out_of_elem()
{
	// Go to parent element
	if (m_iPosParent) {
		x_SetPos(m_aPos[m_iPosParent].iElemParent, m_iPosParent,
			 m_iPos);
		return true;
	}
	return false;
}

string MarkupSTL::get_attrib_name(int n) const
{
	// Return nth attribute name of main position
	if (!m_iPos || m_nNodeType != MNT_ELEMENT)
		return "";

	TokenPos token(m_strDoc.c_str());
	token.nNext = m_aPos[m_iPos].nStartL + 1;
	for (int nAttrib = 0; nAttrib <= n; ++nAttrib)
		if (!x_FindAttrib(token))
			return "";

	// Return substring of document
	return x_GetToken(token);
}

bool MarkupSTL::save_pos(const char *szPosName)
{
	// Save current element position in saved position map
	if (szPosName) {
		SavedPos savedpos;
		savedpos.iPosParent = m_iPosParent;
		savedpos.iPos = m_iPos;
		savedpos.iPosChild = m_iPosChild;
		string strPosName = szPosName;
		m_mapSavedPos[strPosName] = savedpos;
		return true;
	}
	return false;
}

bool MarkupSTL::restore_pos(const char *szPosName)
{
	// Restore element position if found in saved position map
	if (szPosName) {
		string strPosName = szPosName;
		mapSavedPosT::const_iterator iterSavePos =
			m_mapSavedPos.find(strPosName);
		if (iterSavePos != m_mapSavedPos.end()) {
			SavedPos savedpos = (*iterSavePos).second;
			x_SetPos(savedpos.iPosParent, savedpos.iPos,
				 savedpos.iPosChild);
			return true;
		}
	}
	return false;
}

bool MarkupSTL::get_offsets(int &nStart, int &nEnd) const
{
	// Return document offsets of current main position element
	// This is not part of EDOM but is used by the Markup project
	if (m_iPos) {
		nStart = m_aPos[m_iPos].nStartL;
		nEnd = m_aPos[m_iPos].nEndR;
		return true;
	}
	return false;
}

string MarkupSTL::get_child_sub_doc() const
{
	if (m_iPosChild) {
		int nL = m_aPos[m_iPosChild].nStartL;
		int nR = m_aPos[m_iPosChild].nEndR + 1;
		TokenPos token(m_strDoc.c_str());
		token.nNext = nR;
		if (!x_FindToken(token) || m_strDoc[token.nL] == '<')
			nR = token.nL;
		return m_strDoc.substr(nL, nR - nL);
	}
	return "";
}

bool MarkupSTL::remove_elem()
{
	// Remove current main position element
	if (m_iPos && m_nNodeType == MNT_ELEMENT) {
		int iPos = x_RemoveElem(m_iPos);
		x_SetPos(m_iPosParent, iPos, 0);
		return true;
	}
	return false;
}

bool MarkupSTL::remove_child_elem()
{
	// Remove current child position element
	if (m_iPosChild) {
		int iPosChild = x_RemoveElem(m_iPosChild);
		x_SetPos(m_iPosParent, m_iPos, iPosChild);
		return true;
	}
	return false;
}

//[CMARKUPDEV
string MarkupSTL::find_get_data(const char *szName)
{
	if (find_elem(szName))
		return x_GetData(m_iPos);
	return "";
}

bool MarkupSTL::find_set_data(const char *szName, const char *szData,
			      int nCDATA)
{
	if (find_elem(szName))
		return x_SetData(m_iPos, szData, nCDATA);
	return false;
}

// Base64 methods
string MarkupSTL::EncodeBase64(const unsigned char *pBuffer, int nBufferLen)
{
	// Return Base64 string
	// 1 byte takes 2, 2:3, 3:4, 4:6, 5:7, 6:8, 7:10....
	int nLenBase64 = nBufferLen + nBufferLen / 3 + 3;
	nLenBase64 += (nLenBase64 / 64) * 2 + 4; // CRLFs
	string strBase64;
	strBase64.resize(nLenBase64);

	// Start with CRLF
	int nOffsetBase64 = 0;
	strBase64[nOffsetBase64++] = '\r';
	strBase64[nOffsetBase64++] = '\n';

	// Loop through pBuffer 3 bytes at a time
	const char *pCodes =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	int nOffsetBuffer = 0;
	unsigned int n3BufferBytes;
	int nTop;
	int nPad = 0;
	while (nOffsetBuffer < nBufferLen) {
		// Set n3BufferBytes
		n3BufferBytes = pBuffer[nOffsetBuffer++];
		n3BufferBytes = n3BufferBytes << 8;
		if (nOffsetBuffer < nBufferLen)
			n3BufferBytes |= pBuffer[nOffsetBuffer++];
		else
			++nPad;
		n3BufferBytes = n3BufferBytes << 8;
		if (nOffsetBuffer < nBufferLen)
			n3BufferBytes |= pBuffer[nOffsetBuffer++];
		else
			++nPad;

		// Fill strBase64
		nTop = nOffsetBase64 + 4;
		while (nTop != nOffsetBase64) {
			--nTop;
			if (nPad == 0 || nTop < nOffsetBase64 + 4 - nPad)
				strBase64[nTop] = pCodes[n3BufferBytes & 0x3f];
			else
				strBase64[nTop] = '=';
			n3BufferBytes = n3BufferBytes >> 6;
		}
		nOffsetBase64 += 4;

		if (!(nOffsetBase64 % 66) || nPad) {
			strBase64[nOffsetBase64++] = '\r';
			strBase64[nOffsetBase64++] = '\n';
		}
	}

	strBase64.resize(nOffsetBase64);
	return strBase64;
}

int MarkupSTL::DecodeBase64(const string &strBase64, unsigned char *pBuffer,
			    int nBufferLen)
{
	// Return length of binary buffer after decoding
	// If nBufferLen is 0, returns long enough length without decoding
	// so that you can allocate a large enough buffer
	// 1 byte takes 2, 2:3, 3:4, 4:6, 5:7, 6:8, 7:10....
	// Otherwise, returns actual decoded length as long as it fits inside nBufferLen
	int nLenBase64 = strBase64.size();
	if (!nBufferLen)
		return (nLenBase64 / 4) * 3 + 3;

	// Loop through Base64, 4 bytes at a time
	const unsigned char *pBase64 = (const unsigned char *)strBase64.c_str();
	int nOffsetBase64 = 0;
	int nOffsetBuffer = 0;
	unsigned int n3BufferBytes;
	int nTop;
	int nCount;
	int nCode;
	int nPad = 0;
	while (nOffsetBase64 < nLenBase64) {
		// Set n3BufferBytes
		nCount = 0;
		n3BufferBytes = 0;
		while (nCount != 4 && nOffsetBase64 < nLenBase64) {
			// Check encoded character and bypass if whitespace
			nCode = pBase64[nOffsetBase64++];
			if (nCode == '\r' || nCode == '\n')
				continue;

			// Shift previous bits up before OR-ing 6 lower sig bits
			n3BufferBytes = n3BufferBytes << 6;

			// Use ASCII codes for fast conversion
			if (nCode > 96) //(a-z)
				n3BufferBytes |= (nCode - 71);
			else if (nCode > 64) //(A-Z)
				n3BufferBytes |= (nCode - 65);
			else if (nCode == 61) //(=)
				++nPad;
			else if (nCode > 47) //(0-9)
				n3BufferBytes |= (nCode + 4);
			else if (nCode == 47) //(/)
				n3BufferBytes |= 63;
			else if (nCode == 43) //(+)
				n3BufferBytes |= 62;
			++nCount;
		}

		if (nCount == 4) {
			// Fill pBuffer
			nTop = nOffsetBuffer + 3;
			if (nTop - nPad > nBufferLen)
				return 0;
			while (nTop != nOffsetBuffer) {
				--nTop;
				if (nPad == 0 ||
				    nTop < nOffsetBuffer + 3 - nPad)
					pBuffer[nTop] =
						(unsigned char)(n3BufferBytes &
								0xff);
				n3BufferBytes = n3BufferBytes >> 8;
			}
			nOffsetBuffer += 3 - nPad;
		}
	}

	return nOffsetBuffer;
}
//]CMARKUPDEV

//////////////////////////////////////////////////////////////////////
// Private Methods
//////////////////////////////////////////////////////////////////////

int MarkupSTL::x_GetFreePos()
{
	//
	// This returns the index of the next unused ElemPos in the array
	//
	if ((uint32_t)m_iPosFree == m_aPos.size())
		m_aPos.resize(m_iPosFree + m_iPosFree / 2);
	++m_iPosFree;
	return m_iPosFree - 1;
}

int MarkupSTL::x_ReleasePos()
{
	//
	// This decrements the index of the next unused ElemPos in the array
	// allowing the element index returned by GetFreePos() to be reused
	//
	--m_iPosFree;
	return 0;
}

int MarkupSTL::x_ParseError(const char *szError, const char *szName)
{
	if (szName) {
		char szFormat[300];
		snprintf(szFormat, 299, szError, szName);
		m_strError = szFormat;
	} else
		m_strError = szError;
	x_ReleasePos();
	return -1;
}

int MarkupSTL::x_ParseElem(int iPosParent)
{
	// This is either called by set_doc, x_AddSubDoc, or itself recursively
	// m_aPos[iPosParent].nEndL is where to start parsing for the child element
	// This returns the new position if a tag is found, otherwise zero
	// In all cases we need to get a new ElemPos, but release it if unused
	//
	int iPos = x_GetFreePos();
	m_aPos[iPos].nStartL = m_aPos[iPosParent].nEndL;
	m_aPos[iPos].iElemParent = iPosParent;
	m_aPos[iPos].iElemChild = 0;
	m_aPos[iPos].iElemNext = 0;

	// Start get_tag
	// A loop is used to ignore all remarks tags and special tags
	// i.e. <?xml version="1.0"?>, and <!-- comment here -->
	// So any tag beginning with ? or ! is ignored
	// Loop past ignored tags
	TokenPos token(m_strDoc.c_str());
	token.nNext = m_aPos[iPosParent].nEndL;
	string strName;
	while (strName.empty()) {
		// Look for left angle bracket of start tag
		m_aPos[iPos].nStartL = token.nNext;
		if (!x_FindChar(token.szDoc, m_aPos[iPos].nStartL, '<'))
			return x_ParseError("Element tag not found");

		// Set parent's End tag to start looking from here (or later)
		m_aPos[iPosParent].nEndL = m_aPos[iPos].nStartL;

		// Determine whether this is an element, or bypass other type of node
		token.nNext = m_aPos[iPos].nStartL + 1;
		if (x_FindToken(token)) {
			if (token.bIsString)
				return x_ParseError("get_tag starts with quote");
			char cFirstChar = m_strDoc[token.nL];
			if (cFirstChar == '?' || cFirstChar == '!') {
				token.nNext = m_aPos[iPos].nStartL;
				if (!x_ParseNode(token))
					return x_ParseError("Invalid node");
			} else if (cFirstChar != '/') {
				strName = x_GetToken(token);
				// Look for end of tag
				if (!x_FindChar(token.szDoc, token.nNext, '>'))
					return x_ParseError(
						"End of tag not found");
			} else
				return x_ReleasePos(); // probably end tag of parent
		} else
			return x_ParseError("Abrupt end within tag");
	}
	m_aPos[iPos].nStartR = token.nNext;

	// Is ending mark within start tag, i.e. empty element?
	if (m_strDoc[m_aPos[iPos].nStartR - 1] == '/') {
		// Empty element
		// close tag left is set to ending mark, and right to open tag right
		m_aPos[iPos].nEndL = m_aPos[iPos].nStartR - 1;
		m_aPos[iPos].nEndR = m_aPos[iPos].nStartR;
	} else // look for end tag
	{
		// Element probably has contents
		// Determine where to start looking for left angle bracket of end tag
		// This is done by recursively parsing the contents of this element
		int iInner, iInnerPrev = 0;
		m_aPos[iPos].nEndL = m_aPos[iPos].nStartR + 1;
		while ((iInner = x_ParseElem(iPos)) > 0) {
			// Set links to iInner
			if (iInnerPrev)
				m_aPos[iInnerPrev].iElemNext = iInner;
			else
				m_aPos[iPos].iElemChild = iInner;
			iInnerPrev = iInner;

			// Set offset to reflect child
			m_aPos[iPos].nEndL = m_aPos[iInner].nEndR + 1;
		}
		if (iInner == -1)
			return -1;

		// Look for left angle bracket of end tag
		if (!x_FindChar(token.szDoc, m_aPos[iPos].nEndL, '<'))
			return x_ParseError("End tag of %s element not found",
					    strName.c_str());

		// Look through tokens of end tag
		token.nNext = m_aPos[iPos].nEndL + 1;
		int nTokenCount = 0;
		while (x_FindToken(token)) {
			++nTokenCount;
			if (!token.bIsString) {
				// Is first token not an end slash mark?
				if (nTokenCount == 1 &&
				    m_strDoc[token.nL] != '/')
					return x_ParseError(
						"Expecting end tag of element %s",
						strName.c_str());

				else if (nTokenCount == 2 &&
					 !token.Match(strName.c_str()))
					return x_ParseError(
						"End tag does not correspond to %s",
						strName.c_str());

				// Else is it a right angle bracket?
				else if (m_strDoc[token.nL] == '>')
					break;
			}
		}

		// Was a right angle bracket not found?
		if (!token.szDoc[token.nL] || nTokenCount < 2)
			return x_ParseError(
				"End tag not completed for element %s",
				strName.c_str());
		m_aPos[iPos].nEndR = token.nL;
	}

	// Successfully parsed element (and contained elements)
	return iPos;
}

bool MarkupSTL::x_FindChar(const char *szDoc, int &nChar, char c)
{
	// static function
	const char *pChar = &szDoc[nChar];
	while (*pChar && *pChar != c)
		pChar += 1;
	nChar = pChar - szDoc;
	if (!*pChar)
		return false;
	/*
	while ( szDoc[nChar] && szDoc[nChar] != c )
		nChar += _tclen( &szDoc[nChar] );
	if ( ! szDoc[nChar] )
		return false;
	*/
	return true;
}

bool MarkupSTL::x_FindToken(MarkupSTL::TokenPos &token)
{
	// Starting at token.nNext, bypass whitespace and find the next token
	// returns true on success, members of token point to token
	// returns false on end of document, members point to end of document
	const char *szDoc = token.szDoc;
	int nChar = token.nNext;
	token.bIsString = false;

	// By-pass leading whitespace
	while (szDoc[nChar] && strchr(" \t\n\r", szDoc[nChar]))
		++nChar;
	if (!szDoc[nChar]) {
		// No token was found before end of document
		token.nL = nChar;
		token.nR = nChar;
		token.nNext = nChar;
		return false;
	}

	// Is it an opening quote?
	char cFirstChar = szDoc[nChar];
	if (cFirstChar == '\"' || cFirstChar == '\'') {
		token.bIsString = true;

		// Move past opening quote
		++nChar;
		token.nL = nChar;

		// Look for closing quote
		x_FindChar(token.szDoc, nChar, cFirstChar);

		// Set right to before closing quote
		token.nR = nChar - 1;

		// Set nChar past closing quote unless at end of document
		if (szDoc[nChar])
			++nChar;
	} else {
		// Go until special char or whitespace
		token.nL = nChar;
		while (szDoc[nChar] && !strchr(" \t\n\r<>=\\/?!", szDoc[nChar]))
			nChar += 1;

		// Adjust end position if it is one special char
		if (nChar == token.nL)
			++nChar; // it is a special char
		token.nR = nChar - 1;
	}

	// nNext points to one past last char of token
	token.nNext = nChar;
	return true;
}

string MarkupSTL::x_GetToken(const MarkupSTL::TokenPos &token) const
{
	// The token contains indexes into the document identifying a small substring
	// Build the substring from those indexes and return it
	if (token.nL > token.nR)
		return "";
	return m_strDoc.substr(
		token.nL, (token.nR - token.nL +
			   (((uint32_t)(token.nR) < m_strDoc.size()) ? 1 : 0)));
}

int MarkupSTL::x_FindElem(int iPosParent, int iPos, const char *szPath)
{
	// If szPath is NULL or empty, go to next sibling element
	// Otherwise go to next sibling element with matching path
	//
	/*
	if ( iPos )
		iPos = m_aPos[iPos].iElemNext;
	else
		iPos = m_aPos[iPosParent].iElemChild;

	// Finished here if szPath not specified
	if ( szPath == NULL || !szPath[0] )
		return iPos;

	// Search
	TokenPos token( m_strDoc.c_str() );
	while ( iPos )
	{
		// Compare tag name
		token.nNext = m_aPos[iPos].nStartL + 1;
		x_FindToken( token ); // Locate tag name
		if ( token.Match(szPath) )
			return iPos;
		iPos = m_aPos[iPos].iElemNext;
	}
	return 0;
	*/

	//[CMARKUPDEV
	//
	// Example relative paths:
	//
	// "ITEM/QTY" first QTY child element of next ITEM element
	// "ITEM/*" first child element of next ITEM element
	//
	// Example absolute paths:
	//
	// "/"  root
	// "/ORDER"  root must be an ORDER element
	// "/*/ITEM" first ITEM child element of root
	//
	int iPathOffset = 0;
	if (szPath && szPath[0] == '/') {
		// Go to root
		iPos = m_aPos[0].iElemChild;
		iPosParent = 0;
		++iPathOffset;
	} else if (iPos)
		iPos = m_aPos[iPos].iElemNext;
	else
		iPos = m_aPos[iPosParent].iElemChild;

	// Finished here if szPath not specified
	if (szPath == NULL || !szPath[iPathOffset])
		return iPos;

	// Search
	TokenPos token(m_strDoc.c_str());
	while (iPos) {
		// Compare tag name
		token.nNext = m_aPos[iPos].nStartL + 1;
		x_FindToken(token); // Locate tag name
		if (szPath[iPathOffset] == '*') {
			// Wildcard
			if (szPath[iPathOffset + 1] == '/') {
				iPathOffset += 2;
				iPosParent = iPos;
				iPos = m_aPos[iPosParent].iElemChild;
			} else
				return iPos;
		} else if (token.Match(&szPath[iPathOffset])) {
			// Matched tag name
			int nLen = token.nR - token.nL + 1;
			if (szPath[iPathOffset + nLen] == '/') {
				iPathOffset += nLen + 1;
				iPosParent = iPos;
				iPos = m_aPos[iPosParent].iElemChild;
			} else
				return iPos;
		} else
			iPos = m_aPos[iPos].iElemNext;
	}
	return 0;
	//]CMARKUPDEV
}

int MarkupSTL::x_ParseNode(MarkupSTL::TokenPos &token)
{
	// Call this with token.nNext set to the start of the node
	// This returns the node type and token.nNext set to the char after the node
	// If the node is not found or an element, token.nR is not determined
	// White space between elements is a text node
	int nTypeFound = 0;
	const char *szDoc = token.szDoc;
	token.nL = token.nNext;
	if (szDoc[token.nL] == '<') {
		// Started with <, could be:
		// <!--...--> comment
		// <!DOCTYPE ...> dtd
		// <?target ...?> processing instruction
		// <![CDATA[...]]> cdata section
		// <NAME ...> element
		//
		if (!szDoc[token.nL + 1] || !szDoc[token.nL + 2])
			return 0;
		char cFirstChar = szDoc[token.nL + 1];
		const char *szEndOfNode = NULL;
		if (cFirstChar == '?') {
			nTypeFound =
				MNT_PROCESSING_INSTRUCTION; // processing instruction
			szEndOfNode = "?>";
		} else if (cFirstChar == '!') {
			char cSecondChar = szDoc[token.nL + 2];
			if (cSecondChar == '[') {
				nTypeFound = MNT_CDATA_SECTION;
				szEndOfNode = "]]>";
			} else if (cSecondChar == '-') {
				nTypeFound = MNT_COMMENT;
				szEndOfNode = "-->";
			} else {
				// Document type requires tokenizing because of strings and brackets
				nTypeFound = 0;
				int nBrackets = 0;
				while (x_FindToken(token)) {
					if (!token.bIsString) {
						char cChar = szDoc[token.nL];
						if (cChar == '[')
							++nBrackets;
						else if (cChar == ']')
							--nBrackets;
						else if (nBrackets == 0 &&
							 cChar == '>') {
							nTypeFound =
								MNT_DOCUMENT_TYPE;
							break;
						}
					}
				}
				if (!nTypeFound)
					return 0;
			}
		} else if (cFirstChar == '/') {
			// End tag means no node found within parent element
			return 0;
		} else {
			nTypeFound = MNT_ELEMENT;
		}

		// Search for end of node if not found yet
		if (szEndOfNode) {
			const char *pEnd =
				strstr(&szDoc[token.nNext], szEndOfNode);
			if (!pEnd)
				return 0; // not well-formed
			token.nNext = (pEnd - szDoc) + strlen(szEndOfNode);
		}
	} else if (szDoc[token.nL]) {
		// It is text or whitespace because it did not start with <
		nTypeFound = MNT_WHITESPACE;
		if (x_FindToken(token)) {
			if (szDoc[token.nL] == '<')
				token.nNext = token.nL;
			else {
				nTypeFound = MNT_TEXT;
				x_FindChar(token.szDoc, token.nNext, '<');
			}
		}
	}
	return nTypeFound;
}

string MarkupSTL::x_GetTagName(int iPos) const
{
	// Return the tag name at specified element
	TokenPos token(m_strDoc.c_str());
	token.nNext = m_aPos[iPos].nStartL + 1;
	if (!iPos || !x_FindToken(token))
		return "";

	// Return substring of document
	return x_GetToken(token);
}

bool MarkupSTL::x_FindAttrib(MarkupSTL::TokenPos &token,
			     const char *szAttrib) const
{
	// If szAttrib is NULL find next attrib, otherwise find named attrib
	// Return true if found
	int nAttrib = 0;
	for (int nCount = 0; x_FindToken(token); ++nCount) {
		if (!token.bIsString) {
			// Is it the right angle bracket?
			if (m_strDoc[token.nL] == '>' ||
			    m_strDoc[token.nL] == '/')
				break; // attrib not found

			// Equal sign
			if (m_strDoc[token.nL] == '=')
				continue;

			// Potential attribute
			if (!nAttrib && nCount) {
				// Attribute name search?
				if (!szAttrib || !szAttrib[0])
					return true; // return with token at attrib name

				// Compare szAttrib
				if (token.Match(szAttrib))
					nAttrib = nCount;
			}
		} else if (nAttrib && nCount == nAttrib + 2) {
			return true;
		}
	}

	// Not found
	return false;
}

string MarkupSTL::x_GetAttrib(int iPos, const char *szAttrib) const
{
	// Return the value of the attrib at specified element
	if (!iPos || m_nNodeType != MNT_ELEMENT)
		return "";

	TokenPos token(m_strDoc.c_str());
	token.nNext = m_aPos[iPos].nStartL + 1;
	if (szAttrib && x_FindAttrib(token, szAttrib))
		return x_TextFromDoc(
			token.nL,
			token.nR - (((uint32_t)(token.nR) < m_strDoc.size()) ?
					    0 :
					    1));
	return "";
}

bool MarkupSTL::x_SetAttrib(int iPos, const char *szAttrib, int nValue)
{
	// Convert integer to string and call set_child_attrib
	char szVal[25];
	snprintf(szVal, 24, "%d", nValue);
	return x_SetAttrib(iPos, szAttrib, szVal);
}

bool MarkupSTL::x_SetAttrib(int iPos, const char *szAttrib, const char *szValue)
{
	// Set attribute in iPos element
	if (!iPos || m_nNodeType != MNT_ELEMENT)
		return false;

	TokenPos token(m_strDoc.c_str());
	token.nNext = m_aPos[iPos].nStartL + 1;
	int nInsertAt, nReplace = 0;
	string strInsert;
	if (x_FindAttrib(token, szAttrib)) {
		// Decision: for empty value leaving attrib="" instead of removing attrib
		// Replace value only
		strInsert = x_TextToDoc(szValue, true);
		nInsertAt = token.nL;
		nReplace = token.nR - token.nL + 1;
	} else {
		// Insert string name value pair
		string strFormat;
		strFormat = " ";
		strFormat += szAttrib;
		strFormat += "=\"";
		strFormat += x_TextToDoc(szValue, true);
		strFormat += "\"";
		strInsert = strFormat;

		// take into account whether it is an empty element
		nInsertAt = m_aPos[iPos].nStartR -
			    (m_aPos[iPos].is_empty_element() ? 1 : 0);
	}

	x_DocChange(nInsertAt, nReplace, strInsert);
	int nAdjust = strInsert.size() - nReplace;
	m_aPos[iPos].nStartR += nAdjust;
	m_aPos[iPos].adjust_end(nAdjust);
	x_Adjust(iPos, nAdjust);
	MARKUP_SETDEBUGSTATE;
	return true;
}

//[CMARKUPDEV
bool MarkupSTL::x_RemoveAttrib(int iPos, const char *szAttrib)
{
	// Find attribute in iPos element and remove it
	if (iPos && m_nNodeType == MNT_ELEMENT) {
		TokenPos token(m_strDoc.c_str());
		token.nNext = m_aPos[iPos].nStartL + 1;
		while (x_FindAttrib(token)) {
			// Compare szAttrib
			if (token.Match(szAttrib)) {
				int nInsertAt =
					token.nL - 1; // preceding whitespace
				if (x_FindToken(token) &&
				    m_strDoc[token.nL] == '=' &&
				    x_FindToken(token) && token.bIsString) {
					int nReplace = token.nR - nInsertAt + 2;
					x_DocChange(nInsertAt, nReplace, "");
					m_aPos[iPos].nStartR -= nReplace;
					m_aPos[iPos].adjust_end(-nReplace);
					x_Adjust(iPos, -nReplace);
					MARKUP_SETDEBUGSTATE;
					return true;
				}
			}
		}
	}
	return false;
}
//]CMARKUPDEV

bool MarkupSTL::x_CreateNode(string &strNode, int nNodeType, const char *szText)
{
	// Set strNode based on nNodeType and szText
	// Return false if szText would jeopardize well-formed document
	//
	switch (nNodeType) {
	case MNT_CDATA_SECTION:
		if (strstr(szText, "]]>") != NULL)
			return false;
		strNode = "<![CDATA[";
		strNode += szText;
		strNode += "]]>";
		break;
	//[CMARKUPDEV
	case MNT_PROCESSING_INSTRUCTION:
		strNode = "<?";
		strNode += szText;
		strNode += "?>";
		break;
	case MNT_COMMENT:
		strNode = "<!--";
		strNode += szText;
		strNode += "-->";
		break;
	case MNT_ELEMENT:
		strNode = "<";
		strNode += szText;
		strNode += "/>";
		break;
	case MNT_TEXT:
	case MNT_WHITESPACE:
		strNode = x_TextToDoc(szText);
		break;
	case MNT_DOCUMENT_TYPE:
		strNode = szText;
		break;
		//]CMARKUPDEV
	}
	return true;
}

bool MarkupSTL::x_SetData(int iPos, const char *szData, int nCDATA)
{
	// Set data at specified position
	// if nCDATA==1, set content of element to a CDATA Section
	string strInsert;

	//[CMARKUPDEV
	if (iPos == m_iPos && m_nNodeLength) {
		// Not an element
		if (!x_CreateNode(strInsert, m_nNodeType, szData))
			return false;
		x_DocChange(m_nNodeOffset, m_nNodeLength, strInsert);
		x_AdjustForNode(m_iPosParent, iPos,
				strInsert.size() - m_nNodeLength);
		m_nNodeLength = strInsert.size();
		MARKUP_SETDEBUGSTATE;
		return true;
	}
	//]CMARKUPDEV

	// Set data in iPos element
	if (!iPos || m_aPos[iPos].iElemChild)
		return false;

	// Build strInsert from szData based on nCDATA
	// If CDATA section not valid, use parsed text (PCDATA) instead
	if (nCDATA != 0)
		if (!x_CreateNode(strInsert, MNT_CDATA_SECTION, szData))
			nCDATA = 0;
	if (nCDATA == 0)
		strInsert = x_TextToDoc(szData);

	// Decide where to insert
	int nInsertAt, nReplace;
	if (m_aPos[iPos].is_empty_element()) {
		nInsertAt = m_aPos[iPos].nEndL;
		nReplace = 1;

		// Pre-adjust since <NAME/> becomes <NAME>data</NAME>
		string strTagName = x_GetTagName(iPos);
		m_aPos[iPos].nStartR -= 1;
		m_aPos[iPos].nEndL -= (1 + strTagName.size());
		string strFormat;
		strFormat = ">";
		strFormat += strInsert;
		strFormat += "</";
		strFormat += strTagName;
		strInsert = strFormat;
	} else {
		nInsertAt = m_aPos[iPos].nStartR + 1;
		nReplace = m_aPos[iPos].nEndL - m_aPos[iPos].nStartR - 1;
	}
	x_DocChange(nInsertAt, nReplace, strInsert);
	int nAdjust = strInsert.size() - nReplace;
	x_Adjust(iPos, nAdjust);
	m_aPos[iPos].adjust_end(nAdjust);
	MARKUP_SETDEBUGSTATE;
	return true;
}

string MarkupSTL::x_GetData(int iPos) const
{
	//[CMARKUPDEV
	if (iPos == m_iPos && m_nNodeLength) {
		if (m_nNodeType == MNT_COMMENT)
			return m_strDoc.substr(m_nNodeOffset + 4,
					       m_nNodeLength - 7);
		else if (m_nNodeType == MNT_PROCESSING_INSTRUCTION)
			return m_strDoc.substr(m_nNodeOffset + 2,
					       m_nNodeLength - 4);
		else if (m_nNodeType == MNT_CDATA_SECTION)
			return m_strDoc.substr(m_nNodeOffset + 8,
					       m_nNodeLength - 11);
		else if (m_nNodeType == MNT_TEXT)
			return x_TextFromDoc(m_nNodeOffset,
					     m_nNodeOffset + m_nNodeLength - 1);
		else
			return m_strDoc.substr(m_nNodeOffset, m_nNodeLength);
	}
	//]CMARKUPDEV

	// Return a string representing data between start and end tag
	// Return empty string if there are any children elements
	if (!m_aPos[iPos].iElemChild && !m_aPos[iPos].is_empty_element()) {
		// See if it is a CDATA section
		TokenPos token(m_strDoc.c_str());
		token.nNext = m_aPos[iPos].nStartR + 1;
		if (x_FindToken(token) && m_strDoc[token.nL] == '<' &&
		    token.nL + 11 < m_aPos[iPos].nEndL &&
		    strncmp(&token.szDoc[token.nL + 1], "![CDATA[", 8) == 0) {
			int nEndCDATA = m_strDoc.find("]]>", token.nNext);
			if (nEndCDATA != (int32_t)(string::npos) &&
			    nEndCDATA < m_aPos[iPos].nEndL) {
				return m_strDoc.substr(
					token.nL + 9, nEndCDATA - token.nL - 9);
			}
		}
		return x_TextFromDoc(m_aPos[iPos].nStartR + 1,
				     m_aPos[iPos].nEndL - 1);
	}
	return "";
}

string MarkupSTL::x_TextToDoc(const char *szText, bool bAttrib) const
{
	//
	// &lt;   less than
	// &amp;  ampersand
	// &gt;   greater than
	//
	// and for attributes:
	//
	// &apos; apostrophe or single quote
	// &quot; double quote
	//
	static const char *szaReplace[] = { "&lt;", "&amp;", "&gt;", "&apos;",
					    "&quot;" };
	const char *pFind = bAttrib ? "<&>\'\"" : "<&>";
	const char *pSource = szText;
	string strResult;
	int nLen = strlen(szText);
	strResult.reserve(nLen + nLen / 10);
	char cSource = *pSource;
	char *pFound;
	while (cSource) {
		if ((pFound = (char *)strchr(pFind, cSource)) != NULL) {
			pFound = (char *)szaReplace[pFound - pFind];
			strResult.append(pFound);
		} else {
			strResult += cSource;
		}
		cSource = *(++pSource);
	}
	return strResult;
}

string MarkupSTL::x_TextFromDoc(int nLeft, int nRight) const
{
	//
	// Conveniently the result is always the same or shorter in length
	//
	static const char *szaCode[] = { "lt;", "amp;", "gt;", "apos;",
					 "quot;" };
	static int anCodeLen[] = { 3, 4, 3, 5, 5 };
	static const char *szSymbol = "<&>\'\"";
	string strResult;
	strResult.reserve(nRight - nLeft + 1);
	const char *pSource = m_strDoc.c_str();
	int nChar = nLeft;
	char cSource = pSource[nChar];
	while (nChar <= nRight) {
		if (cSource == '&') {
			// If no match is found it means XML doc is invalid
			// no devastating harm done, ampersand code will just be left in result
			for (int nMatch = 0; nMatch < 5; ++nMatch) {
				if (nChar <= nRight - anCodeLen[nMatch] &&
				    strncmp(szaCode[nMatch],
					    &pSource[nChar + 1],
					    anCodeLen[nMatch]) == 0) {
					cSource = szSymbol[nMatch];
					nChar += anCodeLen[nMatch];
					break;
				}
			}
		}
		strResult += cSource;
		nChar++;
		cSource = pSource[nChar];
	}
	return strResult;
}

void MarkupSTL::x_DocChange(int nLeft, int nReplace, const string &strInsert)
{
	// Insert strInsert int m_strDoc at nLeft replacing nReplace chars
	//
	//[CMARKUPDEV
	// When creating a document, reduce reallocs by reserving string space
	// Allow for 1.5 times the current allocation, with minimum of 200
	int nNewLength = strInsert.size() + m_strDoc.size() - nReplace;
	int nAllocLen = m_strDoc.capacity();
	if (nNewLength > nAllocLen) {
		int nReserve = nAllocLen + nAllocLen / 2;
		if (nReserve < nNewLength)
			nReserve = nNewLength;
		if (nReserve < 200)
			nReserve = 200;
		m_strDoc.reserve(nReserve);
	}
	//]CMARKUPDEV
	m_strDoc.replace(nLeft, nReplace, strInsert);
}

void MarkupSTL::x_Adjust(int iPos, int nShift, bool bAfterPos)
{
	// Loop through affected elements and adjust indexes
	// Algorithm:
	// 1. update children unless bAfterPos
	//    (if no children or bAfterPos is true, end tag of iPos not affected)
	// 2. update next siblings and their children
	// 3. go up until there is a next sibling of a parent and update end tags
	// 4. step 2
	int iPosTop = m_aPos[iPos].iElemParent;
	bool bPosFirst = bAfterPos; // mark as first to skip its children
	while (iPos) {
		// Were we at containing parent of affected position?
		bool bPosTop = false;
		if (iPos == iPosTop) {
			// Move iPosTop up one towards root
			iPosTop = m_aPos[iPos].iElemParent;
			bPosTop = true;
		}

		// Traverse to the next update position
		if (!bPosTop && !bPosFirst && m_aPos[iPos].iElemChild) {
			// Depth first
			iPos = m_aPos[iPos].iElemChild;
		} else if (m_aPos[iPos].iElemNext) {
			iPos = m_aPos[iPos].iElemNext;
		} else {
			// Look for next sibling of a parent of iPos
			// When going back up, parents have already been done except iPosTop
			while ((iPos = m_aPos[iPos].iElemParent) != 0 &&
			       iPos != iPosTop)
				if (m_aPos[iPos].iElemNext) {
					iPos = m_aPos[iPos].iElemNext;
					break;
				}
		}
		bPosFirst = false;

		// Shift indexes at iPos
		if (iPos != iPosTop)
			m_aPos[iPos].adjust_start(nShift);
		m_aPos[iPos].adjust_end(nShift);
	}
}

void MarkupSTL::x_LocateNew(int iPosParent, int &iPosRel, int &nOffset,
			    int nLength, int nFlags)
{
	// Determine where to insert new element or node
	//
	bool bInsert = (nFlags & 1) ? true : false;
	bool bHonorWhitespace = (nFlags & 2) ? true : false;

	int nStartL;
	if (nLength) {
		// Located at a non-element node
		if (bInsert)
			nStartL = nOffset;
		else
			nStartL = nOffset + nLength;
	} else if (iPosRel) {
		// Located at an element
		if (bInsert) // precede iPosRel
			nStartL = m_aPos[iPosRel].nStartL;
		else // follow iPosRel
			nStartL = m_aPos[iPosRel].nEndR + 1;
	} else if (m_aPos[iPosParent].is_empty_element()) {
		// Parent has no separate end tag, so split empty element
		nStartL = m_aPos[iPosParent].nStartR;
	} else {
		if (bInsert) // after start tag
			nStartL = m_aPos[iPosParent].nStartR + 1;
		else // before end tag
			nStartL = m_aPos[iPosParent].nEndL;
	}

	// Go up to start of next node, unless its splitting an empty element
	if (!bHonorWhitespace && !m_aPos[iPosParent].is_empty_element()) {
		TokenPos token(m_strDoc.c_str());
		token.nNext = nStartL;
		if (!x_FindToken(token) || m_strDoc[token.nL] == '<')
			nStartL = token.nL;
	}

	// Determine iPosBefore
	int iPosBefore = 0;
	if (iPosRel) {
		if (bInsert) {
			// Is iPosRel past first sibling?
			int iPosPrev = m_aPos[iPosParent].iElemChild;
			if (iPosPrev != iPosRel) {
				// Find previous sibling of iPosRel
				while (m_aPos[iPosPrev].iElemNext != iPosRel)
					iPosPrev = m_aPos[iPosPrev].iElemNext;
				iPosBefore = iPosPrev;
			}
		} else {
			iPosBefore = iPosRel;
		}
	} else if (m_aPos[iPosParent].iElemChild) {
		if (!bInsert) {
			// Find last element under iPosParent
			int iPosLast = m_aPos[iPosParent].iElemChild;
			int iPosNext = iPosLast;
			while (iPosNext) {
				iPosLast = iPosNext;
				iPosNext = m_aPos[iPosNext].iElemNext;
			}
			iPosBefore = iPosLast;
		}
	}

	nOffset = nStartL;
	iPosRel = iPosBefore;
}

bool MarkupSTL::x_AddElem(const char *szName, const char *szValue, bool bInsert,
			  bool bAddChild)
{
	if (bAddChild) {
		// Adding a child element under main position
		if (!m_iPos)
			return false;
	} else if (m_iPosParent == 0) {
		// Adding root element
		if (is_well_formed())
			return false;

//[CMARKUPDEV
#if defined(x_VERSIONTAG)
		if (m_strDoc.is_empty())
			m_strDoc = x_VERSIONTAG;
#endif
		//]CMARKUPDEV

		// Locate after any version and DTD
		m_aPos[0].nEndL = m_strDoc.size();
	}

	// Locate where to add element relative to current node
	int iPosParent, iPosBefore, nOffset = 0, nLength = 0;
	if (bAddChild) {
		iPosParent = m_iPos;
		iPosBefore = m_iPosChild;
	} else {
		iPosParent = m_iPosParent;
		iPosBefore = m_iPos;
		//[CMARKUPDEV
		nOffset = m_nNodeOffset;
		nLength = m_nNodeLength;
		//]CMARKUPDEV
	}
	int nFlags = bInsert ? 1 : 0;
	x_LocateNew(iPosParent, iPosBefore, nOffset, nLength, nFlags);
	bool bEmptyParent = m_aPos[iPosParent].is_empty_element();
	if (bEmptyParent)
		nOffset += 2; // include CRLF

	// Create element and modify positions of affected elements
	// If no szValue is specified, an empty element is created
	// i.e. either <NAME>value</NAME> or <NAME/>
	//
	int iPos = x_GetFreePos();
	m_aPos[iPos].nStartL = nOffset;

	// Set links
	m_aPos[iPos].iElemParent = iPosParent;
	m_aPos[iPos].iElemChild = 0;
	m_aPos[iPos].iElemNext = 0;
	if (iPosBefore) {
		// Link in after iPosBefore
		m_aPos[iPos].iElemNext = m_aPos[iPosBefore].iElemNext;
		m_aPos[iPosBefore].iElemNext = iPos;
	} else {
		// First child
		m_aPos[iPos].iElemNext = m_aPos[iPosParent].iElemChild;
		m_aPos[iPosParent].iElemChild = iPos;
	}

	// Create string for insert
	string strInsert;
	int nLenName = strlen(szName);
	int nLenValue = szValue ? strlen(szValue) : 0;
	if (!nLenValue) {
		// <NAME/> empty element
		strInsert = "<";
		strInsert += szName;
		strInsert += "/>\r\n";
		m_aPos[iPos].nStartR = m_aPos[iPos].nStartL + nLenName + 2;
		m_aPos[iPos].nEndL = m_aPos[iPos].nStartR - 1;
		m_aPos[iPos].nEndR = m_aPos[iPos].nEndL + 1;
	} else {
		// <NAME>value</NAME>
		string strValue = x_TextToDoc(szValue);
		nLenValue = strValue.size();
		strInsert = "<";
		strInsert += szName;
		strInsert += ">";
		strInsert += strValue;
		strInsert += "</";
		strInsert += szName;
		strInsert += ">\r\n";
		m_aPos[iPos].nStartR = m_aPos[iPos].nStartL + nLenName + 1;
		m_aPos[iPos].nEndL = m_aPos[iPos].nStartR + nLenValue + 1;
		m_aPos[iPos].nEndR = m_aPos[iPos].nEndL + nLenName + 2;
	}

	// Insert
	int nReplace = 0, nLeft = m_aPos[iPos].nStartL;
	if (bEmptyParent) {
		string strParentTagName = x_GetTagName(iPosParent);
		string strFormat;
		strFormat = ">\r\n";
		strFormat += strInsert;
		strFormat += "</";
		strFormat += strParentTagName;
		strInsert = strFormat;
		nLeft -= 3;
		nReplace = 1;
		// x_Adjust is going to update all affected indexes by one amount
		// This will satisfy all except the empty parent
		// Here we pre-adjust for the empty parent
		// The empty tag slash is removed
		m_aPos[iPosParent].nStartR -= 1;
		// For the newly created end tag, see the following example:
		// <A/> (len 4) becomes <A><B/></A> (len 11)
		// In x_Adjust everything will be adjusted 11 - 4 = 7
		// But the nEndL of element A should only be adjusted 5
		m_aPos[iPosParent].nEndL -= (strParentTagName.size() + 1);
	}
	x_DocChange(nLeft, nReplace, strInsert);
	x_Adjust(iPos, strInsert.size() - nReplace);

	if (bAddChild)
		x_SetPos(m_iPosParent, iPosParent, iPos);
	else
		x_SetPos(iPosParent, iPos, 0);
	return true;
}

bool MarkupSTL::x_AddSubDoc(const char *szSubDoc, bool bInsert, bool bAddChild)
{
	// Add subdocument, parse, and modify positions of affected elements
	//
	int nOffset = 0, iPosParent, iPosBefore;
	if (bAddChild) {
		// Add a subdocument under main position, after current child position
		if (!m_iPos)
			return false;
		iPosParent = m_iPos;
		iPosBefore = m_iPosChild;
	} else {
		iPosParent = m_iPosParent;
		iPosBefore = m_iPos;
	}
	int nFlags = bInsert ? 1 : 0;
	x_LocateNew(iPosParent, iPosBefore, nOffset, 0, nFlags);
	bool bEmptyParent = m_aPos[iPosParent].is_empty_element();
	if (bEmptyParent)
		nOffset += 2; // include CRLF

	// if iPosBefore is NULL, insert as first element under parent
	int nParentEndLBeforeAdd = m_aPos[iPosParent].nEndL;
	int iPosFreeBeforeAdd = m_iPosFree;

	// Skip version tag or DTD at start of subdocument
	TokenPos token(szSubDoc);
	int nNodeType = x_ParseNode(token);
	while (nNodeType && nNodeType != MNT_ELEMENT) {
		token.szDoc = &szSubDoc[token.nNext];
		token.nNext = 0;
		nNodeType = x_ParseNode(token);
	}
	string strInsert = token.szDoc;

	// Insert subdocument
	m_aPos[iPosParent].nEndL = nOffset;
	int nReplace = 0, nLeft = nOffset;
	string strParentTagName;
	if (bEmptyParent) {
		strParentTagName = x_GetTagName(iPosParent);
		string strFormat;
		strFormat = ">\r\n";
		strFormat += strInsert;
		strFormat += "</";
		strFormat += strParentTagName;
		strInsert = strFormat;
		m_aPos[iPosParent].nEndL = m_aPos[iPosParent].nStartR + 2;
		nLeft = m_aPos[iPosParent].nStartR - 1;
		nReplace = 1;
	}
	x_DocChange(nLeft, nReplace, strInsert);

	// Parse subdocument
	int iPos = x_ParseElem(iPosParent);
	m_aPos[iPosParent].nEndL = nParentEndLBeforeAdd;
	if (iPos <= 0) {
		// Abort because not well-formed
		string strRevert = bEmptyParent ? "/" : "";
		x_DocChange(nLeft, strInsert.size(), strRevert);
		m_iPosFree = iPosFreeBeforeAdd;
		return false;
	} else {
		// Link in parent and siblings
		m_aPos[iPos].iElemParent = iPosParent;
		if (iPosBefore) {
			m_aPos[iPos].iElemNext = m_aPos[iPosBefore].iElemNext;
			m_aPos[iPosBefore].iElemNext = iPos;
		} else {
			m_aPos[iPos].iElemNext = m_aPos[iPosParent].iElemChild;
			m_aPos[iPosParent].iElemChild = iPos;
		}

		// Make empty parent pre-adjustment
		if (bEmptyParent) {
			m_aPos[iPosParent].nStartR -= 1;
			m_aPos[iPosParent].nEndL -=
				(strParentTagName.size() + 1);
		}

		// Adjust, but don't adjust children of iPos (bAfterPos=true)
		x_Adjust(iPos, strInsert.size() - nReplace, true);
	}

	// Set position to top element of subdocument
	if (bAddChild)
		x_SetPos(m_iPosParent, iPosParent, iPos);
	else // Main
		x_SetPos(m_iPosParent, iPos, 0);
	return true;
}

int MarkupSTL::x_RemoveElem(int iPos)
{
	// Remove element and all contained elements
	// Return new position
	//
	int iPosParent = m_aPos[iPos].iElemParent;

	// Find previous sibling and bypass removed element
	// This leaves orphan positions in m_aPos array
	int iPosLook = m_aPos[iPosParent].iElemChild;
	int iPosPrev = 0;
	while (iPosLook != iPos) {
		iPosPrev = iPosLook;
		iPosLook = m_aPos[iPosLook].iElemNext;
	}
	if (iPosPrev)
		m_aPos[iPosPrev].iElemNext = m_aPos[iPos].iElemNext;
	else
		m_aPos[iPosParent].iElemChild = m_aPos[iPos].iElemNext;

	// Remove from document
	// Links have been changed to go around removed element
	// But element position and links are still valid
	int nAfterEnd = m_aPos[iPos].nEndR + 1;
	TokenPos token(m_strDoc.c_str());
	token.nNext = nAfterEnd;
	if (!x_FindToken(token) || token.szDoc[token.nL] == '<')
		nAfterEnd = token.nL;
	int nLen = nAfterEnd - m_aPos[iPos].nStartL;
	x_DocChange(m_aPos[iPos].nStartL, nLen, string());
	x_Adjust(iPos, -nLen, true);
	return iPosPrev;
}

//[CMARKUPDEV
void MarkupSTL::x_AdjustForNode(int iPosParent, int iPos, int nShift)
{
	// Adjust affected indexes
	if (!iPos) {
		// Adjust end tag of parent, and adjust from there
		int iPosChild = m_aPos[iPosParent].iElemChild;
		if (!iPosParent) {
			if (!iPosChild)
				return; // no elements in this document yet

			// Change happened before root element
			m_aPos[iPosChild].adjust_start(nShift);
			iPos = m_aPos[iPosChild].iElemChild;
			if (iPos)
				m_aPos[iPos].adjust_start(nShift);
			else
				iPos = iPosChild;
		} else {
			// Change happened before or at first element under iPosParent
			iPos = iPosChild;
			if (iPos)
				m_aPos[iPos].adjust_start(nShift);
			else
				iPos = iPosParent;
		}
		m_aPos[iPos].adjust_end(nShift);
	}
	x_Adjust(iPos, nShift, true);
}

bool MarkupSTL::x_AddNode(int nNodeType, const char *szText, bool bInsert)
{
	// Add node of nNodeType after current node position
	string strInsert;
	if (!x_CreateNode(strInsert, nNodeType, szText))
		return false;

	// Only comments, DTDs, and processing instructions are followed by CRLF
	// Other nodes are usually concerned with mixed content, so no CRLF
	bool bNewline = false;
	if (nNodeType == MNT_PROCESSING_INSTRUCTION ||
	    nNodeType == MNT_COMMENT || nNodeType == MNT_DOCUMENT_TYPE)
		bNewline = true;

	// Locate where to add node relative to current node
	int iPosBefore = m_iPos;
	int iPosParent = m_iPosParent;
	int nNodeOffset = m_nNodeOffset;
	int nNodeLength = m_nNodeLength;
	int nFlags = bNewline ? 0 : 2;
	if (bInsert)
		nFlags |= 1;
	x_LocateNew(iPosParent, iPosBefore, nNodeOffset, nNodeLength, nFlags);
	bool bEmptyParent = m_aPos[iPosParent].is_empty_element();
	int nInsertAt = nNodeOffset;

	// Get node length before adding CRLF
	nNodeLength = strInsert.size();
	if (bNewline)
		strInsert += "\r\n";

	// If its a new element, link it in
	int iPos = iPosBefore;
	if (nNodeType == MNT_ELEMENT) {
		// Set indexes
		iPos = x_GetFreePos();
		m_aPos[iPos].nStartL = nNodeOffset;
		m_aPos[iPos].nStartR = nNodeOffset + nNodeLength - 1;
		m_aPos[iPos].nEndL = m_aPos[iPos].nStartR - 1;
		m_aPos[iPos].nEndR = m_aPos[iPos].nStartR;
		nNodeOffset = 0;
		nNodeLength = 0;

		// Set links
		m_aPos[iPos].iElemParent = iPosParent;
		m_aPos[iPos].iElemChild = 0;
		m_aPos[iPos].iElemNext = 0;
		if (iPosBefore) {
			// Link in after iPosBefore
			m_aPos[iPos].iElemNext = m_aPos[iPosBefore].iElemNext;
			m_aPos[iPosBefore].iElemNext = iPos;
		} else {
			// First child
			m_aPos[iPos].iElemNext = m_aPos[iPosParent].iElemChild;
			m_aPos[iPosParent].iElemChild = iPos;
		}
	}

	// Insert
	int nReplace = 0;
	if (bEmptyParent) {
		nReplace = 1;
		--nInsertAt;
		string strParentTagName = x_GetTagName(iPosParent);
		string strFormat;
		strFormat = ">";
		if (bNewline)
			strFormat += "\r\n";
		strFormat += strInsert;
		strFormat += "</";
		strFormat += strParentTagName;
		strInsert = strFormat;
		m_aPos[iPosParent].nStartR -= 1;
		m_aPos[iPosParent].nEndL -= (strParentTagName.size() + 1);
	}
	x_DocChange(nInsertAt, nReplace, strInsert);

	// need to adjust element positions after iPos
	x_AdjustForNode(iPosParent, iPos, strInsert.size() - nReplace);

	// Set current position
	m_iPos = iPos;
	m_iPosChild = 0;
	m_nNodeOffset = nNodeOffset;
	m_nNodeLength = nNodeLength;
	m_nNodeType = nNodeType;
	MARKUP_SETDEBUGSTATE;
	return true;
}

void MarkupSTL::x_RemoveNode(int iPosParent, int &iPos, int &nNodeType,
			     int &nNodeOffset, int &nNodeLength)
{
	// Remove node and return new position
	//
	int iPosPrev = iPos;

	// Removing an element?
	if (nNodeType == MNT_ELEMENT) {
		nNodeOffset = m_aPos[iPos].nStartL;
		nNodeLength = m_aPos[iPos].nEndR - m_aPos[iPos].nStartL + 1;

		// Deleting iPos element, so find previous element if any
		iPosPrev = m_aPos[iPosParent].iElemChild;
		if (iPosPrev == iPos) {
			iPosPrev = 0;
			m_aPos[iPosParent].iElemChild = m_aPos[iPos].iElemNext;
		} else {
			while (m_aPos[iPosPrev].iElemNext != iPos)
				iPosPrev = m_aPos[iPosPrev].iElemNext;
			m_aPos[iPosPrev].iElemNext = m_aPos[iPos].iElemNext;
		}
	}

	// Find previous node type, offset and length
	int nPrevOffset = 0;
	if (iPosPrev)
		nPrevOffset = m_aPos[iPosPrev].nEndR + 1;
	else if (iPosParent)
		nPrevOffset = m_aPos[iPosParent].nStartR + 1;
	TokenPos token(m_strDoc.c_str());
	token.nNext = nPrevOffset;
	int nPrevType = 0;
	while (token.nNext < nNodeOffset) {
		nPrevOffset = token.nNext;
		nPrevType = x_ParseNode(token);
	}
	int nPrevLength = nNodeOffset - nPrevOffset;
	if (!nPrevLength) {
		// Previous node is iPosPrev element
		nPrevOffset = 0;
		if (iPosPrev)
			nPrevType = MNT_ELEMENT;
	}

	// Remove node from document
	x_DocChange(nNodeOffset, nNodeLength, string());
	x_AdjustForNode(iPosParent, iPos, -nNodeLength);

	nNodeType = nPrevType;
	nNodeOffset = nPrevOffset;
	nNodeLength = nPrevLength;
	iPos = iPosPrev;
}
//]CMARKUPDEV
