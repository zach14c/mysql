/* Copyright 2009 Sun Microsystems, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA 
*/

#ifndef SRL_TABLESPACES_H
#define SRL_TABLESPACES_H

#include "SerialLogRecord.h"
class TableSpaceManager;

class SRLTableSpaces : public SerialLogRecord
{
public:
	virtual void print();
	void append(TableSpaceManager *manager);
	virtual void read();
	SRLTableSpaces();
	virtual ~SRLTableSpaces();

	// Return the most recent tablespace list 
	// found in serialLog
	static TableSpaceManager *getTableSpaceManager();
};

#endif
