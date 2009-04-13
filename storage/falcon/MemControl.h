/* Copyright (C) 2007 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


#ifndef _MEMCONTROL_H_
#define _MEMCONTROL_H_

class MemMgr;

class MemControl
{
public:
	MemControl(void);
	virtual ~MemControl(void);
	
	MemMgr	*pools[5];
	int		count;
	uint64	maxMemory;
	virtual bool poolExtensionCheck(uint size);
	virtual uint64 getCurrentMemory(int poolMask = MemMgrAllPools);
	void addPool(MemMgr* pool);
	void setMaxSize(uint64 size);
	void setMaxSize(int mgrId, uint64 size);
};

#endif
