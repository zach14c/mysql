/* Copyright (C) 2008 MySQL AB

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

#ifndef _SECTOR_CACHE_H_
#define _SECTOR_CACHE_H_

#include "SyncObject.h"

static const int SECTOR_BUFFER_SIZE		= 65536;
static const int SECTOR_HASH_SIZE		= 1024;

class SectorBuffer;
class Dbb;
class Bdb;

class SectorCache
{
public:
	SectorCache(int numberBuffers, int pageSize);
	~SectorCache(void);
	
	void readPage(Bdb* bdb);

	SyncObject		syncObject;
	SectorBuffer	*buffers;
	SectorBuffer	*nextBuffer;
	SectorBuffer	*hashTable[SECTOR_HASH_SIZE];
	UCHAR			*bufferSpace;
	int				numberBuffers;
	int				pageSize;
	int				pagesPerSector;
	void writePage(Bdb* bdb);
};

#endif
