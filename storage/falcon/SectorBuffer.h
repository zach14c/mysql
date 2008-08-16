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

#ifndef _SECTOR_BUFFER_H_
#define _SECTOR_BUFFER_H_

#include "SyncObject.h"

class SectorCache;
class Dbb;
class Bdb;

class SectorBuffer
{
public:
	SectorBuffer();
	~SectorBuffer(void);

	void		readPage(Bdb* bdb);
	void		readSector();
	
	SyncObject		syncObject;
	SectorCache		*cache;
	SectorBuffer	*next;
	SectorBuffer	*collision;
	Dbb				*dbb;
	UCHAR			*buffer;
	int				activeLength;
	int				sectorNumber;
	void setSector(Dbb* dbb, int sectorNumber);
	void writePage(Bdb* bdb);
};

#endif
