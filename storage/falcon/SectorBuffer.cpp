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

#include <memory.h>
#include "Engine.h"
#include "SectorBuffer.h"
#include "SectorCache.h"
#include "BDB.h"
#include "Dbb.h"

SectorBuffer::SectorBuffer()
{
	activeLength = 0;
	sectorNumber = -1;
}

SectorBuffer::~SectorBuffer(void)
{
}

void SectorBuffer::readPage(Bdb* bdb)
{
	int offset = (bdb->pageNumber % cache->pagesPerSector) * cache->pageSize;
	ASSERT(offset < activeLength);
	memcpy(bdb->buffer, buffer + offset, cache->pageSize);
}

void SectorBuffer::readSector()
{
	uint64 offset = sectorNumber * cache->pagesPerSector * cache->pageSize;
	activeLength = dbb->pread(offset, SECTOR_BUFFER_SIZE, buffer);
}

void SectorBuffer::setSector(Dbb* db, int sector)
{
	dbb = db;
	sectorNumber = sector;
}

void SectorBuffer::writePage(Bdb* bdb)
{
	int offset = (bdb->pageNumber % cache->pagesPerSector) * cache->pageSize;
	memcpy(buffer + offset, bdb->buffer, cache->pageSize);
	offset += cache->pageSize;
	activeLength = MAX(activeLength, offset);
}
