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

#include "Engine.h"
#include "Backup.h"
#include "Database.h"
#include "Dbb.h"
#include "Bdb.h"
#include "IndexPage.h"
#include "RecordLocatorPage.h"
#include "DataPage.h"
#include "DataOverFlowPage.h"
#include "Hdr.h"
#include "SectionPage.h"
#include "IndexRootPage.h"
#include "SequencePage.h"
#include "PageInventoryPage.h"
#include "InversionPage.h"
#include "Log.h"
#include "EncodedDataStream.h"

Backup::Backup(Database *db)
{
	database = db;
}

Backup::~Backup(void)
{
}

void Backup::backupPage(Dbb* dbb, int32 pageNumber, EncodedDataStream* stream)
{
	Bdb *bdb = dbb->fetchPage(pageNumber, PAGE_any, Shared);
	Page *page = bdb->buffer;
	stream->encodeInt(BACKUP_page);
	stream->encodeInt(pageNumber);
	
	switch (page->pageType)
		{
		case PAGE_header:		// 1
			((Hdr*) page)->backup(stream);
			break;

		case PAGE_sections:		// 2
			((SectionPage*) page)->backup(stream);
			break;

		case PAGE_record_locator:	// 4
			((RecordLocatorPage*) page)->backup(stream);
			break;

		case PAGE_btree:			// 5
			((IndexPage*) page)->backup(stream);
			break;

		case PAGE_data:	
			((DataPage*) page)->backup(stream);
			break;

		case PAGE_inventory:		// 8
			((PageInventoryPage*) page)->backup(stream);
			break;

		case PAGE_data_overflow:	// 9
			((DataOverflowPage*) page)->backup(stream);
			break;

		case PAGE_inversion:		// 10
			((InversionPage*) page)->backup(stream);
			break;

		case PAGE_free:			// 11 Page has been released
			Log::debug ("Page %d is a free page\n", pageNumber);
			break;

		default:
			Log::debug ("Page %d is unknown type %d\n", pageNumber, page->pageType);
		}

	bdb->release();
}
