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

#include "Engine.h"
#include "SRLInventoryPage.h"
#include "PageInventoryPage.h"
#include "SerialLogControl.h"
#include "Dbb.h"
#include "Bdb.h"
#include "Page.h"
#include "Log.h"

// Recreate inventory page 
void SRLInventoryPage::pass2()
{
	Bdb* bdb = PageInventoryPage::createInventoryPage(log->getDbb(tableSpaceId), pageNumber, NO_TRANSACTION);
	bdb->mark(NO_TRANSACTION);
	bdb->release(REL_HISTORY);
}

void SRLInventoryPage::print()
{
	logPrint("InventoryPage tableSpaceId %d, page %d \n", tableSpaceId, pageNumber);
}

void SRLInventoryPage::read()
{
	tableSpaceId = getInt();
	pageNumber = getInt();
}

void SRLInventoryPage::append(Dbb *dbb, int32 pageNumber)
{
	START_RECORD(srlInventoryPage, "SRLInventoryPage::append");
	putInt(dbb->tableSpaceId);
	putInt(pageNumber);
	sync.unlock();
}

SRLInventoryPage::SRLInventoryPage()
{
}

SRLInventoryPage::~SRLInventoryPage()
{
}

