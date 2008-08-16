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

#ifndef _WALK_DEFERRED_H_
#define _WALK_DEFERRED_H_

#include "IndexWalker.h"
#include "DeferredIndexWalker.h"

class WalkDeferred : public IndexWalker
{
public:
	WalkDeferred(DeferredIndex *deferredIdx, Transaction *transaction, int flags, IndexKey *lower, IndexKey *upper);
	virtual ~WalkDeferred(void);

	virtual Record*		getNext(bool lockForUpdate);
	
	DeferredIndexWalker		walker;
	DINode					*node;
	DeferredIndex			*deferredIndex;
};

#endif
