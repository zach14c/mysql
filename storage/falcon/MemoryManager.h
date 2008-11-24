/* Copyright (C) 2006 MySQL AB

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

#ifndef _MEMORY_MANAGER_H
#define _MEMORY_MANAGER_H

#ifdef _WIN32
#define THROWS_NOTHING
#define THROWS_BAD_ALLOC
#define WINSTATIC			static
#else
#include <new>
#define THROWS_NOTHING		throw()
#define THROWS_BAD_ALLOC	//throw (std::bad_alloc)
#define WINSTATIC
#endif

#define _MFC_OVERRIDES_NEW

#ifdef _WIN32
#define ALWAYS_INLINE inline /* for windows */
#elif __GNUC__
#define ALWAYS_INLINE extern inline __attribute__ ((always_inline)) /* for gcc */
#else
#define ALWAYS_INLINE extern inline
#endif

class Stream;
class InfoTable;
class MemMgr;
struct MemObject;

#ifdef _DEBUG
	extern void* MemMgrAllocateDebug (size_t s, const char *file, int line);
	extern void* MemMgrPoolAllocateDebug (MemMgr *pool, size_t s, const char *file, int line);
	
	WINSTATIC ALWAYS_INLINE void* operator new(size_t s) THROWS_BAD_ALLOC
		{ return MemMgrAllocateDebug (s, __FILE__, __LINE__); }
		
	WINSTATIC ALWAYS_INLINE void* operator new(size_t s, const int  &n) 
		{ return MemMgrAllocateDebug (s, __FILE__, __LINE__); }
		
	WINSTATIC ALWAYS_INLINE void* operator new(size_t s, const char *file, int line) 
		{ return MemMgrAllocateDebug (s, file, line); }
		
	WINSTATIC ALWAYS_INLINE void* operator new[](size_t s) THROWS_BAD_ALLOC
		{ return MemMgrAllocateDebug (s, __FILE__, __LINE__); }
		
	WINSTATIC ALWAYS_INLINE void* operator new[](size_t s, const char *file, int line) THROWS_BAD_ALLOC
		{ return MemMgrAllocateDebug (s, file, line); }

	WINSTATIC ALWAYS_INLINE void* operator new(size_t s, MemMgr *pool) THROWS_BAD_ALLOC
		{ return MemMgrPoolAllocateDebug (pool, s, __FILE__, __LINE__); }

	WINSTATIC ALWAYS_INLINE void* operator new(size_t s, MemMgr *pool, const char *file, int line) 
		{ return MemMgrPoolAllocateDebug (pool, s, file, line); }
		
	WINSTATIC ALWAYS_INLINE void* operator new[](size_t s, MemMgr *pool) THROWS_BAD_ALLOC
		{ return MemMgrPoolAllocateDebug (pool, (unsigned int) s, __FILE__, __LINE__); }

	WINSTATIC ALWAYS_INLINE void* operator new[](size_t s, MemMgr *pool, const char *file, int line) 
		{ return MemMgrPoolAllocateDebug (pool,  s, file, line); }

#define POOL_NEW(arg) new(arg, THIS_FILE, __LINE__)
#define NEW	new (THIS_FILE, __LINE__)

#ifndef new
#define new NEW
#endif

#else
	extern void* MemMgrAllocate (size_t s);
	extern void* MemMgrPoolAllocate (MemMgr *pool, size_t s);
	
	WINSTATIC ALWAYS_INLINE void* operator new(size_t s) THROWS_BAD_ALLOC
		{ return MemMgrAllocate (s); }

	WINSTATIC ALWAYS_INLINE void* operator new(size_t s, MemMgr *pool) THROWS_BAD_ALLOC
		{ return ::MemMgrPoolAllocate (pool, s); }

	WINSTATIC ALWAYS_INLINE void* operator new[](size_t s, MemMgr *pool) THROWS_BAD_ALLOC
		{ return ::MemMgrPoolAllocate (pool,s); }

	WINSTATIC ALWAYS_INLINE void* operator new(size_t s, const int  &n) THROWS_BAD_ALLOC
		{ return ::MemMgrAllocate (s); }

	WINSTATIC ALWAYS_INLINE void* operator new[](size_t s) THROWS_BAD_ALLOC
		{ return ::MemMgrAllocate (s); }
		
#define POOL_NEW(arg) new(arg)
#define NEW new
#endif

enum MemMgrWhat {
	MemMgrSystemSummary,
	MemMgrSystemDetail,
	MemMgrRecordSummary,
	MemMgrRecordDetail
	};

static const int MemMgrPoolGeneral		= 0;
static const int MemMgrPoolRecordData	= 1;
static const int MemMgrPoolRecordObject	= 2;

extern void		MemMgrAnalyze(MemMgrWhat what, InfoTable *table);
extern void		MemMgrRelease (void *object);
extern void		MemMgrValidate (void *object);
extern void		MemMgrAnalyze(int mask, Stream *stream);
extern void*	MemMgrRecordAllocate (size_t size, const char *file, int line);
extern void		MemMgrRecordDelete (char *record);
extern void		MemMgrSetMaxRecordMember (long long size);
extern MemMgr*	MemMgrGetFixedPool (int id);

extern MemObject* MemMgrFindPriorBlock (void *block);

WINSTATIC ALWAYS_INLINE void operator delete(void *object) THROWS_NOTHING
	{ MemMgrRelease (object); }
	
WINSTATIC ALWAYS_INLINE void operator delete(void *object, const char *file, int line)
	{ MemMgrRelease (object); }
	
WINSTATIC ALWAYS_INLINE void operator delete[](void *object) THROWS_NOTHING
	{ MemMgrRelease (object); }
	
WINSTATIC ALWAYS_INLINE void operator delete[](void *object, const char *file, int line)
	{ MemMgrRelease (object); }

WINSTATIC ALWAYS_INLINE void operator delete(void *object, MemMgr *pool) THROWS_NOTHING
	{ MemMgrRelease (object); }
	
WINSTATIC ALWAYS_INLINE void operator delete(void *object, MemMgr *pool, const char *file, int line)
	{ MemMgrRelease (object); }

extern void MemMgrValidate ();

#endif
