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


// IO.cpp: implementation of the IO class.
//
//////////////////////////////////////////////////////////////////////

#define _FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <memory.h>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <direct.h>
#define LSEEK				_lseeki64
#define SEEK_OFFSET	int64
#define MKDIR(dir)			mkdir(dir)
#define PATH_MAX			_MAX_PATH
#else
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#ifdef STORAGE_ENGINE
#include "config.h"
#endif

#ifdef  __linux__
#include <linux/unistd.h>
#include <sys/utsname.h>
static  int getLinuxVersion();
#define KERNEL_VERSION(a, b, c)	((a)<<16 | (b)<<8 | (c))
#define haveBrokenODirect()	(getLinuxVersion() < KERNEL_VERSION(2,6,0))
#else
#define LOCK_SH   1	/* shared lock */
#define LOCK_EX   2	/* exclusive lock */
#define LOCK_NB   4	/* don't block when locking */
#define LOCK_UN   8	/* unlock */
#define haveBrokenODirect()	0
#endif
#include <sys/file.h>
#define O_BINARY		0
#define O_RANDOM		0
#define MKDIR(dir)			mkdir(dir, S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IXUSR | S_IXGRP)
#endif

#ifndef O_SYNC
#define O_SYNC		0
#endif

#ifndef LSEEK
#define LSEEK		lseek
#define SEEK_OFFSET	off_t
#endif

#ifndef S_IRGRP
#define S_IRGRP		0
#define S_IWGRP		0
#endif

#include <fcntl.h>
#include <sys/stat.h>
#include "Engine.h"
#include <string.h>
#include <errno.h>
#include "IOx.h"
#include "BDB.h"
#include "Hdr.h"
#include "SQLError.h"
#include "Sync.h"
#include "Log.h"
#include "Debug.h"
#include "Synchronize.h"

#ifndef STORAGE_ENGINE
#ifndef _WINDOWS
#define HAVE_PREAD
#endif
#endif

#define TRACE_FILE							"io.trace"
//#define SIMULATE_DISK_FULL					1000000;

extern uint		falcon_direct_io;
extern char		falcon_checksums;

static const int TRACE_SYNC_START	= -1;
static const int TRACE_SYNC_END		= -2;


static const uint16 NON_ZERO_CHECKSUM_MAGIC = 0xAFFE;

#ifdef SIMULATE_DISK_FULL
static int simulateDiskFull = SIMULATE_DISK_FULL;
#endif
	
static FILE	*traceFile;
static JString baseDir;

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

IO::IO()
{
	fileId = -1;
	reads = writes = fetches = fakes = flushWrites = 0;
	priorReads = priorWrites = priorFetches = priorFakes = priorFlushWrites = 0;
	writesSinceSync = 0;
	dbb = NULL;
	forceFsync = true;
	fatalError = false;
	memset(writeTypes, 0, sizeof(writeTypes));
	syncObject.setName("IO::syncObject");
}

IO::~IO()
{
	traceClose();
	closeFile();
}

static bool isAbsolutePath(const char *name)
{
#ifdef _WIN32
	size_t len = strlen(name);
	if(len < 2)
		return false;
	return (name[0]=='\\' || name[1]==':');
#else
	return (name[0]=='/');
#endif
}

void IO::setBaseDirectory(const char *directory)
{
	baseDir = directory;
	if (baseDir[baseDir.length()-1] != SEPARATOR)
		baseDir += SEPARATOR;
}

static JString getPath(const char *filename)
{
	if(!baseDir || isAbsolutePath(filename))
		return JString(filename);
	return baseDir + filename;
}

bool IO::openFile(const char * name, bool readOnly)
{
	fileName = getPath(name);
	
	for (int attempt = 0; attempt < 3; ++attempt)
		{
		fileId = ::open (fileName, (readOnly) ? O_RDONLY | O_BINARY : getWriteMode(attempt) | O_RDWR | O_BINARY);
		
		if (fileId >= 0)
			break;
		
		if (attempt == 1)
			forceFsync = true;
		}

	if (fileId < 0)
		{
			int sqlError = (errno == EACCES )? FILE_ACCESS_ERROR :CONNECTION_ERROR;
			throw SQLEXCEPTION (sqlError, "can't open file \"%s\": %s (%d)", name, strerror (errno), errno);
		}

	isReadOnly = readOnly;
	
#ifndef _WIN32
	signal (SIGXFSZ, SIG_IGN);
#ifndef __NETWARE__
	struct flock lock;
	lock.l_type= readOnly ? F_RDLCK : F_WRLCK;
	lock.l_whence= SEEK_SET;
	lock.l_start= lock.l_len= 0;
	if (fcntl(fileId, F_SETLK, &lock) < 0)
		{
		::close (fileId);
		throw SQLEXCEPTION (FILE_ACCESS_ERROR, "file \"%s\" in use by another process", name);
		}
#endif
#endif

	//Log::debug("IO::openFile %s (%d) fd: %d\n", (const char*) fileName, readOnly, fileId);
	
	return fileId != -1;
}

bool IO::createFile(const char *name)
{
	Log::debug("IO::createFile: creating file \"%s\"\n", name);

	fileName = getPath(name);
	
	for (int attempt = 0; attempt < 3; ++attempt)
		{
		fileId = ::open (fileName,
						getWriteMode(attempt) | O_CREAT | O_RDWR | O_RANDOM | O_TRUNC | O_BINARY,
						S_IREAD | S_IWRITE | S_IRGRP | S_IWGRP);

		if (fileId >= 0)
			break;
		
		if (attempt == 1)
			forceFsync = true;
		}

	if (fileId < 0)
		throw SQLEXCEPTION (CONNECTION_ERROR,"can't create file \"%s\", %s (%d)", name, strerror (errno), errno);

	isReadOnly = false;
#ifndef _WIN32
#ifndef __NETWARE__
	struct flock lock;
	lock.l_type= F_WRLCK;
	lock.l_whence= SEEK_SET;
	lock.l_start= lock.l_len= 0;
	// We assume that no other process had a chance to lock new file.
	fcntl(fileId, F_SETLK, &lock);
#endif
#endif

	return fileId != -1;
}

void IO::readPage(Bdb * bdb)
{
	if (fatalError)
		FATAL ("can't continue after fatal error");

	SEEK_OFFSET offset = (int64) bdb->pageNumber * pageSize;
	int length = pread (offset, pageSize, (UCHAR *)bdb->buffer);

	if (length != pageSize)
		{
		declareFatalError();
		FATAL ("read error on page %d of \"%s\": %s (%d)",
				bdb->pageNumber, (const char*) fileName, strerror (errno), errno);
		}

	++reads;

	if(falcon_checksums)
		validateChecksum(bdb->buffer, pageSize, offset);
}

void IO::validateChecksum(Page *page, size_t pageSize, int64 fileOffset)
{
	if (page->checksum == NO_CHECKSUM_MAGIC)
		return;

	uint16 chksum = computeChecksum(page, pageSize);

	if (page->checksum != chksum)
		FATAL("Checksum error (expected %d, got %d) at page %d"
			"file '%s', offset " I64FORMAT ", pagesize %d",
			(int)chksum, (int)page->checksum, (int)(fileOffset/pageSize),
			fileName.getString(), fileOffset, pageSize);
}

bool IO::trialRead(Bdb *bdb)
{
	Sync sync (&syncObject, "IO::trialRead");
	sync.lock (Exclusive);

	seek (bdb->pageNumber);
	int length = ::read (fileId, bdb->buffer, pageSize);

	if (length != pageSize)
		return false;

	++reads;

	return true;
}

void IO::writePage(Bdb * bdb, int type)
{
	if (fatalError)
		FATAL ("can't continue after fatal error");

	ASSERT(bdb->pageNumber != HEADER_PAGE || ((Page*)(bdb->buffer))->pageType == PAGE_header);
	tracePage(bdb);
	writePages(bdb->pageNumber, pageSize, (UCHAR *)bdb->buffer, type);
}

void IO::writePages(int32 pageNumber, int length, const UCHAR* data, int type)
{
	if (fatalError)
		FATAL ("can't continue after fatal error");

	SEEK_OFFSET offset = (int64) pageNumber * pageSize;

#ifdef SIMULATE_DISK_FULL
	if (simulateDiskFull && offset + length > simulateDiskFull)
		throw SQLError(DEVICE_FULL, "device full error on %s, page %d\n", (const char*) fileName, pageNumber);
#endif

	Page *page = (Page *)data;

	for (int i = 0;i < length/pageSize; i++ ,page = (Page *)((UCHAR *)page + pageSize))
		{
		if (falcon_checksums)
			page->checksum = computeChecksum(page, pageSize);
 		else
			page->checksum = NO_CHECKSUM_MAGIC;
		}

	int ret = pwrite (offset, length, data);

	if (ret != length)
		{
		if (errno == ENOSPC)
			throw SQLError(DEVICE_FULL, "device full error on %s, page %d\n", (const char*) fileName, pageNumber);
			
		declareFatalError();
		
		FATAL ("write error on page %d (%d/%d/%d) of \"%s\": %s (%d)",
				pageNumber, length, pageSize, fileId,
				(const char*) fileName, strerror (errno), errno);
		}

	++flushWrites;
	++writesSinceSync;
	++writeTypes[type];
}

void IO::readHeader(Hdr * header)
{
	static const int maxPageSize = 32*1024;
	static const int sectorSize = 4096;
	UCHAR temp[maxPageSize + sectorSize];
	UCHAR *buffer = ALIGN(temp, sectorSize);
	int n = lseek (fileId, 0, SEEK_SET);
	n = ::read (fileId, buffer, maxPageSize);

	if (n < (int) sizeof (Hdr))
		FATAL ("read error on database header");

	Hdr* hdr = (Hdr*) buffer;
	if (falcon_checksums && hdr->pageSize <= n)
		validateChecksum((Page*) buffer, hdr->pageSize, 0);

	memcpy(header, buffer, sizeof(Hdr));
}

void IO::closeFile()
{
	if (fileId != -1)
		{
		::close (fileId);
		//Log::debug("IO::closeFile %s fd: %d\n", (const char*) fileName, fileId);
		fileId = -1;
		}
}

void IO::seek(int pageNumber)
{
	SEEK_OFFSET pos = (int64) pageNumber * pageSize;
	SEEK_OFFSET result = LSEEK (fileId, pos, SEEK_SET);

	if (result != pos)
		Error::error ("long seek failed on page %d of \"%s\"", 
					  pageNumber, (const char*) fileName);
}


void IO::longSeek(int64 offset)
{
	SEEK_OFFSET result = LSEEK (fileId, offset, SEEK_SET);

	if (result != offset)
		Error::error ("long seek failed on  \"%s\"", (const char*) fileName);
}

void IO::declareFatalError()
{
	fatalError = true;
}

void IO::createPath(const char *fileName)
{
	// First, better make sure directories exists
	JString fname = getPath(fileName);

	char directory [256], *q = directory;

	for (const char *p = fname.getString(); *p;)
		{
		char c = *p++;
		
		if (c == '/' || c == '\\')
			{
			*q = 0;
			
			if (q > directory && q [-1] != ':')
				if (MKDIR (directory) && errno != EEXIST)
					throw SQLError (IO_ERROR, 
					"can't create directory \"%s\"\n", directory);
			}
		*q++ = c;
		}
}

const char* IO::baseName(const char *path)
{
	const char *ptr = strrchr(path, SEPARATOR);
	return (ptr ? ptr + 1 : path);
}

void IO::expandFileName(const char *fileName, int length, char *buffer, const char **baseFileName)
{
	char expandedName[PATH_MAX+1];
	char *path;
	JString fname = getPath(fileName);
	fileName = fname.getString();

#ifdef _WIN32
	char *base;
	
	GetFullPathName(fileName, sizeof (expandedName), expandedName, &base);
	
	path = expandedName;
#else
	const char *base;
	char tempName[PATH_MAX+1];
	expandedName [0] = 0;
	tempName[0] = 0;
	
	path = realpath(fileName, tempName);

	if (!path)
		{
		// By definition, the contents of tempName is undefined if realPath() fails.
		// If errno == ENOENT, then tempName will contain the resolved path without
		// the filename--unless it doesn't. Append the filename if necessary.
			
		if (errno == ENOENT)
			{
			base = baseName(fileName);
			
			if (strcmp(base, baseName(tempName)))
				snprintf(expandedName, PATH_MAX, "%s/%s", tempName, base);
			else
				snprintf(expandedName, PATH_MAX, "%s", tempName);
		
			path = expandedName;
			}
		else
			path = fileName;
		}
	
#endif
	if ((int) strlen (path) >= length)
		throw SQLError (IO_ERROR, "expanded filename exceeds buffer length\n");

	strcpy (buffer, path);
	
	if (baseFileName)
		*baseFileName = baseName(buffer);
}

bool IO::doesFileExist(const char *fileName)
{
	struct stat stats;
	int errnum;
	
	return fileStat(fileName, &stats, &errnum) == 0;
}

int IO::fileStat(const char *fileName, struct stat *fileStats, int *errnum)
{
	struct stat stats;
	JString path = getPath(fileName);
	int retCode = stat(path.getString(), &stats);
	
	if (fileStats)
		*fileStats = stats;

	if (errnum)
		*errnum = (retCode == 0) ? 0 : errno;
		
	return(retCode);
}

void IO::write(uint32 length, const UCHAR *data)
{
	uint32 len = ::write (fileId, data, length);

	if (len != length)
		throw SQLError(IO_ERROR, "bad write length, %d of %d requested", len, length);
}

int IO::read(int length, UCHAR *buffer)
{
	return ::read(fileId, buffer, length);
}

void IO::writeHeader(Hdr *header)
{
	int n = lseek (fileId, 0, SEEK_SET);
	n = ::write (fileId, header, sizeof (Hdr));

	if (n != sizeof (Hdr))
		FATAL ("write error on database clone header");
}

void IO::deleteFile()
{
	deleteFile(fileName);
}

int IO::pread(int64 offset, int length, UCHAR* buffer)
{
	int ret;

#if defined(HAVE_PREAD) && !defined(HAVE_BROKEN_PREAD)
	ret = ::pread (fileId, buffer, length, offset);
#elif defined(_WIN32)
	HANDLE hFile = (HANDLE)_get_osfhandle(fileId);
	OVERLAPPED overlapped = {0};
	LARGE_INTEGER pos;

	pos.QuadPart = offset;
	overlapped.Offset = pos.LowPart;
	overlapped.OffsetHigh = pos.HighPart;

	if (!ReadFile(hFile, buffer, length, (DWORD *) &ret, &overlapped))
		ret = -1;
#else
	Sync sync (&syncObject, "IO::pread");
	sync.lock (Exclusive);

	longSeek(offset);
	ret = (int) read(length, buffer);
#endif

	DEBUG_FREEZE;

	return ret;
}

void IO::read(int64 offset, int length, UCHAR* buffer)
{
	Sync sync (&syncObject, "IO::read");
	sync.lock (Exclusive);

	longSeek(offset);
	int len = read (length, buffer);

	if (len != length)
		throw SQLError(IO_ERROR, "read error at " I64FORMAT "%d of \"%s\": %s (%d)",
					   offset, (const char*) fileName, strerror (errno), errno);
}

int IO::pwrite(int64 offset, int length, const UCHAR* buffer)
{
	int ret;
	
#if defined(HAVE_PREAD) && !defined(HAVE_BROKEN_PREAD)
	ret = ::pwrite (fileId, buffer, length, offset);
#elif defined(_WIN32)

	ASSERT(length > 0);

	HANDLE hFile = (HANDLE)_get_osfhandle(fileId);
	OVERLAPPED overlapped = {0};
	LARGE_INTEGER pos;

	pos.QuadPart = offset;
	overlapped.Offset = pos.LowPart;
	overlapped.OffsetHigh = pos.HighPart;

	if (!WriteFile(hFile, buffer, length, (DWORD *)&ret, &overlapped))
		ret = -1;
#else
	Sync sync (&syncObject, "IO::pwrite");
	sync.lock (Exclusive);

	longSeek(offset);
	ret = (int) ::write (fileId, buffer, length);
#endif

#ifndef _WIN32
	if (forceFsync)
		fsync(fileId);
#endif
		
	DEBUG_FREEZE;

	return ret;
}

void IO::write(int64 offset, int length, const UCHAR* buffer)
{
	Sync sync (&syncObject, "IO::write");
	sync.lock (Exclusive);

	longSeek(offset);
	write (length, buffer);
}

void IO::sync(void)
{
	if (fileId == -1)
		return;
	
	if (traceFile)
		traceOperation(TRACE_SYNC_START);
		
#ifdef _WIN32
	if (_commit(fileId))
		{
		declareFatalError();
		FATAL ("_commit failed on \"%s\": %s (%d)",
				(const char*) fileName, strerror (errno), errno);
		}
	
#else
	fsync(fileId);
#endif

	writesSinceSync = 0;
	
	if (traceFile)
		traceOperation(TRACE_SYNC_END);
}

void IO::deleteFile(const char* fileName)
{
	JString path = getPath(fileName);
	unlink(path.getString());
}

void IO::tracePage(Bdb* bdb)
{
	Page *page = bdb->buffer;
	int id = Debug::getPageId(page);
	trace(fileId, bdb->pageNumber, page->pageType, id);
}

void IO::trace(int fd, int pageNumber, int pageType, int pageId)
{
	if (traceFile)
		fprintf(traceFile, "%d %d %d %d\n", fd, pageNumber, pageType, pageId);
}

void IO::traceOpen(void)
{
#ifdef TRACE_FILE
	if (!traceFile)
		traceFile = fopen(TRACE_FILE, "w");
#endif
}

void IO::traceClose(void)
{
	if (traceFile)
		{
		fclose(traceFile);
		traceFile = NULL;
		}
}

void IO::traceOperation(int operation)
{
	trace(fileId, operation, 0, 0);
}

void IO::reportWrites(void)
{
	Log::debug("%s force %d, flush %d, prec %d, reuse %d, pgwrt %d\n",
		(const char*) fileName,
		writeTypes[WRITE_TYPE_FORCE],
		writeTypes[WRITE_TYPE_FLUSH],
		writeTypes[WRITE_TYPE_REUSE],
		writeTypes[WRITE_TYPE_PAGE_WRITER]);
		
	memset(writeTypes, 0, sizeof(writeTypes));
}

int IO::getWriteMode(int attempt)
{
#ifdef O_DIRECT
	if (attempt == 0 && falcon_direct_io > 0 && !haveBrokenODirect())
		return O_DIRECT;
#endif

	if (attempt <= 1)
		return O_SYNC;
	
	return 0;
}

#ifdef __linux__
static int getLinuxVersion()
{
	static int vers = -1;

	if(vers != -1)
		return vers;

	struct utsname utsname;

	if(uname(&utsname) == -1)
		return vers = 0;

	int major, minor ,release;

	if (sscanf(utsname.release,"%d.%d.%d",&major, &minor,&release) != 3)
		return vers = 0;

	return vers = KERNEL_VERSION(major,minor,release);
}
#endif



/*
  Calculate checksum for a page.
  The algorithm somewhat resembles fletcher checksum , but it is performed
  on 64 bit integers and two's complement arithmetic, without taking care
  of overflow.

  Also,
  - Calculation skips 3rd and 4th bytes in the page 
   (which is checksum field in the page header)
  - Returned result is never 0
*/
uint16 IO::computeChecksum(Page *page, size_t len)
{
	uint64 sum1,sum2;
	uint64 *data = (uint64 *)page;
	uint64 *end  = data + len/8;

	ASSERT(len >=8 && OFFSET(Page*,checksum)==2 && sizeof(page->checksum)==2);

	// Initialize sums, mask out checksum bytes in the page header
	static uint16 endian = 1;
	uint64 mask = *(char *)(&endian)?0xFFFFFFFF0000FFFFLL:0xFFFF0000FFFFFFFFLL;
	
	sum1 = sum2 = *data & mask;

	data++;

	while(data < end)
		{
		sum1 += *data++;
		sum2 += sum1;
		}

	uint64 sum = sum1 + sum2;

	// Fold the result to 16 bit
	while(sum >> 16)
		sum = (sum & 0xFFFF) + (sum >> 16);

	if(sum == 0)
		sum = NON_ZERO_CHECKSUM_MAGIC;

	return (uint16) sum;

}
