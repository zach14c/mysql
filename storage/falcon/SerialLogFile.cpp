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

// SerialLogFile.cpp: implementation of the SerialLogFile class.
//
//////////////////////////////////////////////////////////////////////

#define _FILE_OFFSET_BITS	64

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#else

#ifdef STORAGE_ENGINE
#include "config.h"
#endif

#include <unistd.h>
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string.h>
#include <memory.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include "Engine.h"
#include "SerialLogFile.h"
#include "SerialLog.h"
#include "Sync.h"
#include "SQLError.h"
#include "Database.h"
#include "Log.h"
#include "IOx.h"
#include "Priority.h"


#ifndef O_BINARY
#define O_BINARY	0
#endif


extern uint	falcon_serial_log_priority;

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////


SerialLogFile::SerialLogFile(Database *db)
{
	database = db;
	handle = 0;
	offset = 0;
	highWater = 0;
	writePoint = 0;
	forceFsync = false;
	created = false;
	sectorSize = database->serialLogBlockSize;
}

SerialLogFile::~SerialLogFile()
{
	close();
	if(created && deleteFilesOnExit)
		unlink(fileName);
}

void SerialLogFile::open(JString filename, bool create)
{
	if(!create)
		ASSERT(!inCreateDatabase);

#ifdef _WIN32
	handle = 0;
	char pathName[1024];
	char *ptr;
	int n = GetFullPathName(filename, sizeof(pathName), pathName, &ptr);

	handle = CreateFile(pathName,
						GENERIC_READ | GENERIC_WRITE,
						0,							// share mode
						NULL,						// security attributes
						(create) ? CREATE_NEW : OPEN_EXISTING,
						FILE_FLAG_NO_BUFFERING | FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_WRITE_THROUGH,
						0);

	if (handle == INVALID_HANDLE_VALUE)
		throw SQLError(IO_ERROR, "can't open serial log file \"%s\", error code %d", (const char*) pathName, GetLastError());

	fileName = pathName;
	char *p = strchr(pathName, '\\');
	
	if (p)
		p[1] = 0;

	DWORD	sectorsPerCluster;
	DWORD	bytesPerSector;
	DWORD	numberFreeClusters;
	DWORD	numberClusters;

	if (!GetDiskFreeSpace(pathName, &sectorsPerCluster, &bytesPerSector, &numberFreeClusters, &numberClusters))
		throw SQLError(IO_ERROR, "GetDiskFreeSpace failed for \"%s\"", (const char*) pathName);

	sectorSize = MAX(bytesPerSector, database->serialLogBlockSize);
#else
	

	if (create)
		handle = ::open(filename, O_RDWR | O_BINARY | O_CREAT|O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
	else
		handle = ::open(filename, O_RDWR | O_BINARY);


	if (handle <= 0)		
		throw SQLEXCEPTION (IO_ERROR, "can't open file \"%s\": %s (%d)", 
							(const char*) filename, strerror (errno), errno);

	IO::setWriteFlags(handle, &forceFsync);
	fileName = filename;
	struct stat statBuffer;
	fstat(handle, &statBuffer);
	//sectorSize = MAX(statBuffer.st_blksize, database->serialLogBlockSize);
	sectorSize = MAX(512, database->serialLogBlockSize);
#endif

	if (create)
	{
		created = true;
		zap();
	}
}

void SerialLogFile::close()
{
#ifdef _WIN32
	if (handle)
		{
		CloseHandle(handle);
		handle = 0;
		}
#else
	if (handle > 0)
		{
		::close(handle);
		handle = 0;
		}
#endif
}

void SerialLogFile::write(int64 position, uint32 length, const SerialLogBlock *data)
{
	uint32 effectiveLength = ROUNDUP(length, sectorSize);
	time_t start = database->timestamp;
	Priority priority(database->ioScheduler);
	
	if (!(position == writePoint || position == 0 || writePoint == 0))
		throw SQLError(IO_ERROR, "serial log left in inconsistent state");
	
	if (falcon_serial_log_priority)
		priority.schedule(PRIORITY_HIGH);
		
#ifdef _WIN32
	LARGE_INTEGER pos;
	pos.QuadPart = position;
	OVERLAPPED overlapped = {0};
	overlapped.Offset = pos.LowPart;
	overlapped.OffsetHigh = pos.HighPart;

	DWORD ret;
	
	if (!WriteFile(handle, data, effectiveLength, &ret, &overlapped))
		{
		int lastError = GetLastError();
		
		if (lastError == ERROR_HANDLE_DISK_FULL)
			throw SQLError(DEVICE_FULL, "device full error on serial log file %s\n", (const char*) fileName);

		throw SQLError(IO_ERROR, "serial log WriteFile failed with %d", lastError);
		}

#else

#if defined(HAVE_PREAD) && !defined(HAVE_BROKEN_PREAD)
	uint32 n = ::pwrite (handle, data, effectiveLength, position);
#else
	Sync sync (&syncObject, "SerialLogFile::write");
	sync.lock (Exclusive);

	if (position != offset)
		{
		off_t loc = lseek(handle, position, SEEK_SET);
		
		if (loc != position)
			throw SQLEXCEPTION (IO_ERROR, "serial lseek error on \"%s\": %s (%d)", 
								(const char*) fileName, strerror (errno), errno);
		}

	uint32 n = ::write(handle, data, effectiveLength);
	
#endif

	if (forceFsync)
		fsync(handle);

	if (n != effectiveLength)
		{
		if (errno == ENOSPC)
			throw SQLError(DEVICE_FULL, "device full error on serial log file %s\n", (const char*) fileName);

		throw SQLEXCEPTION (IO_ERROR, "serial write error on \"%s\": %s (%d)", 
							(const char*) fileName, strerror (errno), errno);
		}
#endif

	time_t delta = database->timestamp - start;

	if (delta > 1)
		Log::debug("Serial log write took %d seconds\n", delta);

	offset = position + effectiveLength;
	writePoint = offset;
	highWater = offset;
}

uint32 SerialLogFile::read(int64 position, uint32 length, UCHAR *data)
{
	uint32 effectiveLength = ROUNDUP(length, sectorSize);
	//Sync syncIO(&database->syncSerialLogIO, "SerialLogFile::read(1)");
	Priority priority(database->ioScheduler);

	if (falcon_serial_log_priority)
		//syncIO.lock(Exclusive);
		priority.schedule(PRIORITY_HIGH);

#ifdef _WIN32

	ASSERT(position < writePoint || writePoint == 0);
	LARGE_INTEGER pos;
	pos.QuadPart = position;
	OVERLAPPED overlapped = {0};
	overlapped.Offset = pos.LowPart;
	overlapped.OffsetHigh = pos.HighPart;

	DWORD n;

	if (!ReadFile(handle, data, effectiveLength, &n, &overlapped))
		{
		DWORD lastError = GetLastError();
		if(lastError != ERROR_HANDLE_EOF)
			throw SQLError(IO_ERROR, "serial log ReadFile failed with %d", 
							GetLastError());
		else
			n = 0;	// reached end of file
		}


	offset = position + n;
	highWater = MAX(offset, highWater);
	
	return n;
#else

#if defined(HAVE_PREAD) && !defined(HAVE_BROKEN_PREAD)
	int n = ::pread (handle, data, effectiveLength, position);
#else
	Sync sync(&syncObject, "SerialLogFile::read(2)");
	sync.lock(Exclusive);
	ASSERT(position < writePoint || writePoint == 0);
	off_t loc = lseek(handle, position, SEEK_SET);

	if (loc != position)
		throw SQLEXCEPTION (IO_ERROR, "serial lseek error on \"%s\": %s (%d)", 
							(const char*) fileName, strerror (errno), errno);
		
	int n = ::read(handle, data, effectiveLength);
#endif

	if (n < 0)
		throw SQLEXCEPTION (IO_ERROR, "serial read error on \"%s\": %s (%d)", 
							(const char*) fileName, strerror (errno), errno);

	offset = position + n;
	highWater = MAX(offset, highWater);

	return n;
#endif
}

void SerialLogFile::truncate(int64 size)
{
#ifdef _WIN32
	LARGE_INTEGER oldPos, distance;
	distance.QuadPart = 0;

	// Get current position in file 
	if (!SetFilePointerEx(handle, distance ,&oldPos,FILE_CURRENT))
		throw SQLError(IO_ERROR, "SetFilePointerEx failed with %d", 
						GetLastError());

	// Position to the new end of file , set EOF marker there
	distance.QuadPart = size;
	if (!SetFilePointerEx(handle, distance, 0, FILE_BEGIN))
		throw SQLError(IO_ERROR, "SetFilePointerEx failed with %d", 
						GetLastError());

	if (!SetEndOfFile(handle))
		throw SQLError(IO_ERROR, "SetEndOfFile failed with %d", 
						GetLastError());


	// Restore file pointer
	if (!SetFilePointerEx(handle, oldPos ,0,FILE_BEGIN))
		throw SQLError(IO_ERROR, "SetFilePointerEx failed with %d", 
						GetLastError());
#else
	if (ftruncate(handle, size))
		throw SQLError(IO_ERROR, "ftruncate failed with %d", 
						errno);
#endif
}

int64 SerialLogFile::size(void)
{
#ifdef _WIN32
	LARGE_INTEGER size;

	if (!GetFileSizeEx(handle, &size))
		throw SQLError(IO_ERROR, "GetFileSizeEx failed with %u", 
						GetLastError());

	return size.QuadPart;
#else
	struct stat buf;
	if (fstat(handle, &buf))
		throw SQLError(IO_ERROR, "stat failed with %d",
						errno);
	return  buf.st_size;
#endif
}

void SerialLogFile::zap()
{
	// HACK: Files of size between 1 and 4095 bytes cannot be read on 
	// linux on reiserfs file system, if file is opened  with O_DIRECT. 
	// The error is supposedly related to the file size being less than 
	// page size, so initial size is made 8K just in case we'll ever run on IA64
	size_t initialSize = MAX(sectorSize, 8192); 
	UCHAR *junk = new UCHAR[initialSize +sectorSize];
	UCHAR *buffer = ALIGN(junk, sectorSize);
	memset(buffer, 0, sectorSize);
	write(0, sectorSize, (SerialLogBlock*) buffer);
	delete junk;
}

void SerialLogFile::dropDatabase()
{
	close();
	unlink(fileName);
}
