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

// Log.h: interface for the Log class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_LOG_H__C0B6CEB0_9A4F_11D5_B8D6_00E0180AC49E__INCLUDED_)
#define AFX_LOG_H__C0B6CEB0_9A4F_11D5_B8D6_00E0180AC49E__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include <stdarg.h>
#include "SyncObject.h"

// To activate certain messages, set global falcon_debug_mask=x 
// where x is the total of the message types you want to get.

static const int	LogLog           = 0x00000001;	// 1;
static const int	LogDebug         = 0x00000002;	// 2;
static const int	LogInfo          = 0x00000004;	// 4;
static const int	LogJavaLog       = 0x00000008;	// 8;
static const int	LogJavaDebug     = 0x00000010;	// 16;
static const int	LogGG            = 0x00000020;	// 32;
static const int	LogPanic         = 0x00000040;	// 64;
static const int	LogScrub         = 0x00000080;	// 128;
static const int	LogException     = 0x00000100;	// 256;
static const int	LogScavenge      = 0x00000200;	// 512;
static const int	LogXARecovery    = 0x00000400;	// 1024;
static const int	LogMysqlInfo     = 0x20000000;
static const int	LogMysqlWarning  = 0x40000000;
static const int	LogMysqlError    = 0x80000000;

typedef void (Listener) (int, const char*, void *arg);

struct LogListener {
	int			mask;
	Listener	*listener;
	void		*arg;
	LogListener	*next;
	};

class Thread;

class Log  
{
public:
	static void scrubWords (const char *words);
	static void logBreak(const char *txt, ...);
	static void debugBreak(const char *txt, ...);
	static void fini();
	static void logMessage (int mask, const char *text);
	static void log (int mask, const char *text, va_list args);
	static void log (int mask, const char *txt, ...);
	static int	init();
	static void	deleteListener(Listener *fn, void *arg);
	static void addListener (int mask, Listener *fn, void *arg);
	static void print (int mask, const char *text, void *arg);
	static void debug (const char *txt, ...);
	static void log (const char *txt, ...);
	static void setExclusive(void);
	static void releaseExclusive(void);
	static bool isActive(int mask);

	static volatile int		exclusive;
	static volatile Thread	*exclusiveThread;
};

#endif // !defined(AFX_LOG_H__C0B6CEB0_9A4F_11D5_B8D6_00E0180AC49E__INCLUDED_)
