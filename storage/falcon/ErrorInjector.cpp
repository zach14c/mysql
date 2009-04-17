/* Copyright (C) 2009 Sun Microsystems
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
#include "ErrorInjector.h"
#include "Error.h"
#include <stdlib.h>
#include <string.h>

ErrorInjector::ErrorInjector():
	iterations(-1),type(InjectorTypeMax),param(0)
{
}

// Global ErrorInjector variable, accessed as singleton
static ErrorInjector errorInjectorInstance;

ErrorInjector *ErrorInjector::getInstance()
{
	return &errorInjectorInstance;
}


void ErrorInjector::parse(const char *spec)
{
	if (strstr(spec, "type=SerialLogAppend"))
		type = InjectorSerialLogAppend;
	else if (strstr(spec,"type=RecoveryPhase1"))
		type = InjectorRecoveryPhase1;
	else if (strstr(spec,"type=RecoveryPhase2"))
		type = InjectorRecoveryPhase2;
	else if (strstr(spec,"type=RecoveryPhase3"))
		type = InjectorRecoveryPhase3;
	else if (strstr(spec,"type=SerialLogTruncate"))
		type = InjectorSerialLogTruncate;
	
	param=-1;
	const char *p= strstr(spec,"param=");
	if(p)
		param = atoi(p+6);
		
	p= strstr(spec, "iterations=");
	if(p)
		iterations = atoi(p+11);
}


void ErrorInjector::processEvent(InjectorEventType eventType , int eventParam)
{
	if (eventType == type && eventParam == param  && iterations > 0)
		if (--iterations <= 0)
			FATAL("Crash due to error injection");
}