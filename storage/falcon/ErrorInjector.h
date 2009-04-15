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

#ifndef ErrorInjector_H
#define ErrorInjector_H

enum InjectorEventType{
	InjectorSerialLogAppend = 0,
	InjectorRecoveryPhase1 = 1,
	InjectorRecoveryPhase2 = 2,
	InjectorRecoveryPhase3 = 3,
	InjectorSerialLogTruncate = 4,
	InjectorTypeMax = 5 
};


class ErrorInjector
{
private:
	int iterations;
	InjectorEventType type;
	int param;
public:
	
	ErrorInjector();
	
	// Set crash injector parameters
	// Function accepts strings like "type=SerialLogAppend,param=1,iterations=100"
	void parse(const char *spec);
	
	// Process event. If event type, param and number of event occurences match 
	// the parsed specification, force crash
	void processEvent(InjectorEventType eventType, int eventParam);
	
	// get the singleton object
	static ErrorInjector* getInstance();
}; 


// Macros for error injection. Dummies if NO_ERROR_INJECTOR is defined
#ifndef NO_ERROR_INJECTOR
#define ERROR_INJECTOR_EVENT(type, param) \
	ErrorInjector::getInstance()->processEvent(type, param)
#define ERROR_INJECTOR_PARSE(spec) \
	ErrorInjector::getInstance()->parse(spec)
#else
#define ERROR_INJECTOR_EVENT(type, param)
#define ERROR_INJECTOR_PARSE(type, param)
#endif

#endif
