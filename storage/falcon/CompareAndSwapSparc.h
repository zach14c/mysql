/* Copyright (C) 2008 MySQL AB, 2008 Sun Microsystems, Inc.

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

#ifndef __COMPAREANDSWAPSPARC_H
#define __COMPAREANDSWAPSPARC_H

#if defined(__SUNPRO_CC) && defined(__sparc)

/* Declaration of C prototypes for code written as assembly using 
   Sun Studio's inline templates. The implementation is found in 
   CompareAndSwapSparc.il */
extern "C" int cas_sparc(volatile int *target, int compare, int exchange);
extern "C" int cas_pointer_sparc32(volatile void **target, void *compare, 
                                   void *exchange);
extern "C" int cas_pointer_sparc64(volatile void **target, void *compare, 
                                   void *exchange);


inline int cas_pointer_sparc(volatile void **target, void *compare,
                             void *exchange)
{
	if (sizeof(void*) == 4)
		return cas_pointer_sparc32(target, compare, exchange);
	else
		return cas_pointer_sparc64(target, compare, exchange);
}

#endif /* __SUNPRO_CC && __sparc */

#endif /* __COMPAREANDSWAPSPARC_H */
