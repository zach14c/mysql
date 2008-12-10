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


/* Implements compare and swap. The signature of the function is:

     int cas_sparc(volatile int *target, int compare, int exchange);

   Return value: if the swap took place, 1 will be returned,
                 otherwise 0 is returned.
*/
.inline cas_sparc
   membar #LoadStore|#StoreStore
   cas [%o0],%o1,%o2
   cmp %o1,%o2
   mov 1,%o0
   movne %icc,0,%o0
.end


/* Implements compare and swap for 32 bit pointers. Signature:

     int cas_pointer_sparc32(volatile void **target, void *compare, 
                             void *exchange);

   Return value: if the swap took place, 1 will be returned,
                 otherwise 0 is returned.
*/
.inline cas_pointer_sparc32
   membar #LoadStore|#StoreStore
   cas [%o0],%o1,%o2
   cmp %o1,%o2
   mov 1,%o0
   movne %icc,0,%o0
.end


/* Implements compare and swap for 64 bit pointers. Signature:

     int cas_pointer_sparc64(volatile void **target, void *compare, 
                           void *exchange);

   Return value: if the swap took place, 1 will be returned,
                 otherwise 0 is returned.
*/
.inline cas_pointer_sparc64
   membar #LoadStore|#StoreStore
   casx [%o0],%o1,%o2
   cmp %o1,%o2
   mov 1,%o0
   movne %icc,0,%o0
.end
