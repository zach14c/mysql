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

#include "mysql_priv.h"

Fixed_string::~Fixed_string()
{}

void Fixed_string::set(const String* str)
{
  set(str->ptr(), str->length(), (CHARSET_INFO*) str->charset());
}

void Fixed_string::set(const char* str, size_t len, const CHARSET_INFO *str_cs)
{
  uint32 dummy_offset;
  size_t numchars;
  size_t to_copy;
  CHARSET_INFO *cs= m_param->m_cs;
  const char *end= str + len;

  if (str == NULL)
  {
    clear();
    return;
  }

  numchars= str_cs->cset->numchars((CHARSET_INFO*) str_cs, str, end);

  if (numchars <= m_param->m_max_char_length)
  {
    to_copy= len;
    m_truncated= FALSE;
  }
  else
  {
    numchars= m_param->m_max_char_length;
    to_copy= cs->cset->charpos(cs, str, end, numchars);
    m_truncated= TRUE;
  }

  if (String::needs_conversion(to_copy, (CHARSET_INFO*) str_cs,
                               (CHARSET_INFO*) cs, & dummy_offset))
  {
    size_t dest_len= numchars * cs->mbmaxlen;
    reserve(dest_len + cs->mbminlen);
    if (m_ptr)
    {
      const char* well_formed_error_pos;
      const char* cannot_convert_error_pos;
      const char* from_end_pos;

      m_byte_length= well_formed_copy_nchars((CHARSET_INFO *) cs,
                                             m_ptr, dest_len,
                                             (CHARSET_INFO*) str_cs, str, len,
                                             numchars,
                                             & well_formed_error_pos,
                                             & cannot_convert_error_pos,
                                             & from_end_pos);
      add_nul(m_ptr + m_byte_length);
    }
  }
  else
  {
    reserve(to_copy + cs->mbminlen);
    if (m_ptr)
    {
      m_byte_length= to_copy;
      memcpy(m_ptr, str, to_copy);
      add_nul(m_ptr + to_copy);
    }
  }
}

void Fixed_string::add_nul(char *ptr)
{
  /*
    Assume that the NUL character is always encoded using 0 bytes,
    and is always using an encoding of the minimum length.
    Verified for:
    - UTF8 : NUL = 1 zero byte
    - latin* : NUL = 1 zero byte
    - UTF16 : NUL = 2 zero byte
    - UTF32 : NUL = 4 zero byte
  */
  switch (m_param->m_cs->mbminlen)
  {
  case 4: *ptr++ = '\0'; /* fall through */
  case 3: *ptr++ = '\0'; /* fall through */
  case 2: *ptr++ = '\0'; /* fall through */
  case 1: *ptr++ = '\0'; break;
  default: DBUG_ASSERT(FALSE); break;
  }
}

void Fixed_string::copy(const Fixed_string *str)
{
  DBUG_ASSERT(m_param->m_cs->number == str->m_param->m_cs->number);

  set(str->m_ptr, str->m_byte_length, str->m_param->m_cs);
}

void Fixed_string::clear()
{
  m_ptr= NULL;
  m_byte_length= 0;
  m_allocated_length= 0;
  m_truncated= FALSE;
}

void Fixed_string::reserve(size_t len)
{
  if ((m_ptr != NULL) && (m_allocated_length >= len))
    return;

  m_ptr= (char*) alloc_root(m_mem_root, len);
  m_allocated_length= (m_ptr ? len : 0);
}

const Fixed_string_param UTF8String64::params=
{64, & my_charset_utf8_bin };

const Fixed_string_param UTF8String128::params=
{128, & my_charset_utf8_bin };


