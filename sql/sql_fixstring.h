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

#ifndef SQL_FIXSTRING_H
#define SQL_FIXSTRING_H

class String;

/**
  This class represents a VARCHAR string or a given fixed maximum size,
  and of a given fixed character set.
  The size and character set are immutable.
  Memory used to represent the string is allocated from a provided memory root.
*/
class Fixed_string
{
public:
  /**
    Constructor.
    @param max_char Immutable max size
    @param cs Immutable character set
    @param mem_root Memory root to use to represent the string value
  */
  Fixed_string(size_t max_char, CHARSET_INFO *cs, MEM_ROOT *mem_root)
    : m_max_char(max_char),
    m_cs(cs),
    m_mem_root(mem_root),
    m_byte_length(0),
    m_allocated_length(0),
    m_truncated(FALSE),
    m_ptr(NULL)
  {}

  void init(MEM_ROOT *mem_root)
  { m_mem_root= mem_root; }

  /** Destructor. */
  ~Fixed_string();

  /**
    Set the string value, with character set conversion if necessary.
    @param str the value to set, expressed in any character set
  */
  void set(const String* str);

  /**
    Set the string value, with character set conversion if necessary.
    @param str the value to set, expressed in character set str_cs
    @param len length, in bytes, of str
    @param str_cs character set of str
  */
  void set(const char* str, size_t len, const CHARSET_INFO *str_cs);

  /**
    Predicate, indicates if the string is truncated to the maximum size.
    @return true if the string is truncated
  */
  bool is_truncated() const
  { return m_truncated; }

  /**
    Access to the C pointer representation of the string.
    @return a NUL terminated C string, in the character set of m_cs
  */
  const char* ptr() const
  { return m_ptr; }

  /**
    Length, in bytes, of the C string representation,
    excluding the terminating NUL character.
    @return The string length in bytes
  */
  size_t length() const
  { return m_byte_length; }

  /**
    Character set of the string.
    @return the string character set
  */
  const CHARSET_INFO *charset() const
  { return m_cs; }

  /**
    Set the string value from another Fixed_string.
    Note that the character set of this object and of str must be equal.
    @param str the string to copy
  */
  void copy(const Fixed_string *str);

  void clear();

private:
  /**
    Allocate memory for the string representation.
    @param len size in bytes to allocate
  */
  void reserve(size_t len);

  /**
    Add a NUL character at the end of the string.
    @param ptr location of the NUL character
  */
  void add_nul(char *ptr);

  /**
    Immutable maximum length of a VARCHAR string (N), in VARCHAR(N)
  */
  const size_t m_max_char;

  /**
    Immutable character set of a VARCHAR string (C), in VARCHAR(N) CHARACTER SET C.
  */
  CHARSET_INFO * const m_cs;

  /**
    Memory root to use to allocate the string value.
  */
  MEM_ROOT *m_mem_root;

  /**
    Length of the C string representation, in bytes,
    excluding the terminating NUL character.
  */
  size_t m_byte_length;

  /**
    Size, in bytes, of the memory allocated.
  */
  size_t m_allocated_length;

  /**
    True if the string was truncated.
    Note that no warnings or errors are generated,
    the string is truncated silently.
  */
  bool m_truncated;

  /**
    C representation of the string, NUL terminated.
  */
  char * m_ptr;
};

/**
  This class represents a 'VARCHAR(64) CHARACTER SET UTF8' string value.
*/
class UTF8String64 : public Fixed_string
{
public:
  /**
    Constructor.
    @param root The memory root to use to represent the string value.
  */
  UTF8String64(MEM_ROOT *root)
    : Fixed_string(64, & my_charset_utf8_bin, root)
  {}

  UTF8String64()
    : Fixed_string(64, & my_charset_utf8_bin, NULL)
  {}

  ~UTF8String64()
  {}
};

/**
  This class represents a 'VARCHAR(128) CHARACTER SET UTF8' string value.
*/
class UTF8String128 : public Fixed_string
{
public:
  /**
    Constructor.
    @param root The memory root to use to represent the string value.
  */
  UTF8String128(MEM_ROOT *root)
    : Fixed_string(128, & my_charset_utf8_bin, root)
  {}

  UTF8String128()
    : Fixed_string(128, & my_charset_utf8_bin, NULL)
  {}

  ~UTF8String128()
  {}
};

#endif


