/* Copyright (C) 2000-2003 MySQL AB

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

class MYSQL_ERROR: public Sql_alloc
{
public:
  enum enum_warning_level
  { WARN_LEVEL_NOTE, WARN_LEVEL_WARN, WARN_LEVEL_ERROR, WARN_LEVEL_END};

  enum_warning_level level;
  uint code;
  char *msg;

  MYSQL_ERROR(MEM_ROOT *warn_root,
              enum_warning_level level_arg, uint code_arg, const char *msg_arg)
    :level(level_arg), code(code_arg)
  {
    if (msg_arg)
      set_msg(warn_root, msg_arg);
  }

private:
  void set_msg(MEM_ROOT *warn_root, const char *msg_arg);
};

void push_warning(THD *thd, MYSQL_ERROR::enum_warning_level level,
                  uint code, const char *msg);
void push_warning_printf(THD *thd, MYSQL_ERROR::enum_warning_level level,
			 uint code, const char *format, ...);
bool mysqld_show_warnings(THD *thd, ulong levels_to_show);

extern const LEX_STRING warning_level_names[];
