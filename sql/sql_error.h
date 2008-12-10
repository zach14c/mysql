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

#ifndef SQL_ERROR_H
#define SQL_ERROR_H

#include "sql_list.h" /* Sql_alloc, MEM_ROOT */
#include "m_string.h" /* LEX_STRING */
#include "mysql_com.h" /* MYSQL_ERRMSG_SIZE */

class THD;

/**
  Stores status of the currently executed statement.
  Cleared at the beginning of the statement, and then
  can hold either OK, ERROR, or EOF status.
  Can not be assigned twice per statement.
*/

class Diagnostics_area
{
public:
  enum enum_diagnostics_status
  {
    /** The area is cleared at start of a statement. */
    DA_EMPTY= 0,
    /** Set whenever one calls my_ok(). */
    DA_OK,
    /** Set whenever one calls my_eof(). */
    DA_EOF,
    /** Set whenever one calls my_error() or my_message(). */
    DA_ERROR,
    /** Set in case of a custom response, such as one from COM_STMT_PREPARE. */
    DA_DISABLED
  };
  /** True if status information is sent to the client. */
  bool is_sent;
  /** Set to make set_error_status after set_{ok,eof}_status possible. */
  bool can_overwrite_status;

  void set_ok_status(THD *thd, ulonglong affected_rows_arg,
                     ulonglong last_insert_id_arg,
                     const char *message);
  void set_eof_status(THD *thd);
  void set_error_status(THD *thd, uint sql_errno_arg, const char *message_arg);

  void disable_status();

  void reset_diagnostics_area();

  bool is_set() const { return m_status != DA_EMPTY; }
  bool is_error() const { return m_status == DA_ERROR; }
  bool is_eof() const { return m_status == DA_EOF; }
  bool is_ok() const { return m_status == DA_OK; }
  bool is_disabled() const { return m_status == DA_DISABLED; }
  enum_diagnostics_status status() const { return m_status; }

  const char *message() const
  { DBUG_ASSERT(m_status == DA_ERROR || m_status == DA_OK); return m_message; }

  uint sql_errno() const
  { DBUG_ASSERT(m_status == DA_ERROR); return m_sql_errno; }

  uint server_status() const
  {
    DBUG_ASSERT(m_status == DA_OK || m_status == DA_EOF);
    return m_server_status;
  }

  ulonglong affected_rows() const
  { DBUG_ASSERT(m_status == DA_OK); return m_affected_rows; }

  ulonglong last_insert_id() const
  { DBUG_ASSERT(m_status == DA_OK); return m_last_insert_id; }

  uint statement_warn_count() const
  {
    DBUG_ASSERT(m_status == DA_OK || m_status == DA_EOF);
    return m_statement_warn_count;
  }

  Diagnostics_area() { reset_diagnostics_area(); }

private:
  /** Message buffer. Can be used by OK or ERROR status. */
  char m_message[MYSQL_ERRMSG_SIZE];
  /**
    SQL error number. One of ER_ codes from share/errmsg.txt.
    Set by set_error_status.
  */
  uint m_sql_errno;

  /**
    Copied from thd->server_status when the diagnostics area is assigned.
    We need this member as some places in the code use the following pattern:
    thd->server_status|= ...
    my_eof(thd);
    thd->server_status&= ~...
    Assigned by OK, EOF or ERROR.
  */
  uint m_server_status;
  /**
    The number of rows affected by the last statement. This is
    semantically close to thd->row_count_func, but has a different
    life cycle. thd->row_count_func stores the value returned by
    function ROW_COUNT() and is cleared only by statements that
    update its value, such as INSERT, UPDATE, DELETE and few others.
    This member is cleared at the beginning of the next statement.

    We could possibly merge the two, but life cycle of thd->row_count_func
    can not be changed.
  */
  ulonglong    m_affected_rows;
  /**
    Similarly to the previous member, this is a replacement of
    thd->first_successful_insert_id_in_prev_stmt, which is used
    to implement LAST_INSERT_ID().
  */
  ulonglong   m_last_insert_id;
  /**
    Number of warnings of this last statement. May differ from
    the number of warnings returned by SHOW WARNINGS e.g. in case
    the statement doesn't clear the warnings, and doesn't generate
    them.
  */
  uint	     m_statement_warn_count;
  enum_diagnostics_status m_status;
  /**
    @todo: the following THD members belong here:
    - warn_list, warn_count,
  */
};
///////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////

/**
  Information about warnings of the current connection.
*/

class Warning_info
{
  /** A memory root to allocate warnings and errors */
  MEM_ROOT           m_warn_root;
  /** List of warnings of all severities (levels). */
  List <MYSQL_ERROR> m_warn_list;
  /** A break down of the number of warnings per severity (level). */
  uint	             m_warn_count[(uint) MYSQL_ERROR::WARN_LEVEL_END];
  /**
    The number of warnings of the current statement. Warning_info
    life cycle differs from statement life cycle -- it may span
    multiple statements. In that case we get
    m_statement_warn_count 0, whereas m_warn_list is not empty.
  */
  uint	             m_statement_warn_count;
  /*
    Row counter, to print in errors and warnings. Not increased in
    create_sort_index(); may differ from examined_row_count.
  */
  ulong              m_current_row_for_warning;
  /** Used to optionally clear warnings only once per statement.  */
  ulonglong          m_warn_id;

private:
  Warning_info(const Warning_info &rhs); /* Not implemented */
  Warning_info& operator=(const Warning_info &rhs); /* Not implemented */
public:

  Warning_info(ulonglong warn_id_arg);
  ~Warning_info();

  /**
    Reset the warning information. Clear all warnings,
    the number of warnings, reset current row counter
    to point to the first row.
  */
  void clear_warning_info(ulonglong warn_id_arg);
  /**
    Only clear warning info if haven't yet done that already
    for the current query. Allows to be issued at any time
    during the query, without risk of clearing some warnings
    that have been generated by the current statement.

    @todo: This is a sign of sloppy coding. Instead we need to
    designate one place in a statement life cycle where we call
    clear_warning_info().
  */
  void opt_clear_warning_info(ulonglong query_id)
  {
    if (query_id != m_warn_id)
      clear_warning_info(query_id);
  }

  /**
    Concatenate the list of warnings.
    It's considered tolerable to lose a warning.
  */
  void append_warning_info(THD *thd, Warning_info *source)
  {
    MYSQL_ERROR *err;
    List_iterator_fast<MYSQL_ERROR> it(source->warn_list());
    /*
      Don't use ::push_warning() to avoid invocation of condition
      handlers or escalation of warnings to errors.
    */
    while ((err= it++))
      Warning_info::push_warning(thd, err->level, err->code, err->msg);
  }

  /**
    Conditional merge of related warning information areas.
  */
  void merge_with_routine_info(THD *thd, Warning_info *source);

  /**
    Reset between two COM_ commands. Warnings are preserved
    between commands, but statement_warn_count indicates
    the number of warnings of this particular statement only.
  */
  void reset_for_next_command() { m_statement_warn_count= 0; }

  /**
    Used for @@warning_count system variable, which prints
    the number of rows returned by SHOW WARNINGS.
  */
  ulong warn_count() const
  {
    /*
      This may be higher than warn_list.elements if we have
      had more warnings than thd->variables.max_error_count.
    */
    return (m_warn_count[(uint) MYSQL_ERROR::WARN_LEVEL_NOTE] +
            m_warn_count[(uint) MYSQL_ERROR::WARN_LEVEL_ERROR] +
            m_warn_count[(uint) MYSQL_ERROR::WARN_LEVEL_WARN]);
  }

  /**
    This is for iteration purposes. We return a non-constant reference
    since List doesn't have constant iterators.
  */
  List<MYSQL_ERROR> &warn_list() { return m_warn_list; }

  /**
    The number of errors, or number of rows returned by SHOW ERRORS,
    also the value of session variable @@error_count.
  */
  ulong error_count() const
  {
    return m_warn_count[(uint) MYSQL_ERROR::WARN_LEVEL_ERROR];
  }

  /** Id of the warning information area. */
  ulonglong warn_id() const { return m_warn_id; }

  /** Do we have any errors and warnings that we can *show*? */
  bool is_empty() const { return m_warn_list.elements == 0; }

  /** Increment the current row counter to point at the next row. */
  void inc_current_row_for_warning() { m_current_row_for_warning++; }
  /** Reset the current row counter. Start counting from the first row. */
  void reset_current_row_for_warning() { m_current_row_for_warning= 1; }
  /** Return the current counter value. */
  ulong current_row_for_warning() const { return m_current_row_for_warning; }

  ulong statement_warn_count() const { return m_statement_warn_count; }

  /** Add a new warning to the current list. */
  void push_warning(THD *thd, MYSQL_ERROR::enum_warning_level level,
                    uint code, const char *msg);
};

///////////////////////////////////////////////////////////////////////////

void push_warning(THD *thd, MYSQL_ERROR::enum_warning_level level,
                  uint code, const char *msg);
void push_warning_printf(THD *thd, MYSQL_ERROR::enum_warning_level level,
			 uint code, const char *format, ...);
bool mysqld_show_warnings(THD *thd, ulong levels_to_show);

extern const LEX_STRING warning_level_names[];

#endif // SQL_ERROR_H
