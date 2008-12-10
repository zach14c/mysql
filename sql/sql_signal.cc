/* Copyright (C) 2008 Sun Microsystems, Inc

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
#include "sp_head.h"
#include "sp_pcontext.h"
#include "sp_rcontext.h"
#include "sql_signal.h"

/*
  Design notes about SQL_condition::m_message_text.

  The member SQL_condition::m_message_text contains the text associated with
  an error, warning or note (which are all SQL 'conditions')

  Producer of SQL_condition::m_message_text:
  ------------------------------------------

  (#1) the server implementation itself, when invoking functions like
  my_error() or push_warning()

  (#2) user code in stored programs, when using the SIGNAL statement.

  (#3) user code in stored programs, when using the RESIGNAL statement.

  When invoking my_error(), the error number and message is typically
  provided like this:
  - my_error(ER_WRONG_DB_NAME, MYF(0), ...);
  - my_message(ER_SLAVE_IGNORED_TABLE, ER(ER_SLAVE_IGNORED_TABLE), MYF(0));

  In both cases, the message is retrieved from ER(ER_XXX), which in turn
  is read from the resource file errmsg.sys at server startup.
  The strings stored in the errmsg.sys file are expressed in the character set
  that corresponds to the server --language start option
  (see error_message_charset_info).

  When executing:
  - a SIGNAL statement,
  - a RESIGNAL statement,
  the message text is provided by the user logic, and is expressed in UTF8.

  Storage of SQL_condition::m_message_text:
  -----------------------------------------

  (#4) The class SQL_condition is used to hold the message text member.
  This class represents a single SQL condition.

  (#5) The thread warn_list is a collection of SQL_condition, and represents
  the condition area for the top level statement.

  (#6) For nested statements (sub statements invoked in stored programs),
  the SQL standard mandates that the implementation maintains a stack of
  SQL diagnostics area, so that each sub statement has an associated
  condition area (a collection os SQL conditions).
  This is currently not implemented in MySQL, only the top level condition area
  is implemented (represented by warn_list).
  The code is minimal, and only keeps the last SQL_condition caught in a SQL
  exception handler (DECLARE HANDLER in a stored program).
  This is implemented by sp_rcontext::m_raised_conditions.

  Consumer of SQL_condition::m_message_text:
  ------------------------------------------

  (#7) The statements SHOW WARNINGS and SHOW ERRORS display the content of
  the warning list.

  (#8) The RESIGNAL statement reads the SQL_condition caught by an exception
  handler, to raise a new or modified condition (in #3).

  (#9) The GET DIAGNOSTICS statement (planned, not implemented yet) will
  also read the content of:
  - the top level statement condition area (when executed  in a query),
  - a sub statement (when executed in a stored program)
  and return the data stored in a SQL_condition.

  The big picture
  ---------------

  my_error(#1)                 SIGNAL(#2)                 RESIGNAL(#3)
      |(#A)                       |(#B)                       |(#C)
      |                           |                           |
      ----------------------------|----------------------------
                                  |
                                  V
                           SQL_condition(#4)
                                  |
                    -----------------------------------------
                    |(#D)                                   |(#I)
                    V                                       V
                warn_list(#5)                         Stored Programs(#6)
                    |                                       |
          ---------------------                 ---------------------
          |(#G)               |(#K)             |(#J)               |(#M)
          V                   V                 V                   V
   SHOW WARNINGS(#7)  GET DIAGNOSTICS(#9)    RESIGNAL(#8)  GET DIAGNOSTICS(#9)
          |  |                |
          |  --------         |
          |         |         |
          V         |         |
      Connectors    |         |
          |(#F)     |(#H)     |(#L)
          ---------------------
                    |
                    V
             Client application

  Current implementation status
  -----------------------------

  (#1) (my_error) produces data in the 'error_message_charset_info' CHARSET

  (#2) and (#3) (SIGNAL, RESIGNAL) produces data internally in UTF8

  (#7) (SHOW WARNINGS) produces data in the 'error_message_charset_info' CHARSET

  (#8) (RESIGNAL) produces data internally in UTF8 (see #3)

  (#9) (GET DIAGNOSTICS) is not implemented.

  As a result, the design choice for (#4), (#5) and (#6) is to store data in
  the 'error_message_charset_info' CHARSET, to minimize impact on the code base.
  This is implemented by using 'String SQL_condition::m_message_text'.

  The UTF8 -> error_message_charset_info convertion is implemented in
  Abstract_signal::eval_signal_informations() (for path #B and #C).

  Future work
  -----------

  - Change (#1) (my_error) to generate errors in UTF8.
    See WL#751 (Recoding of error messages)

  - Change (#4, #5 and #6) to store message text in UTF8 natively.
    In practice, this means changing the type of the message text to
    'UTF8String128 SQL_condition::m_message_text', and is a direct
    consequence of WL#751.

  - Change (#6) to implement a full stacked diagnostics area for sub statements.
    There are many reported bugs affecting this area.
    See Bug#36649 (Condition area is not properly cleaned up after trigger
                   invocation)

  - Implement (#9) (GET DIAGNOSTICS).
    See WL#2111 (Stored Procedures: Implement GET DIAGNOSTICS)
*/

/*
  The parser accepts any error code (desired)
  The runtime internally supports any error code (desired)
  The client server protocol is limited to 16 bits error codes (restriction)
  Enforcing the 65535 limit in the runtime until the protocol can change.
*/
#define MAX_MYSQL_ERRNO UINT_MAX16

const LEX_STRING Diag_condition_item_names[]=
{
  { C_STRING_WITH_LEN("CLASS_ORIGIN") },
  { C_STRING_WITH_LEN("SUBCLASS_ORIGIN") },
  { C_STRING_WITH_LEN("CONSTRAINT_CATALOG") },
  { C_STRING_WITH_LEN("CONSTRAINT_SCHEMA") },
  { C_STRING_WITH_LEN("CONSTRAINT_NAME") },
  { C_STRING_WITH_LEN("CATALOG_NAME") },
  { C_STRING_WITH_LEN("SCHEMA_NAME") },
  { C_STRING_WITH_LEN("TABLE_NAME") },
  { C_STRING_WITH_LEN("COLUMN_NAME") },
  { C_STRING_WITH_LEN("CURSOR_NAME") },
  { C_STRING_WITH_LEN("MESSAGE_TEXT") },
  { C_STRING_WITH_LEN("MYSQL_ERRNO") },

  { C_STRING_WITH_LEN("CONDITION_IDENTIFIER") },
  { C_STRING_WITH_LEN("CONDITION_NUMBER") },
  { C_STRING_WITH_LEN("CONNECTION_NAME") },
  { C_STRING_WITH_LEN("MESSAGE_LENGTH") },
  { C_STRING_WITH_LEN("MESSAGE_OCTET_LENGTH") },
  { C_STRING_WITH_LEN("PARAMETER_MODE") },
  { C_STRING_WITH_LEN("PARAMETER_NAME") },
  { C_STRING_WITH_LEN("PARAMETER_ORDINAL_POSITION") },
  { C_STRING_WITH_LEN("RETURNED_SQLSTATE") },
  { C_STRING_WITH_LEN("ROUTINE_CATALOG") },
  { C_STRING_WITH_LEN("ROUTINE_NAME") },
  { C_STRING_WITH_LEN("ROUTINE_SCHEMA") },
  { C_STRING_WITH_LEN("SERVER_NAME") },
  { C_STRING_WITH_LEN("SPECIFIC_NAME") },
  { C_STRING_WITH_LEN("TRIGGER_CATALOG") },
  { C_STRING_WITH_LEN("TRIGGER_NAME") },
  { C_STRING_WITH_LEN("TRIGGER_SCHEMA") }
};

const LEX_STRING Diag_statement_item_names[]=
{
  { C_STRING_WITH_LEN("NUMBER") },
  { C_STRING_WITH_LEN("MORE") },
  { C_STRING_WITH_LEN("COMMAND_FUNCTION") },
  { C_STRING_WITH_LEN("COMMAND_FUNCTION_CODE") },
  { C_STRING_WITH_LEN("DYNAMIC_FUNCTION") },
  { C_STRING_WITH_LEN("DYNAMIC_FUNCTION_CODE") },
  { C_STRING_WITH_LEN("ROW_COUNT") },
  { C_STRING_WITH_LEN("TRANSACTIONS_COMMITTED") },
  { C_STRING_WITH_LEN("TRANSACTIONS_ROLLED_BACK") },
  { C_STRING_WITH_LEN("TRANSACTION_ACTIVE") }
};


SQL_condition::SQL_condition()
 : Sql_alloc(),
   m_class_origin(),
   m_subclass_origin(),
   m_constraint_catalog(),
   m_constraint_schema(),
   m_constraint_name(),
   m_catalog_name(),
   m_schema_name(),
   m_table_name(),
   m_column_name(),
   m_cursor_name(),
   m_message_text(),
   m_message_text_set(FALSE),
   m_sql_errno(0),
   m_level(MYSQL_ERROR::WARN_LEVEL_ERROR),
   m_mem_root(NULL)
{
  memset(m_returned_sqlstate, 0, sizeof(m_returned_sqlstate));
}

void SQL_condition::init(MEM_ROOT *mem_root)
{
  m_class_origin.init(mem_root);
  m_subclass_origin.init(mem_root);
  m_constraint_catalog.init(mem_root);
  m_constraint_schema.init(mem_root);
  m_constraint_name.init(mem_root);
  m_catalog_name.init(mem_root);
  m_schema_name.init(mem_root);
  m_table_name.init(mem_root);
  m_column_name.init(mem_root);
  m_cursor_name.init(mem_root);
  m_mem_root= mem_root;
}

void SQL_condition::clear()
{
  m_class_origin.clear();
  m_subclass_origin.clear();
  m_constraint_catalog.clear();
  m_constraint_schema.clear();
  m_constraint_name.clear();
  m_catalog_name.clear();
  m_schema_name.clear();
  m_table_name.clear();
  m_column_name.clear();
  m_cursor_name.clear();
  m_message_text.length(0);
  m_message_text_set= FALSE;
  m_sql_errno= 0;
  m_level= MYSQL_ERROR::WARN_LEVEL_ERROR;
}

SQL_condition::SQL_condition(MEM_ROOT *mem_root)
 : Sql_alloc(),
   m_class_origin(mem_root),
   m_subclass_origin(mem_root),
   m_constraint_catalog(mem_root),
   m_constraint_schema(mem_root),
   m_constraint_name(mem_root),
   m_catalog_name(mem_root),
   m_schema_name(mem_root),
   m_table_name(mem_root),
   m_column_name(mem_root),
   m_cursor_name(mem_root),
   m_message_text(),
   m_message_text_set(FALSE),
   m_sql_errno(0),
   m_level(MYSQL_ERROR::WARN_LEVEL_ERROR),
   m_mem_root(mem_root)
{
  memset(m_returned_sqlstate, 0, sizeof(m_returned_sqlstate));
}

void
SQL_condition::copy_opt_attributes(const SQL_condition *cond)
{
  DBUG_ASSERT(this != cond);
  m_class_origin.copy(& cond->m_class_origin);
  m_subclass_origin.copy(& cond->m_subclass_origin);
  m_constraint_catalog.copy(& cond->m_constraint_catalog);
  m_constraint_schema.copy(& cond->m_constraint_schema);
  m_constraint_name.copy(& cond->m_constraint_name);
  m_catalog_name.copy(& cond->m_catalog_name);
  m_schema_name.copy(& cond->m_schema_name);
  m_table_name.copy(& cond->m_table_name);
  m_column_name.copy(& cond->m_column_name);
  m_cursor_name.copy(& cond->m_cursor_name);
}

void
SQL_condition::set(uint sql_errno, const char* sqlstate,
                   MYSQL_ERROR::enum_warning_level level, const char* msg)
{
  DBUG_ASSERT(sql_errno != 0);
  DBUG_ASSERT(sqlstate != NULL);
  DBUG_ASSERT(msg != NULL);

  m_sql_errno= sql_errno;
  memcpy(m_returned_sqlstate, sqlstate, SQLSTATE_LENGTH);
  m_returned_sqlstate[SQLSTATE_LENGTH]= '\0';

  set_builtin_message_text(msg);
  m_level= level;
}

void
SQL_condition::set_builtin_message_text(const char* str)
{
  /*
    See the comments
     "Design notes about SQL_condition::m_message_text."
    in file sql_signal.cc
  */
  const char* copy;

  copy= strdup_root(m_mem_root, str);
  m_message_text.set(copy, strlen(copy), error_message_charset_info);
  DBUG_ASSERT(! m_message_text.is_alloced());
  m_message_text_set= TRUE;
}

const char*
SQL_condition::get_message_text() const
{
  return m_message_text.ptr();
}

int
SQL_condition::get_message_octet_length() const
{
  return m_message_text.length();
}

void
SQL_condition::set_sqlstate(const char* sqlstate)
{
  memcpy(m_returned_sqlstate, sqlstate, SQLSTATE_LENGTH);
  m_returned_sqlstate[SQLSTATE_LENGTH]= '\0';
}

Set_signal_information::Set_signal_information()
{
  clear();
}

Set_signal_information::Set_signal_information(
  const Set_signal_information& set)
{
  int i;
  for (i= FIRST_DIAG_SET_PROPERTY;
       i <= LAST_DIAG_SET_PROPERTY;
       i++)
  {
    m_item[i]= set.m_item[i];
  }
}

void Set_signal_information::clear()
{
  int i;
  for (i= FIRST_DIAG_SET_PROPERTY;
       i <= LAST_DIAG_SET_PROPERTY;
       i++)
  {
    m_item[i]= NULL;
  }
}

void Abstract_signal::eval_sqlcode_sqlstate(THD *thd, SQL_condition *cond)
{
  DBUG_ASSERT(m_cond);
  DBUG_ASSERT(cond);

  /*
    SIGNAL is restricted in sql_yacc.yy to only signal SQLSTATE conditions
  */
  DBUG_ASSERT(m_cond->type == sp_cond_type::state);
  const char* sqlstate= m_cond->sqlstate;

  DBUG_ASSERT((sqlstate[0] != '0') || (sqlstate[1] != '0'));

  cond->set_sqlstate(sqlstate);

  if ((sqlstate[0] == '0') && (sqlstate[1] == '1'))
  {
    /* SQLSTATE class "01": warning */
    cond->m_level= MYSQL_ERROR::WARN_LEVEL_WARN;
    cond->m_sql_errno= ER_SIGNAL_WARN;
  }
  else if ((sqlstate[0] == '0') && (sqlstate[1] == '2'))
  {
    /* SQLSTATE class "02": not found */
    cond->m_level= MYSQL_ERROR::WARN_LEVEL_ERROR;
    cond->m_sql_errno= ER_SIGNAL_NOT_FOUND;
  }
  else
  {
    cond->m_level= MYSQL_ERROR::WARN_LEVEL_ERROR;
    cond->m_sql_errno= ER_SIGNAL_EXCEPTION;
  }
}

void Abstract_signal::eval_default_message_text(THD *thd, SQL_condition *cond)
{
  const char* sqlstate= cond->get_sqlstate();

  DBUG_ASSERT((sqlstate[0] != '0') || (sqlstate[1] != '0'));

  if (! cond->is_message_text_set())
  {
    if ((sqlstate[0] == '0') && (sqlstate[1] == '1'))
    {
      /* SQLSTATE class "01": warning */
      cond->set_builtin_message_text(ER(ER_SIGNAL_WARN));
    }
    else if ((sqlstate[0] == '0') && (sqlstate[1] == '2'))
    {
      /* SQLSTATE class "02": not found */
      cond->set_builtin_message_text(ER(ER_SIGNAL_NOT_FOUND));
    }
    else
    {
      cond->set_builtin_message_text(ER(ER_SIGNAL_EXCEPTION));
    }
  }
}

int assign_condition_item(const char* name, THD *thd, Item *set,
                          UTF8String64 *ci)
{
  String str_value;
  String *str;

  DBUG_ENTER("assign_condition_item");

  if (set->is_null())
  {
    thd->raise_error_printf(ER_WRONG_VALUE_FOR_VAR, name, "NULL");
    DBUG_RETURN(1);
  }

  str= set->val_str(& str_value);
  ci->set(str);
  if (ci->is_truncated())
  {
    if (thd->variables.sql_mode & (MODE_STRICT_TRANS_TABLES |
                                   MODE_STRICT_ALL_TABLES))
    {
      thd->raise_error_printf(ER_COND_ITEM_TOO_LONG, name);
      DBUG_RETURN(1);
    }

    thd->raise_warning_printf(WARN_COND_ITEM_TRUNCATED, name);
  }

  DBUG_RETURN(0);
}


int Abstract_signal::eval_signal_informations(THD *thd, SQL_condition *cond)
{
  Item *set;
  String str_value;
  String *str;
  int i;
  int result= 1;

  DBUG_ENTER("Abstract_signal::eval_signal_informations");

  for (i= FIRST_DIAG_SET_PROPERTY;
       i <= LAST_DIAG_SET_PROPERTY;
       i++)
  {
    set= m_set_signal_information.m_item[i];
    if (set)
    {
      if (! set->fixed)
      {
        if (set->fix_fields(thd, & set))
          goto end;
        m_set_signal_information.m_item[i]= set;
      }
    }
  }

  set= m_set_signal_information.m_item[DIAG_CLASS_ORIGIN];
  if (set != NULL)
  {
    if (assign_condition_item("CLASS_ORIGIN", thd, set,
                              & cond->m_class_origin))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_SUBCLASS_ORIGIN];
  if (set != NULL)
  {
    if (assign_condition_item("SUBCLASS_ORIGIN", thd, set,
                              & cond->m_subclass_origin))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_CONSTRAINT_CATALOG];
  if (set != NULL)
  {
    if (assign_condition_item("CONSTRAINT_CATALOG", thd, set,
                              & cond->m_constraint_catalog))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_CONSTRAINT_SCHEMA];
  if (set != NULL)
  {
    if (assign_condition_item("CONSTRAINT_SCHEMA", thd, set,
                              & cond->m_constraint_schema))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_CONSTRAINT_NAME];
  if (set != NULL)
  {
    if (assign_condition_item("CONSTRAINT_NAME", thd, set,
                              & cond->m_constraint_name))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_CATALOG_NAME];
  if (set != NULL)
  {
    if (assign_condition_item("CATALOG_NAME", thd, set,
                              & cond->m_catalog_name))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_SCHEMA_NAME];
  if (set != NULL)
  {
    if (assign_condition_item("SCHEMA_NAME", thd, set,
                              & cond->m_schema_name))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_TABLE_NAME];
  if (set != NULL)
  {
    if (assign_condition_item("TABLE_NAME", thd, set,
                              & cond->m_table_name))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_COLUMN_NAME];
  if (set != NULL)
  {
    if (assign_condition_item("COLUMN_NAME", thd, set,
                              & cond->m_column_name))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_CURSOR_NAME];
  if (set != NULL)
  {
    if (assign_condition_item("CURSOR_NAME", thd, set,
                              & cond->m_cursor_name))
      goto end;
  }

  set= m_set_signal_information.m_item[DIAG_MESSAGE_TEXT];
  if (set != NULL)
  {
    if (set->is_null())
    {
      thd->raise_error_printf(ER_WRONG_VALUE_FOR_VAR,
                              "MESSAGE_TEXT", "NULL");
      goto end;
    }
    /*
      Enforce that SET MESSAGE_TEXT = <value> evaluates the value
      as VARCHAR(128) CHARACTER SET UTF8.
    */
    UTF8String128 utf8_text(thd->mem_root);
    str= set->val_str(& str_value);
    utf8_text.set(str->ptr(), (size_t) str->length(), str->charset());

    if (utf8_text.is_truncated())
    {
      if (thd->variables.sql_mode & (MODE_STRICT_TRANS_TABLES |
                                     MODE_STRICT_ALL_TABLES))
      {
        thd->raise_error_printf(ER_COND_ITEM_TOO_LONG,
                                "MESSAGE_TEXT");
        goto end;
      }

      thd->raise_warning_printf(WARN_COND_ITEM_TRUNCATED,
                                "MESSAGE_TEXT");
    }

    /*
      See the comments
       "Design notes about SQL_condition::m_message_text."
      in file sql_signal.cc
    */
    String converted_text;
    converted_text.set_charset(error_message_charset_info);
    converted_text.append(utf8_text.ptr(), utf8_text.length(),
                          (CHARSET_INFO *) utf8_text.charset());
    cond->set_builtin_message_text(converted_text.c_ptr_safe());
  }

  set= m_set_signal_information.m_item[DIAG_MYSQL_ERRNO];
  if (set != NULL)
  {
    if (set->is_null())
    {
      thd->raise_error_printf(ER_WRONG_VALUE_FOR_VAR,
                              "MYSQL_ERRNO", "NULL");
      goto end;
    }
    longlong code= set->val_int();
    if ((code <= 0) || (code > MAX_MYSQL_ERRNO))
    {
      str= set->val_str(& str_value);
      thd->raise_error_printf(ER_WRONG_VALUE_FOR_VAR,
                              "MYSQL_ERRNO", str->c_ptr_safe());
      goto end;
    }
    cond->m_sql_errno= (int) code;
  }

  /*
    The various item->val_xxx() methods don't return an error code,
    but flag thd in case of failure.
  */
  if (! thd->is_error())
    result= 0;

end:
  for (i= FIRST_DIAG_SET_PROPERTY;
       i <= LAST_DIAG_SET_PROPERTY;
       i++)
  {
    set= m_set_signal_information.m_item[i];
    if (set)
    {
      if (set->fixed)
        set->cleanup();
    }
  }

  DBUG_RETURN(result);
}

int Abstract_signal::raise_condition(THD *thd, SQL_condition *cond)
{
  int result= 1;

  DBUG_ENTER("Abstract_signal::raise_condition");

  DBUG_ASSERT(m_lex->query_tables == NULL);

  if (m_cond != NULL)
  {
    eval_sqlcode_sqlstate(thd, cond);
  }

  if (eval_signal_informations(thd, cond))
    DBUG_RETURN(result);

  eval_default_message_text(thd, cond);

  /* SIGNAL should not signal WARN_LEVEL_NOTE */
  DBUG_ASSERT((cond->m_level == MYSQL_ERROR::WARN_LEVEL_WARN) ||
              (cond->m_level == MYSQL_ERROR::WARN_LEVEL_ERROR));

  SQL_condition *raised= NULL;
  raised= thd->raise_condition(cond->get_sql_errno(),
                               cond->get_sqlstate(),
                               cond->get_level(),
                               cond->get_message_text(),
                               MYF(0));
  if (raised)
  {
    raised->copy_opt_attributes(cond);
  }

  if (cond->m_level == MYSQL_ERROR::WARN_LEVEL_WARN)
  {
    my_ok(thd);
    result= 0;
  }

  DBUG_RETURN(result);
}

int SQLCOM_signal::execute(THD *thd)
{
  int result= 1;
  SQL_condition cond(thd->mem_root);

  DBUG_ENTER("SQLCOM_signal::execute");

  thd->stmt_da->reset_diagnostics_area();
  thd->row_count_func= 0;
  thd->warning_info->clear_warning_info(thd->query_id);

  result= raise_condition(thd, &cond);

  DBUG_RETURN(result);
}


int SQLCOM_resignal::execute(THD *thd)
{
  SQL_condition *signaled;
  int result= 1;

  DBUG_ENTER("SQLCOM_resignal::execute");

  if (! thd->spcont || ! (signaled= thd->spcont->raised_condition()))
  {
    thd->raise_error(ER_RESIGNAL_NO_HANDLER);
    DBUG_RETURN(result);
  }

  if (m_cond == NULL)
  {
    /* RESIGNAL without signal_value */
    result= raise_condition(thd, signaled);
    DBUG_RETURN(result);
  }

  /* RESIGNAL with signal_value */

  /* Make room for 2 conditions */
  thd->warning_info->reserve_space(thd, 2);

  SQL_condition *raised= NULL;
  raised= thd->raise_condition_no_handler(signaled->get_sql_errno(),
                                          signaled->get_sqlstate(),
                                          signaled->get_level(),
                                          signaled->get_message_text());
  if (raised)
  {
    raised->copy_opt_attributes(signaled);
  }

  result= raise_condition(thd, signaled);

  DBUG_RETURN(result);
}

