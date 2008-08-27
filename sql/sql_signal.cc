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
   m_flags(0),
   m_mem_root(mem_root)
{
  memset(m_returned_sqlstate, 0, sizeof(m_returned_sqlstate));
}

SQL_condition *
SQL_condition::deep_copy(THD *thd, MEM_ROOT *mem_root,
                         const SQL_condition *cond)
{
  SQL_condition *copy= new (mem_root) SQL_condition(mem_root);
  if (copy)
    copy->deep_copy(cond);
  return copy;
}

void
SQL_condition::deep_copy(const SQL_condition *cond)
{
  memcpy(m_returned_sqlstate, cond->m_returned_sqlstate,
         sizeof(m_returned_sqlstate));

  if (cond->m_message_text.length())
  {
    const char* copy;

    copy= strdup_root(m_mem_root, cond->m_message_text.ptr());
    m_message_text.set(copy, cond->m_message_text.length(),
                       error_message_charset_info);
  }
  else
    m_message_text.length(0);

  DBUG_ASSERT(! m_message_text.is_alloced());

  m_message_text_set= cond->m_message_text_set;
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
  m_sql_errno= cond->m_sql_errno;
  m_level= cond->m_level;
  m_flags= cond->m_flags;
}

void
SQL_condition::set_printf(THD *thd, uint code, const char *str,
                          MYSQL_ERROR::enum_warning_level level,
                          myf flags, ...)
{
  va_list args;
  char ebuff[ERRMSGSIZE+20];


  DBUG_ENTER("SQL_condition::set_printf");

  va_start(args, flags);
  (void) my_vsnprintf (ebuff, sizeof(ebuff), str, args);
  va_end(args);

  set(thd, code, ebuff, level, flags);

  DBUG_VOID_RETURN;
}

void
SQL_condition::set(THD *thd, uint code, const char *str,
                   MYSQL_ERROR::enum_warning_level level, myf flags)
{
  const char* sqlstate;

  /*
    TODO: replace by DBUG_ASSERT(code != 0) once all bugs similar to
    Bug#36760 are fixed: a SQL condition must have a real (!=0) error number
    so that it can be caught by handlers.
  */
  if (code == 0)
    code= ER_UNKNOWN_ERROR;
  if (str == NULL)
    str= ER(code);
  m_sql_errno= code;

  sqlstate= mysql_errno_to_sqlstate(m_sql_errno);
  memcpy(m_returned_sqlstate, sqlstate, SQLSTATE_LENGTH);
  m_returned_sqlstate[SQLSTATE_LENGTH]= '\0';

  set_builtin_message_text(str);
  if ((level == MYSQL_ERROR::WARN_LEVEL_WARN) &&
      thd->really_abort_on_warning())
  {
    /*
      FIXME:
      push_warning and strict SQL_MODE case.
    */
    m_level= MYSQL_ERROR::WARN_LEVEL_ERROR;
    thd->killed= THD::KILL_BAD_DATA;
  }
  else
    m_level= level;
  m_flags= flags;
}

void
SQL_condition::set_builtin_message_text(const char* str)
{
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

int Abstract_signal::eval_sqlcode_sqlstate(THD *thd, SQL_condition *cond)
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

  return 0;
}

int Abstract_signal::eval_default_message_text(THD *thd, SQL_condition *cond)
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

  return 0;
}

int assign_condition_item(const char* name, THD *thd, Item *set,
                          UTF8String64 *ci)
{
  String str_value;
  String *str;

  DBUG_ENTER("assign_condition_item");

  if (set->is_null())
  {
    my_error(ER_WRONG_VALUE_FOR_VAR, MYF(0), name, "NULL");
    DBUG_RETURN(1);
  }

  str= set->val_str(& str_value);
  ci->set(str);
  if (ci->is_truncated())
  {
    if (thd->variables.sql_mode & (MODE_STRICT_TRANS_TABLES |
                                   MODE_STRICT_ALL_TABLES))
    {
      my_error(ER_COND_ITEM_TOO_LONG, MYF(0), name);
      DBUG_RETURN(1);
    }

    push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                        WARN_COND_ITEM_TRUNCATED,
                        ER(WARN_COND_ITEM_TRUNCATED),
                        name);
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
      my_error(ER_WRONG_VALUE_FOR_VAR, MYF(0), "MESSAGE_TEXT", "NULL");
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
        my_error(ER_COND_ITEM_TOO_LONG, MYF(0), "MESSAGE_TEXT");
        goto end;
      }

      push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                          WARN_COND_ITEM_TRUNCATED,
                          ER(WARN_COND_ITEM_TRUNCATED),
                          "MESSAGE_TEXT");
    }

    /*
      Convert to the character set used with --language.
      This code should be removed when WL#751 is implemented.
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
      my_error(ER_WRONG_VALUE_FOR_VAR, MYF(0), "MYSQL_ERRNO", "NULL");
      goto end;
    }
    longlong code= set->val_int();
    if ((code <= 0) || (code > MAX_MYSQL_ERRNO))
    {
      str= set->val_str(& str_value);
      my_error(ER_WRONG_VALUE_FOR_VAR, MYF(0),
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
    if (eval_sqlcode_sqlstate(thd, cond))
      DBUG_RETURN(result);
  }

  if (eval_signal_informations(thd, cond))
    DBUG_RETURN(result);

  if (eval_default_message_text(thd, cond))
    DBUG_RETURN(result);

  /* SIGNAL should not signal WARN_LEVEL_NOTE */
  DBUG_ASSERT((cond->m_level == MYSQL_ERROR::WARN_LEVEL_WARN) ||
              (cond->m_level == MYSQL_ERROR::WARN_LEVEL_ERROR));

  thd->raise_condition(cond);

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

  thd->main_da.reset_diagnostics_area();
  thd->row_count_func= 0;
  mysql_reset_errors(thd, TRUE);

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
    my_error(ER_RESIGNAL_NO_HANDLER, MYF(0));
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
  while ((thd->main_da.m_stmt_area.warn_list.elements > 0) &&
         ((thd->main_da.m_stmt_area.warn_list.elements + 2)
                       > thd->variables.max_error_count))
  {
    thd->main_da.m_stmt_area.warn_list.pop();
  }

  thd->raise_condition_no_handler(signaled);

  SQL_condition new_cond(thd->mem_root);
  new_cond.deep_copy(signaled);
  result= raise_condition(thd, & new_cond);
  DBUG_RETURN(result);
}

