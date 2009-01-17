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

Set_signal_information::Set_signal_information()
{
  clear();
}

Set_signal_information::Set_signal_information(
  const Set_signal_information& set)
{
  memcpy(m_item, set.m_item, sizeof(m_item));
}

void Set_signal_information::clear()
{
  memset(m_item, 0, sizeof(m_item));
}

void Abstract_signal::assign_defaults(MYSQL_ERROR *cond,
                                      bool set_level_code,
                                      MYSQL_ERROR::enum_warning_level level,
                                      int sqlcode)
{
  if (set_level_code)
  {
    cond->m_level= level;
    cond->m_sql_errno= sqlcode;
  }
  if (! cond->get_message_text())
    cond->set_builtin_message_text(ER(sqlcode));
}

void Abstract_signal::eval_defaults(THD *thd, MYSQL_ERROR *cond)
{
  DBUG_ASSERT(cond);

  const char* sqlstate;
  bool set_defaults= (m_cond != 0);

  if (set_defaults)
  {
    /*
      SIGNAL is restricted in sql_yacc.yy to only signal SQLSTATE conditions.
    */
    DBUG_ASSERT(m_cond->type == sp_cond_type::state);
    sqlstate= m_cond->sqlstate;
    cond->set_sqlstate(sqlstate);
  }
  else
    sqlstate= cond->get_sqlstate();

  DBUG_ASSERT(sqlstate);
  /* SQLSTATE class "00": illegal, rejected in the parser. */
  DBUG_ASSERT((sqlstate[0] != '0') || (sqlstate[1] != '0'));

  if ((sqlstate[0] == '0') && (sqlstate[1] == '1'))
  {
    /* SQLSTATE class "01": warning. */
    assign_defaults(cond, set_defaults,
                    MYSQL_ERROR::WARN_LEVEL_WARN, ER_SIGNAL_WARN);
  }
  else if ((sqlstate[0] == '0') && (sqlstate[1] == '2'))
  {
    /* SQLSTATE class "02": not found. */
    assign_defaults(cond, set_defaults,
                    MYSQL_ERROR::WARN_LEVEL_ERROR, ER_SIGNAL_NOT_FOUND);
  }
  else
  {
    /* other SQLSTATE classes : error. */
    assign_defaults(cond, set_defaults,
                    MYSQL_ERROR::WARN_LEVEL_ERROR, ER_SIGNAL_EXCEPTION);
  }
}

static int assign_condition_item(const char* name, THD *thd, Item *set,
                                 UTF8String64 *ci)
{
  char str_buff[(64+1)*4]; /* Room for a null terminated UFT8String64 */
  String str_value(str_buff, sizeof(str_buff), & my_charset_utf8_bin);
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


int Abstract_signal::eval_signal_informations(THD *thd, MYSQL_ERROR *cond)
{
  struct cond_item_map
  {
    enum enum_diag_condition_item_name m_item;
    UTF8String64 MYSQL_ERROR::*m_member;
  };

  static cond_item_map map[]=
  {
    { DIAG_CLASS_ORIGIN, & MYSQL_ERROR::m_class_origin },
    { DIAG_SUBCLASS_ORIGIN, & MYSQL_ERROR::m_subclass_origin },
    { DIAG_CONSTRAINT_CATALOG, & MYSQL_ERROR::m_constraint_catalog },
    { DIAG_CONSTRAINT_SCHEMA, & MYSQL_ERROR::m_constraint_schema },
    { DIAG_CONSTRAINT_NAME, & MYSQL_ERROR::m_constraint_name },
    { DIAG_CATALOG_NAME, & MYSQL_ERROR::m_catalog_name },
    { DIAG_SCHEMA_NAME, & MYSQL_ERROR::m_schema_name },
    { DIAG_TABLE_NAME, & MYSQL_ERROR::m_table_name },
    { DIAG_COLUMN_NAME, & MYSQL_ERROR::m_column_name },
    { DIAG_CURSOR_NAME, & MYSQL_ERROR::m_cursor_name }
  };

  Item *set;
  String str_value;
  String *str;
  int i;
  uint j;
  int result= 1;
  enum enum_diag_condition_item_name item_enum;
  UTF8String64 *member;
  const LEX_STRING *name;

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

  /*
    Generically assign all the UTF8String64 condition items
    described in the map.
  */
  for (j= 0; j < sizeof(map)/sizeof(map[0]); j++)
  {
    item_enum= map[j].m_item;
    set= m_set_signal_information.m_item[item_enum];
    if (set != NULL)
    {
      member= & (cond->* map[j].m_member);
      name= & Diag_condition_item_names[item_enum];
      if (assign_condition_item(name->str, thd, set, member))
        goto end;
    }
  }

  /*
    Assign the remaining attributes.
  */

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
       "Design notes about MYSQL_ERROR::m_message_text."
      in file sql_error.cc
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

int Abstract_signal::raise_condition(THD *thd, MYSQL_ERROR *cond)
{
  int result= 1;

  DBUG_ENTER("Abstract_signal::raise_condition");

  DBUG_ASSERT(m_lex->query_tables == NULL);

  eval_defaults(thd, cond);
  if (eval_signal_informations(thd, cond))
    DBUG_RETURN(result);

  /* SIGNAL should not signal WARN_LEVEL_NOTE */
  DBUG_ASSERT((cond->m_level == MYSQL_ERROR::WARN_LEVEL_WARN) ||
              (cond->m_level == MYSQL_ERROR::WARN_LEVEL_ERROR));

  MYSQL_ERROR *raised= NULL;
  raised= thd->raise_condition(cond->get_sql_errno(),
                               cond->get_sqlstate(),
                               cond->get_level(),
                               cond->get_message_text());
  if (raised)
    raised->copy_opt_attributes(cond);

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
  MYSQL_ERROR cond(thd->mem_root);

  DBUG_ENTER("SQLCOM_signal::execute");

  thd->stmt_da->reset_diagnostics_area();
  thd->row_count_func= 0;
  thd->warning_info->clear_warning_info(thd->query_id);

  result= raise_condition(thd, &cond);

  DBUG_RETURN(result);
}


int SQLCOM_resignal::execute(THD *thd)
{
  MYSQL_ERROR *signaled;
  int result= 1;

  DBUG_ENTER("SQLCOM_resignal::execute");

  thd->warning_info->m_warn_id= thd->query_id;

  if (! thd->spcont || ! (signaled= thd->spcont->raised_condition()))
  {
    thd->raise_error(ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER);
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

  MYSQL_ERROR *raised= NULL;
  raised= thd->raise_condition_no_handler(signaled->get_sql_errno(),
                                          signaled->get_sqlstate(),
                                          signaled->get_level(),
                                          signaled->get_message_text());
  if (raised)
    raised->copy_opt_attributes(signaled);

  result= raise_condition(thd, signaled);

  DBUG_RETURN(result);
}

