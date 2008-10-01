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

#ifndef SQL_SIGNAL_H
#define SQL_SIGNAL_H

/**
  Abstract_signal represents the common properties of the SIGNAL and RESIGNAL
  statements.
*/
class Abstract_signal : public SQLCOM_statement
{
protected:
  /**
    Constructor.
    @param lex the LEX structure for this statement.
    @param cond the condition signaled if any, or NULL.
    @param set collection of signal condition item assignments.
  */
  Abstract_signal(LEX *lex,
                  const sp_cond_type_t *cond,
                  const Set_signal_information& set)
    : SQLCOM_statement(lex),
      m_cond(cond),
      m_set_signal_information(set)
  {}

  virtual ~Abstract_signal()
  {}

  /**
    Evaluate the condition items 'MYSQL_ERRNO', 'RETURNED_SQLSTATE' and 'level'
    default values for this statement.
    @param thd the current thread.
    @param cond the condition to update.
  */
  void eval_sqlcode_sqlstate(THD *thd, SQL_condition *cond);

  /**
    Evaluate condition item 'MESSAGE_TEXT' default value.
    @param thd the current thread.
    @param cond the condition to update.
  */
  void eval_default_message_text(THD *thd, SQL_condition *cond);

  /**
    Evaluate each signal condition items for this statement.
    @param thd the current thread.
    @param cond the condition to update.
    @return 0 on success.
  */
  int eval_signal_informations(THD *thd, SQL_condition *cond);

  /**
    Raise a SQL condition.
    @param thd the current thread.
    @param cond the condition to raise.
    @return 0 on success.
  */
  int raise_condition(THD *thd, SQL_condition *cond);

  /**
    The condition to signal or resignal.
    This member is optional and can be NULL (RESIGNAL).
  */
  const sp_cond_type_t *m_cond;

  /**
    Collection of 'SET item = value' assignments in the
    SIGNAL/RESIGNAL statement.
  */
  Set_signal_information m_set_signal_information;
};

/**
  SQLCOM_signal represents a SIGNAL statement.
*/
class SQLCOM_signal : public Abstract_signal
{
public:
  /**
    Constructor, used to represent a SIGNAL statement.
    @param lex the LEX structure for this statement.
    @param cond the SQL condition to signal (required).
    @param set the collection of signal informations to signal.
  */
  SQLCOM_signal(LEX *lex,
                const sp_cond_type_t *cond,
                const Set_signal_information& set)
    : Abstract_signal(lex, cond, set)
  {}

  virtual ~SQLCOM_signal()
  {}

  /**
    Execute a SIGNAL statement at runtime.
    @param thd the current thread.
    @return 0 on success.
  */
  virtual int execute(THD *thd);
};

/**
  SQLCOM_resignal represents a RESIGNAL statement.
*/
class SQLCOM_resignal : public Abstract_signal
{
public:
  /**
    Constructor, used to represent a RESIGNAL statement.
    @param lex the LEX structure for this statement.
    @param cond the SQL condition to resignal (optional, may be NULL).
    @param set the collection of signal informations to resignal.
  */
  SQLCOM_resignal(LEX *lex,
                  const sp_cond_type_t *cond,
                  const Set_signal_information& set)
    : Abstract_signal(lex, cond, set)
  {}

  virtual ~SQLCOM_resignal()
  {}

  /**
    Execute a RESIGNAL statement at runtime.
    @param thd the current thread.
    @return 0 on success.
  */
  virtual int execute(THD *thd);
};

#endif

