#ifndef SQL_PREPARE_H
#define SQL_PREPARE_H
/* Copyright (C) 1995-2008 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA */

#include "my_global.h"
#include "m_string.h"

class THD;
struct LEX;
template <typename T> class List;

/**
  An interface that is used to take an action when
  the locking module notices that a table version has changed
  since the last execution. "Table" here may refer to any kind of
  table -- a base table, a temporary table, a view or an
  information schema table.

  When we open and lock tables for execution of a prepared
  statement, we must verify that they did not change
  since statement prepare. If some table did change, the statement
  parse tree *may* be no longer valid, e.g. in case it contains
  optimizations that depend on table metadata.

  This class provides an interface (a method) that is
  invoked when such a situation takes place.
  The implementation of the method simply reports an error, but
  the exact details depend on the nature of the SQL statement.

  At most 1 instance of this class is active at a time, in which
  case THD::m_reprepare_observer is not NULL.

  @sa check_and_update_table_version() for details of the
  version tracking algorithm 

  @sa Open_tables_state::m_reprepare_observer for the life cycle
  of metadata observers.
*/

class Reprepare_observer
{
public:
  /**
    Check if a change of metadata is OK. In future
    the signature of this method may be extended to accept the old
    and the new versions, but since currently the check is very
    simple, we only need the THD to report an error.
  */
  bool report_error(THD *thd);
  bool is_invalidated() const { return m_invalidated; }
  void reset_reprepare_observer() { m_invalidated= FALSE; }
private:
  bool m_invalidated;
};


void mysql_stmt_prepare(THD *thd, const char *packet, uint packet_length);
void mysql_stmt_execute(THD *thd, char *packet, uint packet_length);
void mysql_stmt_close(THD *thd, char *packet);
void mysql_sql_stmt_prepare(THD *thd);
void mysql_sql_stmt_execute(THD *thd);
void mysql_sql_stmt_close(THD *thd);
void mysql_stmt_fetch(THD *thd, char *packet, uint packet_length);
void mysql_stmt_reset(THD *thd, char *packet);
void mysql_stmt_get_longdata(THD *thd, char *pos, ulong packet_length);
void reinit_stmt_before_use(THD *thd, LEX *lex);

/**
  Execute a fragment of server code in an isolated context, so that
  it doesn't leave any effect on THD. THD must have no open tables.
  The code must not leave any open tables around.
  The result of execution (if any) is stored in Ed_result.
*/

class Server_runnable
{
public:
  virtual bool execute_server_code(THD *thd)= 0;
  virtual ~Server_runnable();
};

class Ed_result;

bool
mysql_execute_direct(THD *thd, LEX_STRING query, Ed_result *ed_result);
bool
mysql_execute_direct(THD *thd, Server_runnable *server_runnable,
                     Ed_result *ed_result);

#endif // SQL_PREPARE_H
