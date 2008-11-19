/**
   @file

   This file defines the API for the following object services:
     - serialize database objects into a string;
     - materialize (deserialize) object from a string;
     - enumerating objects;
     - finding dependencies for objects;
     - executor for SQL statements;
     - wrappers for controlling the DDL Blocker;

  The methods defined below are used to provide server functionality to
  and permitting an isolation layer for the client (caller).
*/

#include "mysql_priv.h"
#include "si_objects.h"
#include "ddl_blocker.h"
#include "sql_show.h"
#ifdef HAVE_EVENT_SCHEDULER
#include "events.h"
#include "event_data_objects.h"
#include "event_db_repository.h"
#endif
#include "sql_trigger.h"
#include "sp.h"
#include "sp_head.h" // for sp_add_to_query_tables().

DDL_blocker_class *DDL_blocker= NULL;

#define QUERY_BUFFER_SIZE 4096

///////////////////////////////////////////////////////////////////////////

namespace {

///////////////////////////////////////////////////////////////////////////
//
// Type identifiers in INFORMATION_SCHEMA.
//
///////////////////////////////////////////////////////////////////////////

const LEX_STRING IS_TYPE_TABLE= { C_STRING_WITH_LEN("BASE TABLE") };
const LEX_STRING IS_TYPE_VIEW=  { C_STRING_WITH_LEN("VIEW") };

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

bool run_query(THD *thd, const LEX_STRING *query, Ed_result *result)
{
  DBUG_ENTER("run_query()");
  DBUG_PRINT("run_query",
             ("query: %.*s",
              (int) query->length, (const char *) query->str));

  ulong sql_mode_saved= thd->variables.sql_mode;
  CHARSET_INFO *client_cs_saved= thd->variables.character_set_client;
  CHARSET_INFO *results_cs_saved= thd->variables.character_set_results;
  CHARSET_INFO *connection_cl_saved= thd->variables.collation_connection;

  thd->variables.sql_mode= 0;

  /*
    Temporary tables should be ignored while looking for table structures.
    Backup wants to backup ordinary tables, not temporary ones.
  */
  TABLE *tmp_tables_saved= thd->temporary_tables;
  thd->temporary_tables= NULL;

  /* A query is in UTF8 (internal character set). */
  thd->variables.character_set_client= system_charset_info;

  /*
    Ed_results should be fetched without any conversion (in the original
    character set) in order to preserve object definition query intact.
  */
  thd->variables.character_set_results= &my_charset_bin;
  thd->variables.collation_connection= system_charset_info;
  thd->update_charset();

  bool rc= mysql_execute_direct(thd, query, result);

  thd->variables.sql_mode= sql_mode_saved;
  thd->variables.collation_connection= connection_cl_saved;
  thd->variables.character_set_results= results_cs_saved;
  thd->variables.character_set_client= client_cs_saved;
  thd->update_charset();

  thd->temporary_tables= tmp_tables_saved;

  DBUG_RETURN(rc);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

struct Table_name_key
{
public:
  static uchar * get_key(const uchar *record,
                         size_t *key_length,
                         my_bool not_used __attribute__((unused)));

  static void delete_key(void *data);

public:
  Table_name_key(const char *db_name_str,
                 uint db_name_length,
                 const char *table_name_str,
                 uint table_name_length)
  {
    db_name.copy(db_name_str, db_name_length, system_charset_info);
    table_name.copy(table_name_str, table_name_length, system_charset_info);

    key.length(0);
    key.append(db_name);
    key.append(".");
    key.append(table_name);
  }

public:
  String db_name;
  String table_name;

  String key;
};

///////////////////////////////////////////////////////////////////////////

uchar *Table_name_key::get_key(const uchar *record,
                               size_t *key_length,
                               my_bool not_used __attribute__((unused)))
{
  Table_name_key *tnk= (Table_name_key *) record;
  *key_length= tnk->key.length();
  return (uchar *) tnk->key.c_ptr_safe();
}

///////////////////////////////////////////////////////////////////////////

void Table_name_key::delete_key(void *data)
{
  Table_name_key *tnk= (Table_name_key *) data;
  delete tnk;
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Fmt
{
public:
  Fmt(const char *format, ...);

public:
  const char *str() const { return m_buffer; }
  int length() const { return m_length; }

private:
  char m_buffer[QUERY_BUFFER_SIZE];
  int m_length;
};

///////////////////////////////////////////////////////////////////////////

Fmt::Fmt(const char *format, ...)
{
  va_list args;

  va_start(args, format);
  m_length= my_vsnprintf(m_buffer, sizeof (m_buffer), format, args);
  va_end(args);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Out_stream
{
public:
  Out_stream(String *serialization) :
    m_serialization(serialization)
  { }

public:
  Out_stream &operator <<(const char *query);
  Out_stream &operator <<(const LEX_STRING *query);
  Out_stream &operator <<(const String &query);
  Out_stream &operator <<(const Fmt &query);

private:
  String *m_serialization;
};

///////////////////////////////////////////////////////////////////////////

Out_stream &Out_stream::operator <<(const char *query)
{
  LEX_STRING str= { (char *) query, strlen(query) };
  return Out_stream::operator <<(&str);
}

///////////////////////////////////////////////////////////////////////////

Out_stream &Out_stream::operator <<(const LEX_STRING *query)
{
  char chunk_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING chunk;

  chunk.str= chunk_buffer;
  chunk.length= my_snprintf(chunk_buffer, QUERY_BUFFER_SIZE,
    "%d %.*s\n",
    (int) query->length,
    (int) query->length,
    (const char *) query->str);

  m_serialization->append(chunk.str, chunk.length);

  return *this;
}

///////////////////////////////////////////////////////////////////////////

Out_stream &Out_stream::operator <<(const String &query)
{
  LEX_STRING str= { (char *) query.ptr(), query.length() };
  return Out_stream::operator <<(&str);
}

///////////////////////////////////////////////////////////////////////////

Out_stream &Out_stream::operator <<(const Fmt &query)
{
  LEX_STRING str= { (char *) query.str(), query.length() };
  return Out_stream::operator <<(&str);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class In_stream
{
public:
  In_stream(uint serialization_version,
            const String *serialization) :
    m_serialization_version(serialization_version),
    m_serialization(serialization),
    m_read_ptr(m_serialization->ptr()),
    m_end_ptr(m_serialization->ptr() + m_serialization->length())
  { }

public:
  uint serialization_version() const { return m_serialization_version; }
public:
  bool next(LEX_STRING *chunk);

private:
  uint m_serialization_version;
  const String *m_serialization;
  const char *m_read_ptr;
  const char *m_end_ptr;
};

///////////////////////////////////////////////////////////////////////////

bool In_stream::next(LEX_STRING *chunk)
{
  if (m_read_ptr >= m_end_ptr)
    return TRUE;

  const char *delimiter_ptr=
    my_strchr(system_charset_info, m_read_ptr, m_end_ptr, ' ');

  if (!delimiter_ptr)
  {
    m_read_ptr= m_end_ptr;
    return TRUE;
  }

  char buffer[STRING_BUFFER_USUAL_SIZE];
  int n= delimiter_ptr - m_read_ptr;

  memcpy(buffer, m_read_ptr, n);
  buffer[n]= 0;

  chunk->str= (char *) delimiter_ptr + 1;
  chunk->length= atoi(buffer);

  m_read_ptr+= n /* chunk length */
               + 1 /* delimiter (a space) */
               + chunk->length /* chunk */
               + 1; /* chunk delimiter (\n) */

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

#define STR(x) x.length(), x.ptr()
#define LXS(x) x->length, x->str

///////////////////////////////////////////////////////////////////////////

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

namespace obs {

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Abstract_obj : public Obj
{
public:
  virtual bool serialize(THD *thd, String *serialization);

  virtual bool execute(THD *thd);

protected:
  virtual bool materialize(uint serialization_version,
                           const String *serialization);

  virtual bool do_materialize(In_stream *is);

  /**
    Primitive implementing @c serialize() method.
  */
  virtual bool do_serialize(THD *thd, Out_stream &os) = 0;

protected:
  MEM_ROOT m_mem_root; /* This mem-root is for keeping stmt list. */
  List<String> m_stmt_lst;

protected:
  Abstract_obj();
  virtual ~Abstract_obj();

private:
  Abstract_obj(const Abstract_obj &);
  Abstract_obj &operator =(const Abstract_obj &);
};

///////////////////////////////////////////////////////////////////////////

Abstract_obj::Abstract_obj()
{
  init_sql_alloc(&m_mem_root, ALLOC_ROOT_MIN_BLOCK_SIZE, 0);
}

///////////////////////////////////////////////////////////////////////////

Abstract_obj::~Abstract_obj()
{
  free_root(&m_mem_root, MYF(0));
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize object state into a buffer. The buffer actually should be a
  binary buffer. String class is used here just because we don't have
  convenient primitive for binary buffers.

  Serialization format is opaque to the client, i.e. the client should
  not make any assumptions about the format or the content of the
  returned buffer.

  Serialization format can be changed in the future versions. However,
  the server must be able to materialize objects coded in any previous
  formats.

  @param[in] thd              Server thread context.
  @param[in] serialization Buffer to serialize the object

  @return error status.
    @retval FALSE on success.
    @retval TRUE on error.

  @note The real work is done inside @c do_serialize() primitive which should be
  defied in derived classes. This method prepares appropriate context and calls
  the primitive.
*/

bool Abstract_obj::serialize(THD *thd, String *serialization)
{
  ulong saved_sql_mode= thd->variables.sql_mode;
  thd->variables.sql_mode= 0;

  Out_stream os(serialization);

  bool ret= do_serialize(thd, os);

  thd->variables.sql_mode= saved_sql_mode;

  return ret;
}

///////////////////////////////////////////////////////////////////////////

/**
  Create the object in the database.

  @param[in] thd              Server thread context.

  @return error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Abstract_obj::execute(THD *thd)
{
  /*
    Save and update session sql_mode.

    Although backup queries can reset it by itself, we should be able to
    run at least "set" statement. Backup queries are generated using
    sql_mode == 0, so we also should use it. If a query needs another
    sql_mode (stored rountines), it will reset it once more.
  */

  ulong saved_sql_mode= thd->variables.sql_mode;
  thd->variables.sql_mode= 0;

  /*
    NOTE: other session variables are not preserved, so backup query must
    take care to clean up the environment after itself.
  */

  bool rc;
  List_iterator_fast<String> it(m_stmt_lst);
  while (true)
  {
    String *stmt= it++;

    if (!stmt)
      break;

    LEX_STRING query= { (char *) stmt->ptr(), stmt->length() };
    Ed_result result(thd->mem_root);
    DBUG_ASSERT(alloc_root_inited(thd->mem_root));

    rc= mysql_execute_direct(thd, &query, &result);

    /* Ignore warnings from materialization for now. */

    if (rc)
      break;
  }

  thd->variables.sql_mode= saved_sql_mode;

  return rc == TRUE;
}

///////////////////////////////////////////////////////////////////////////

bool Abstract_obj::materialize(uint serialization_version,
                               const String *serialization)
{
  m_stmt_lst.delete_elements();

  In_stream is(serialization_version, serialization);

  return do_materialize(&is);
}

///////////////////////////////////////////////////////////////////////////

bool Abstract_obj::do_materialize(In_stream *is)
{
  while (true)
  {
    LEX_STRING stmt;

    if (is->next(&stmt))
      break;

    String *s= new (&m_mem_root) String();

    if (!s ||
        s->copy(stmt.str, stmt.length, system_charset_info) ||
        m_stmt_lst.push_back(s))
      return TRUE;
  }

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
   @class Database_obj

   This class provides an abstraction to a database object for creation and
   capture of the creation data.
*/

class Database_obj : public Abstract_obj
{
public:
  Database_obj(const char *db_name_str, int db_name_length);

public:
  virtual inline const String *get_name() const    { return &m_db_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////

/**
   @class Table_obj

   This class provides an abstraction to a table object for creation and
   capture of the creation data.
*/

class Table_obj : public Abstract_obj
{
public:
  Table_obj(const char *db_name_str, int db_name_length,
           const char *table_name_str, int table_name_length);

public:
  virtual inline const String *get_name() const    { return &m_table_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;
  String m_table_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////

/**
   @class View_obj

   This class provides an abstraction to a view object for creation and
   capture of the creation data.
*/

class View_obj : public Abstract_obj
{
public:
  View_obj(const char *db_name_str, int db_name_length,
           const char *view_name_str, int view_name_length);

public:
  virtual inline const String *get_name() const    { return &m_view_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;
  String m_view_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////

/**
  @class Trigger_obj

  This class provides an abstraction to a trigger object for creation and
  capture of the creation data.
*/

class Trigger_obj : public Abstract_obj
{
public:
  Trigger_obj(const char *db_name_str, int db_name_length,
             const char *trigger_name_str, int trigger_name_length);

public:
  virtual inline const String *get_name() const    { return &m_trigger_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;
  String m_trigger_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////

/**
  @class Stored_proc_obj

  This class provides an abstraction to a stored procedure object for creation
  and capture of the creation data.
*/

class Stored_proc_obj : public Abstract_obj
{
public:
  Stored_proc_obj(const char *db_name_str, int db_name_length,
                const char *sp_name_str, int sp_name_length);

public:
  virtual inline const String *get_name() const    { return &m_sp_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;
  String m_sp_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////

/**
  @class Stored_func_obj

  This class provides an abstraction to a stored function object for creation
  and capture of the creation data.
*/

class Stored_func_obj : public Abstract_obj
{
public:
  Stored_func_obj(const char *db_name_str, int db_name_length,
                const char *sf_name_str, int sf_name_length);

public:
  virtual inline const String *get_name() const    { return &m_sf_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;
  String m_sf_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////

#ifdef HAVE_EVENT_SCHEDULER

/**
  @class Event_obj

  This class provides an abstraction to a event object for creation and capture
  of the creation data.
*/

class Event_obj : public Abstract_obj
{
public:
  Event_obj(const char *db_name_str, int db_name_length,
           const char *event_name_str, int event_name_length);

public:
  virtual inline const String *get_name() const    { return &m_event_name; }
  virtual inline const String *get_db_name() const { return &m_db_name; }

private:
  /* These attributes are to be used only for serialization. */
  String m_db_name;
  String m_event_name;

  virtual bool do_serialize(THD *thd, Out_stream &os);
};

#endif // HAVE_EVENT_SCHEDULER

///////////////////////////////////////////////////////////////////////////

/**
  @class Tablespace_obj

  This class provides an abstraction to a user object for creation and
  capture of the creation data.
*/

class Tablespace_obj : public Abstract_obj
{
public:
  Tablespace_obj(const char *ts_name_str, int ts_name_length,
                 const char *comment_str, int comment_length,
                 const char *data_file_name_str, int data_file_name_length,
                 const char *engine_str, int engine_length);

  Tablespace_obj(const char *ts_name_str, int ts_name_length);

public:
  virtual inline const String *get_name() const    { return &m_ts_name; }
  virtual inline const String *get_db_name() const { return NULL; }

  const String *get_description();

protected:
  virtual bool do_serialize(THD *thd, Out_stream &os);

  virtual bool materialize(uint serialization_version,
                           const String *serialization);

private:
  /* These attributes are to be used only for serialization. */
  String m_ts_name;
  String m_comment;
  String m_data_file_name;
  String m_engine;

private:
  String m_description;
};

///////////////////////////////////////////////////////////////////////////

/**
  @class Grant_obj

  This class provides an abstraction to grants. This class will permit the
  recording and replaying of these grants.
*/

class Grant_obj : public Abstract_obj
{
public:
  static void generate_unique_id(const String *user_name,
                                 const String *host_name,
                                 String *id);

public:
  Grant_obj(const char *id_str, int id_length);

  Grant_obj(const char *user_name_str, int user_name_length,
            const char *host_name_str, int host_name_length,
            const char *priv_type_str, int priv_type_length,
            const char *db_name_str, int db_name_length,
            const char *table_name_str, int table_name_length,
            const char *column_name_str, int column_name_length);

public:
  virtual bool do_materialize(In_stream *is);

public:
  virtual inline const String *get_name() const    { return &m_id; }
  virtual inline const String *get_db_name() const { return &m_id; }

  inline const String *get_user_name() const { return &m_user_name; }
  inline const String *get_host_name() const { return &m_host_name; }
  inline const String *get_grant_info() const { return &m_grant_info; }

protected:
  /* These attributes are to be used only for serialization. */
  String m_id;      ///< identify grant object (grantee is not unique).
  String m_user_name;
  String m_host_name;
  String m_grant_info;

private:
  virtual bool do_serialize(THD *thd, Out_stream &os);
};

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

template <typename Iterator>
Iterator *create_row_set_iterator(THD *thd, const LEX_STRING *query)
{
  Ed_result result(thd->mem_root);

  if (run_query(thd, query, &result) ||
      result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return NULL;
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  return new Iterator(rs);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Ed_result_set_iterator : public Obj_iterator
{
protected:
  inline Ed_result_set_iterator(Ed_result_set *rs);

protected:
  Ed_result_set *m_rs;
  List_iterator_fast<Ed_row> m_row_it;
};

///////////////////////////////////////////////////////////////////////////

inline Ed_result_set_iterator::Ed_result_set_iterator(Ed_result_set *rs)
  : m_rs(rs),
    m_row_it(*rs->data())
{ }

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Database_iterator : public Ed_result_set_iterator
{
public:
  static Database_iterator *create(THD *thd);

public:
  inline Database_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Database_iterator *Database_iterator::create(THD *thd)
{
  LEX_STRING query= { C_STRING_WITH_LEN(
    "SELECT schema_name "
    "FROM INFORMATION_SCHEMA.SCHEMATA "
    "WHERE LCASE(schema_name) != 'mysql' AND "
          "LCASE(schema_name) != 'information_schema'") };

  return create_row_set_iterator<Database_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

template
Database_iterator *
create_row_set_iterator<Database_iterator>(THD *thd, const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////

Obj *Database_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 1);
  const Ed_column *db_name= row->get_column(0);

  return new Database_obj(db_name->str, db_name->length);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Db_tables_iterator : public Ed_result_set_iterator
{
public:
  static Db_tables_iterator *create(THD *thd, const String *db_name);

public:
  inline Db_tables_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Db_tables_iterator *Db_tables_iterator::create(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT '%.*s', table_name "
    "FROM INFORMATION_SCHEMA.TABLES "
    "WHERE table_schema = '%.*s' AND table_type = '%.*s'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) IS_TYPE_TABLE.length,
    (const char *) IS_TYPE_TABLE.str);

  return create_row_set_iterator<Db_tables_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

template
Db_tables_iterator *
create_row_set_iterator<Db_tables_iterator>(THD *thd, const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////

Obj *Db_tables_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *db_name= row->get_column(0);
  const Ed_column *table_name= row->get_column(1);

  return new Table_obj(db_name->str, db_name->length,
                       table_name->str, table_name->length);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Db_views_iterator : public Ed_result_set_iterator
{
public:
  static Db_views_iterator *create(THD *thd, const String *db_name);

public:
  inline Db_views_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Db_views_iterator *Db_views_iterator::create(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT '%.*s', table_name "
    "FROM INFORMATION_SCHEMA.TABLES "
    "WHERE table_schema = '%.*s' AND table_type = '%.*s'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) IS_TYPE_VIEW.length,
    (const char *) IS_TYPE_VIEW.str);

  return create_row_set_iterator<Db_views_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

template
Db_views_iterator *
create_row_set_iterator<Db_views_iterator>(THD *thd, const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////

Obj *Db_views_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *db_name= row->get_column(0);
  const Ed_column *view_name= row->get_column(1);

  return new View_obj(db_name->str, db_name->length,
                      view_name->str, view_name->length);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Db_trigger_iterator : public Ed_result_set_iterator
{
public:
  static Db_trigger_iterator *create(THD *thd, const String *db_name);

public:
  inline Db_trigger_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Db_trigger_iterator *Db_trigger_iterator::create(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT '%.*s', trigger_name "
    "FROM INFORMATION_SCHEMA.TRIGGERS "
    "WHERE trigger_schema = '%.*s'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr());

  return create_row_set_iterator<Db_trigger_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

template
Db_trigger_iterator *
create_row_set_iterator<Db_trigger_iterator>(THD *thd, const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////

Obj *Db_trigger_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *db_name= row->get_column(0);
  const Ed_column *trigger_name= row->get_column(1);

  return new Trigger_obj(db_name->str, db_name->length,
                         trigger_name->str, trigger_name->length);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Db_stored_proc_iterator : public Ed_result_set_iterator
{
public:
  static Db_stored_proc_iterator *create(THD *thd, const String *db_name);

public:
  inline Db_stored_proc_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Db_stored_proc_iterator *
Db_stored_proc_iterator::create(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT '%.*s', routine_name "
    "FROM INFORMATION_SCHEMA.ROUTINES "
    "WHERE routine_schema = '%.*s' AND routine_type = 'PROCEDURE'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr());

  return create_row_set_iterator<Db_stored_proc_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

template
Db_stored_proc_iterator *
create_row_set_iterator<Db_stored_proc_iterator>(THD *thd,
                                                 const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////

Obj *Db_stored_proc_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *db_name= row->get_column(0);
  const Ed_column *routine_name= row->get_column(1);

  return new Stored_proc_obj(db_name->str, db_name->length,
                             routine_name->str, routine_name->length);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Db_stored_func_iterator : public Ed_result_set_iterator
{
public:
  static Db_stored_func_iterator *create(THD *thd, const String *db_name);

public:
  inline Db_stored_func_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Db_stored_func_iterator *
Db_stored_func_iterator::create(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT '%.*s', routine_name "
    "FROM INFORMATION_SCHEMA.ROUTINES "
    "WHERE routine_schema = '%.*s' AND routine_type = 'FUNCTION'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr());

  return create_row_set_iterator<Db_stored_func_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

template
Db_stored_func_iterator *
create_row_set_iterator<Db_stored_func_iterator>(THD *thd,
                                                 const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////

Obj *Db_stored_func_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *db_name= row->get_column(0);
  const Ed_column *routine_name= row->get_column(1);

  return new Stored_func_obj(db_name->str, db_name->length,
                             routine_name->str, routine_name->length);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Db_event_iterator : public Ed_result_set_iterator
{
public:
  static Db_event_iterator *create(THD *thd, const String *db_name);

public:
  inline Db_event_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Db_event_iterator *
Db_event_iterator::create(THD *thd, const String *db_name)
{
#ifdef HAVE_EVENT_SCHEDULER

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT '%.*s', event_name "
    "FROM INFORMATION_SCHEMA.EVENTS "
    "WHERE event_schema = '%.*s'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr());

  return create_row_set_iterator<Db_event_iterator>(thd, &query);

#else

  return NULL;

#endif
}

///////////////////////////////////////////////////////////////////////////

#ifdef HAVE_EVENT_SCHEDULER
template
Db_event_iterator *
create_row_set_iterator<Db_event_iterator>(THD *thd, const LEX_STRING *query);
#endif

///////////////////////////////////////////////////////////////////////////

Obj *Db_event_iterator::next()
{
#ifdef HAVE_EVENT_SCHEDULER

  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *db_name= row->get_column(0);
  const Ed_column *event_name= row->get_column(1);

  return new Event_obj(db_name->str, db_name->length,
                       event_name->str, event_name->length);

#else

  return NULL;

#endif
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class View_base_obj_iterator : public Obj_iterator
{
public:
  View_base_obj_iterator();
  virtual ~View_base_obj_iterator();

public:
  virtual Obj *next();

protected:
  template <typename Iterator>
  static Iterator *create(THD *thd,
                          const String *db_name, const String *view_name);

protected:
  bool init(THD *thd, const String *db_name, const String *view_name);

  virtual bool is_obj_accepted(TABLE_LIST *obj)= 0;
  virtual Obj *create_obj(const String *db_name, const String *obj_name)= 0;

private:
  HASH *m_table_names;
  uint m_cur_idx;
};

///////////////////////////////////////////////////////////////////////////

template <typename Iterator>
Iterator *View_base_obj_iterator::create(THD *thd,
                                         const String *db_name,
                                         const String *view_name)
{
  Iterator *it= new Iterator();

  if (it->init(thd, db_name, view_name))
  {
    delete it;
    return NULL;
  }

  return it;
}

///////////////////////////////////////////////////////////////////////////

View_base_obj_iterator::View_base_obj_iterator() :
  m_table_names(NULL),
  m_cur_idx(0)
{
}

///////////////////////////////////////////////////////////////////////////

View_base_obj_iterator::~View_base_obj_iterator()
{
  if (!m_table_names)
    return;

  hash_free(m_table_names);
  delete m_table_names;
}

///////////////////////////////////////////////////////////////////////////

bool View_base_obj_iterator::init(THD *thd,
                                  const String *db_name,
                                  const String *view_name)
{
  DBUG_ASSERT(!m_table_names);

  uint not_used; /* Passed to open_tables(). Not used. */
  THD *my_thd= new THD();

  my_thd->security_ctx= thd->security_ctx;

  my_thd->thread_stack= (char*) &my_thd;
  my_thd->store_globals();
  lex_start(my_thd);

  TABLE_LIST *tl =
    sp_add_to_query_tables(my_thd,
                           my_thd->lex,
                           ((String *) db_name)->c_ptr_safe(),
                           ((String *) view_name)->c_ptr_safe(),
                           TL_READ);

  if (open_tables(my_thd, &tl, &not_used, MYSQL_OPEN_SKIP_TEMPORARY))
  {
    close_thread_tables(my_thd);
    delete my_thd;
    thd->store_globals();

    return TRUE;
  }

  m_table_names = new HASH();

  hash_init(m_table_names, system_charset_info, 16, 0, 0,
            Table_name_key::get_key,
            Table_name_key::delete_key,
            MYF(0));

  if (tl->view_tables)
  {
    List_iterator_fast<TABLE_LIST> it(*tl->view_tables);
    TABLE_LIST *tl2;

    while ((tl2 = it++))
    {
      Table_name_key *tnk=
        new Table_name_key(tl2->db, tl2->db_length,
                           tl2->table_name, tl2->table_name_length);

      if (!is_obj_accepted(tl2) ||
          hash_search(m_table_names,
                      (uchar *) tnk->key.c_ptr_safe(),
                      tnk->key.length()))
      {
        delete tnk;
        continue;
      }

      my_hash_insert(m_table_names, (uchar *) tnk);
    }
  }

  close_thread_tables(my_thd);
  delete my_thd;

  thd->store_globals();

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

Obj *View_base_obj_iterator::next()
{
  if (m_cur_idx >= m_table_names->records)
    return NULL;

  Table_name_key *tnk=
    (Table_name_key *) hash_element(m_table_names, m_cur_idx);

  ++m_cur_idx;

  return create_obj(&tnk->db_name, &tnk->table_name);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class View_base_table_iterator : public View_base_obj_iterator
{
public:
  static View_base_obj_iterator *
  create(THD *thd, const String *db_name, const String *view_name)
  {
    return View_base_obj_iterator::create<View_base_table_iterator>
      (thd, db_name, view_name);
  }

protected:
  virtual bool is_obj_accepted(TABLE_LIST *obj)
  { return !obj->view; }

  virtual Obj *create_obj(const String *db_name, const String *obj_name)
  {
    return new Table_obj(db_name->ptr(), db_name->length(),
                         obj_name->ptr(), obj_name->length());
  }
};

///////////////////////////////////////////////////////////////////////////

template
View_base_table_iterator *
View_base_obj_iterator::create<View_base_table_iterator>(
  THD *thd, const String *db_name, const String *view_name);

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class View_base_view_iterator : public View_base_obj_iterator
{
public:
  static View_base_obj_iterator *
  create(THD *thd, const String *db_name, const String *view_name)
  {
    return View_base_obj_iterator::create<View_base_view_iterator>
      (thd, db_name, view_name);
  }

protected:
  virtual bool is_obj_accepted(TABLE_LIST *obj)
  { return obj->view; }

  virtual Obj *create_obj(const String *db_name, const String *obj_name)
  {
    return new View_obj(db_name->ptr(), db_name->length(),
                        obj_name->ptr(), obj_name->length());
  }
};

///////////////////////////////////////////////////////////////////////////

template
View_base_view_iterator *
View_base_obj_iterator::create<View_base_view_iterator>(
  THD *thd, const String *db_name, const String *view_name);

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Grant_iterator : public Ed_result_set_iterator
{
public:
  static Grant_iterator *create(THD *thd, const String *db_name);

public:
  inline Grant_iterator(Ed_result_set *rs)
    :Ed_result_set_iterator(rs)
  { }

public:
  virtual Obj *next();
};

///////////////////////////////////////////////////////////////////////////

Grant_iterator *
Grant_iterator::create(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "(SELECT user AS c1, "
            "host AS c2, "
            "privilege_type AS c3, "
            "table_schema AS c4, "
            "NULL AS c5, "
            "NULL AS c6 "
    "FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES, mysql.user "
    "WHERE table_schema = '%.*s' AND "
          "grantee = CONCAT(\"'\", user, \"'@'\", host, \"'\")) "
    "UNION "
    "(SELECT user, host, privilege_type, table_schema, table_name, NULL "
    "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES, mysql.user "
    "WHERE table_schema = '%.*s' AND "
          "grantee = CONCAT(\"'\", user, \"'@'\", host, \"'\")) "
    "UNION "
    "(SELECT user, host, privilege_type, table_schema, table_name, column_name "
    "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES, mysql.user "
    "WHERE table_schema = '%.*s' AND "
          "grantee = CONCAT(\"'\", user, \"'@'\", host, \"'\")) "
    "ORDER BY c1 ASC, c2 ASC, c3 ASC, c4 ASC, c5 ASC, c6 ASC",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) db_name->length(),
    (const char *) db_name->ptr());

  return create_row_set_iterator<Grant_iterator>(thd, &query);
}

///////////////////////////////////////////////////////////////////////////

Obj *Grant_iterator::next()
{
  Ed_row *row= m_row_it++;

  if (!row)
    return NULL;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 6);

  const Ed_column *user_name= row->get_column(0);
  const Ed_column *host_name= row->get_column(1);
  const Ed_column *privilege_type= row->get_column(2);
  const Ed_column *db_name= row->get_column(3);
  const Ed_column *tbl_name= row->get_column(4);
  const Ed_column *col_name= row->get_column(5);

  LEX_STRING table_name= { C_STRING_WITH_LEN("") };
  LEX_STRING column_name= { C_STRING_WITH_LEN("") };

  if (tbl_name)
    table_name= *tbl_name;

  if (col_name)
    column_name= *col_name;

  return new Grant_obj(user_name->str, user_name->length,
                       host_name->str, host_name->length,
                       privilege_type->str, privilege_type->length,
                       db_name->str, db_name->length,
                       table_name.str, table_name.length,
                       column_name.str, column_name.length);
}

///////////////////////////////////////////////////////////////////////////

template
Grant_iterator *
create_row_set_iterator<Grant_iterator>(THD *thd, const LEX_STRING *query);

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Database_obj::Database_obj(const char *db_name_str, int db_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @note this method will return an error if the db_name is either
        mysql or information_schema as these are not objects that
        should be recreated using this interface.

  @returns Error status.
   @retval FALSE on success
   @retval TRUE on error
*/

bool Database_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Database_obj::serialize()");
  DBUG_PRINT("Database_obj::serialize",
             ("name: %.*s",
              m_db_name.length(), m_db_name.ptr()));

  if (is_internal_db_name(&m_db_name))
  {
    DBUG_PRINT("backup",
               (" Skipping internal database %.*s",
                m_db_name.length(), m_db_name.ptr()));

    DBUG_RETURN(TRUE);
  }

  /* Run 'SHOW CREATE' query. */

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE DATABASE `%.*s`",
    (int) m_db_name.length(),
    (const char *) m_db_name.ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  /* Generate serialization. */

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *create_stmt= row->get_column(1);

  os <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET character_set_client = utf8" <<
    Fmt("DROP DATABASE IF EXISTS `%.*s`", STR(m_db_name)) <<
    create_stmt <<
    "SET character_set_client = @saved_cs_client";


  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Table_obj::Table_obj(const char *db_name_str, int db_name_length,
                   const char *table_name_str, int table_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
  m_table_name.copy(table_name_str, table_name_length, system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/

bool Table_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Table_obj::serialize()");
  DBUG_PRINT("Table_obj::serialize",
             ("name: %.*s.%.*s",
              m_db_name.length(), m_db_name.ptr(),
              m_table_name.length(), m_table_name.ptr()));

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE TABLE `%.*s`.`%.*s`",
    (int) m_db_name.length(),
    (const char *) m_db_name.ptr(),
    (int) m_table_name.length(),
    (const char *) m_table_name.ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  const Ed_column *create_stmt= row->get_column(1);

  os <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET character_set_client = utf8" <<
    Fmt("USE `%.*s`", STR(m_db_name)) <<
    create_stmt <<
    "SET character_set_client = @saved_cs_client";

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

View_obj::View_obj(const char *db_name_str, int db_name_length,
                 const char *view_name_str, int view_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
  m_view_name.copy(view_name_str, view_name_length, system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

static bool
get_view_create_stmt(THD *thd,
                     View_obj *view,
                     const LEX_STRING **create_stmt,
                     const LEX_STRING **client_cs_name,
                     const LEX_STRING **connection_cl_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  /* Get a create statement for a view. */

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE VIEW `%.*s`.`%.*s`",
    (int) view->get_db_name()->length(),
    (const char *) view->get_db_name()->ptr(),
    (int) view->get_name()->length(),
    (const char *) view->get_name()->ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    return TRUE;
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    return TRUE;

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 4);

  *create_stmt= row->get_column(1);
  *client_cs_name= row->get_column(2);
  *connection_cl_name= row->get_column(3);

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

static bool
dump_base_object_stubs(THD *thd,
                       Obj_iterator *base_object_it,
                       Out_stream &os)
{
  char base_obj_stmt_buf[QUERY_BUFFER_SIZE];
  String base_obj_stmt(base_obj_stmt_buf,
                        sizeof (base_obj_stmt_buf),
                        system_charset_info);

  while (true)
  {
    Obj *base_obj= base_object_it->next();

    if (!base_obj)
      break;

    /* Dump header of base obj stub. */

    os <<
      Fmt("CREATE DATABASE IF NOT EXISTS `%.*s`",
          (int) base_obj->get_db_name()->length(),
          (const char *) base_obj->get_db_name()->ptr());

    base_obj_stmt.length(0);
    base_obj_stmt.append(C_STRING_WITH_LEN("CREATE TABLE IF NOT EXISTS `"));
    base_obj_stmt.append(*base_obj->get_db_name());
    base_obj_stmt.append(C_STRING_WITH_LEN("`.`"));
    base_obj_stmt.append(*base_obj->get_name());
    base_obj_stmt.append(C_STRING_WITH_LEN("`("));

    /* Get base obj structure. */

    char query_buffer[QUERY_BUFFER_SIZE];
    LEX_STRING query;

    query.str= query_buffer;
    query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
      "SHOW COLUMNS FROM `%.*s`.`%.*s`",
      (int) base_obj->get_db_name()->length(),
      (const char *) base_obj->get_db_name()->ptr(),
      (int) base_obj->get_name()->length(),
      (const char *) base_obj->get_name()->ptr());

    Ed_result result(thd->mem_root);

    if (run_query(thd, &query, &result) ||
        result.get_warnings().elements > 0)
    {
      /*
        There should be no warnings. A warning means that serialization has
        failed.
      */
      delete base_obj;
      return TRUE;
    }

    DBUG_ASSERT(result.elements == 1);

    Ed_result_set *rs= result.get_cur_result_set();
    DBUG_ASSERT(rs);

    /* Dump structure of base obj stub. */

    List_iterator_fast<Ed_row> row_it(*rs->data());
    bool first_column= TRUE;

    while (true)
    {
      Ed_row *row= row_it++;

      if (!row)
        break;

      DBUG_ASSERT(row->get_metadata()->get_num_columns() == 6);

      const LEX_STRING *col_name= row->get_column(0);
      const LEX_STRING *col_type= row->get_column(1);

      if (first_column)
        first_column= FALSE;
      else
        base_obj_stmt.append(C_STRING_WITH_LEN(", "));

      base_obj_stmt.append(C_STRING_WITH_LEN("`"));
      base_obj_stmt.append(col_name->str, col_name->length);
      base_obj_stmt.append(C_STRING_WITH_LEN("` "));
      base_obj_stmt.append(col_type->str, col_type->length);
    }

    base_obj_stmt.append(C_STRING_WITH_LEN(") ENGINE = MyISAM"));

    os << base_obj_stmt;

    delete base_obj;
  }

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/
bool View_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("View_obj::serialize()");
  DBUG_PRINT("View_obj::serialize",
             ("name: %.*s.%.*s",
              m_db_name.length(), m_db_name.ptr(),
              m_view_name.length(), m_view_name.ptr()));

  const LEX_STRING *create_stmt;
  const LEX_STRING *client_cs_name;
  const LEX_STRING *connection_cl_name;

  if (get_view_create_stmt(thd, this, &create_stmt,
                           &client_cs_name, &connection_cl_name))
  {
    DBUG_RETURN(TRUE);
  }

  /* Dump the header. */

  os <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET @saved_col_connection = @@collation_connection" <<
    "SET character_set_client = utf8";

  /* Get view dependencies. */

  {
    Obj_iterator *base_table_it=
      get_view_base_tables(thd, &m_db_name, &m_view_name);

    if (!base_table_it ||
        dump_base_object_stubs(thd, base_table_it, os))
    {
      DBUG_RETURN(TRUE);
    }

    delete base_table_it;
  }

  {
    Obj_iterator *base_view_it=
      get_view_base_views(thd, &m_db_name, &m_view_name);

    if (!base_view_it ||
        dump_base_object_stubs(thd, base_view_it, os))
    {
      DBUG_RETURN(TRUE);
    }

    delete base_view_it;
  }

  os <<
    Fmt("USE `%.*s`", STR(m_db_name)) <<
    Fmt("SET character_set_client = %.*s", LXS(client_cs_name)) <<
    Fmt("SET collation_connection = %.*s", LXS(connection_cl_name)) <<
    create_stmt <<
    "SET character_set_client = @saved_cs_client" <<
    "SET collation_connection = @saved_col_connection";

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Trigger_obj::Trigger_obj(const char *db_name_str, int db_name_length,
                       const char *trigger_name_str, int trigger_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
  m_trigger_name.copy(trigger_name_str, trigger_name_length,
                      system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread handler.
  @param[out] os  Output stream.

  @note this method will return an error if the db_name is either
        mysql or information_schema as these are not objects that
        should be recreated using this interface.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/

bool Trigger_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Trigger_obj::do_serialize()");
  DBUG_PRINT("Trigger_obj::do_serialize",
             ("name: %.*s.%.*s",
              m_db_name.length(), m_db_name.ptr(),
              m_trigger_name.length(), m_trigger_name.ptr()));

  DBUG_EXECUTE_IF("backup_fail_add_trigger", DBUG_RETURN(TRUE););

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE TRIGGER `%.*s`.`%.*s`",
    (int) m_db_name.length(),
    (const char *) m_db_name.ptr(),
    (int) m_trigger_name.length(),
    (const char *) m_trigger_name.ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 6);

  const Ed_column *sql_mode= row->get_column(1);
  const Ed_column *create_stmt= row->get_column(2);
  const Ed_column *client_cs= row->get_column(3);
  const Ed_column *connection_cl= row->get_column(4);
  const Ed_column *db_cl= row->get_column(5);

  os <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET @saved_col_connection = @@collation_connection" <<
    "SET @saved_col_database = @@collation_database" <<
    "SET character_set_client = utf8" <<
    Fmt("USE `%.*s`", STR(m_db_name)) <<
    Fmt("SET character_set_client = %.*s", LXS(client_cs)) <<
    Fmt("SET collation_connection = %.*s", LXS(connection_cl)) <<
    Fmt("SET collation_database = %.*s", LXS(db_cl)) <<
    Fmt("SET sql_mode = '%.*s'", LXS(sql_mode)) <<
    create_stmt <<
    "SET character_set_client = @saved_cs_client" <<
    "SET collation_connection = @saved_col_connection" <<
    "SET collation_database = @saved_col_database";

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Stored_proc_obj::Stored_proc_obj(const char *db_name_str, int db_name_length,
                             const char *sp_name_str, int sp_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
  m_sp_name.copy(sp_name_str, sp_name_length, system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @note this method will return an error if the db_name is either
        mysql or information_schema as these are not objects that
        should be recreated using this interface.

  @returns Error status.
*/

bool Stored_proc_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Stored_proc_obj::do_serialize()");
  DBUG_PRINT("Stored_proc_obj::do_serialize",
             ("name: %.*s.%.*s",
              m_db_name.length(), m_db_name.ptr(),
              m_sp_name.length(), m_sp_name.ptr()));

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE PROCEDURE `%.*s`.`%.*s`",
    (int) m_db_name.length(),
    (const char *) m_db_name.ptr(),
    (int) m_sp_name.length(),
    (const char *) m_sp_name.ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 6);

  const Ed_column *sql_mode= row->get_column(1);
  const Ed_column *create_stmt= row->get_column(2);
  const Ed_column *client_cs= row->get_column(3);
  const Ed_column *connection_cl= row->get_column(4);
  const Ed_column *db_cl= row->get_column(5);

  os <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET @saved_col_connection = @@collation_connection" <<
    "SET @saved_col_database = @@collation_database" <<
    "SET character_set_client = utf8" <<
    Fmt("USE `%.*s`", STR(m_db_name)) <<
    Fmt("SET character_set_client = %.*s", LXS(client_cs)) <<
    Fmt("SET collation_connection = %.*s", LXS(connection_cl)) <<
    Fmt("SET collation_database = %.*s", LXS(db_cl)) <<
    Fmt("SET sql_mode = '%.*s'", LXS(sql_mode)) <<
    create_stmt <<
    "SET character_set_client = @saved_cs_client" <<
    "SET collation_connection = @saved_col_connection" <<
    "SET collation_database = @saved_col_database";

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Stored_func_obj::Stored_func_obj(const char *db_name_str, int db_name_length,
                             const char *sf_name_str, int sf_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
  m_sf_name.copy(sf_name_str, sf_name_length, system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @note this method will return an error if the db_name is either
        mysql or information_schema as these are not objects that
        should be recreated using this interface.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/

bool Stored_func_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Stored_func_obj::do_serialize()");
  DBUG_PRINT("Stored_func_obj::do_serialize",
             ("name: %.*s.%.*s",
              m_db_name.length(), m_db_name.ptr(),
              m_sf_name.length(), m_sf_name.ptr()));

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE FUNCTION `%.*s`.`%.*s`",
    (int) m_db_name.length(),
    (const char *) m_db_name.ptr(),
    (int) m_sf_name.length(),
    (const char *) m_sf_name.ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 6);

  const Ed_column *sql_mode= row->get_column(1);
  const Ed_column *create_stmt= row->get_column(2);
  const Ed_column *client_cs= row->get_column(3);
  const Ed_column *connection_cl= row->get_column(4);
  const Ed_column *db_cl= row->get_column(5);

  os <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET @saved_col_connection = @@collation_connection" <<
    "SET @saved_col_database = @@collation_database" <<
    "SET character_set_client = utf8" <<
    Fmt("USE `%.*s`", STR(m_db_name)) <<
    Fmt("SET character_set_client = %.*s", LXS(client_cs)) <<
    Fmt("SET collation_connection = %.*s", LXS(connection_cl)) <<
    Fmt("SET collation_database = %.*s", LXS(db_cl)) <<
    Fmt("SET sql_mode = '%.*s'", LXS(sql_mode)) <<
    create_stmt <<
    "SET character_set_client = @saved_cs_client" <<
    "SET collation_connection = @saved_col_connection" <<
    "SET collation_database = @saved_col_database";

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#ifdef HAVE_EVENT_SCHEDULER

Event_obj::Event_obj(const char *db_name_str, int db_name_length,
                   const char *event_name_str, int event_name_length)
{
  m_db_name.copy(db_name_str, db_name_length, system_charset_info);
  m_event_name.copy(event_name_str, event_name_length, system_charset_info);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @note this method will return an error if the db_name is either
        mysql or information_schema as these are not objects that
        should be recreated using this interface.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/

bool Event_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Event_obj::serialize()");
  DBUG_PRINT("Event_obj::serialize",
             ("name: %.*s.%.*s",
              m_db_name.length(), m_db_name.ptr(),
              m_event_name.length(), m_event_name.ptr()));

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE EVENT `%.*s`.`%.*s`",
    (int) m_db_name.length(),
    (const char *) m_db_name.ptr(),
    (int) m_event_name.length(),
    (const char *) m_event_name.ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  DBUG_ASSERT(result.elements == 1);

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs);

  if (rs->data()->elements == 0)
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(rs->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*rs->data());
  Ed_row *row= row_it++;

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 7);

  const Ed_column *sql_mode= row->get_column(1);
  const Ed_column *tz= row->get_column(2);
  const Ed_column *create_stmt= row->get_column(3);
  const Ed_column *client_cs= row->get_column(4);
  const Ed_column *connection_cl= row->get_column(5);
  const Ed_column *db_cl= row->get_column(6);

  os <<
    "SET @saved_time_zone = @@time_zone" <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET @saved_col_connection = @@collation_connection" <<
    "SET @saved_col_database = @@collation_database" <<
    "SET character_set_client = utf8" <<
    Fmt("USE `%.*s`", STR(m_db_name)) <<
    Fmt("SET time_zone = '%.*s'", LXS(tz)) <<
    Fmt("SET character_set_client = %.*s", LXS(client_cs)) <<
    Fmt("SET collation_connection = %.*s", LXS(connection_cl)) <<
    Fmt("SET collation_database = %.*s", LXS(db_cl)) <<
    Fmt("SET sql_mode = '%.*s'", LXS(sql_mode)) <<
    create_stmt <<
    "SET time_zone = @saved_time_zone" <<
    "SET character_set_client = @saved_cs_client" <<
    "SET collation_connection = @saved_col_connection" <<
    "SET collation_database = @saved_col_database";

  DBUG_RETURN(FALSE);
}

#endif // HAVE_EVENT_SCHEDULER

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

Tablespace_obj::Tablespace_obj(
  const char *ts_name_str, int ts_name_length,
  const char *comment_str, int comment_length,
  const char *data_file_name_str, int data_file_name_length,
  const char *engine_str, int engine_length)
{
  m_ts_name.copy(ts_name_str, ts_name_length, system_charset_info);
  m_comment.copy(comment_str, comment_length, system_charset_info);
  m_data_file_name.copy(data_file_name_str, data_file_name_length,
                        system_charset_info);
  m_engine.copy(engine_str, engine_length, system_charset_info);

  m_description.length(0);
}

Tablespace_obj::Tablespace_obj(const char *ts_name_str, int ts_name_length)
{
  m_ts_name.copy(ts_name_str, ts_name_length, system_charset_info);
  m_comment.length(0);
  m_data_file_name.length(0);
  m_engine.length(0);

  m_description.length(0);
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/

bool Tablespace_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Tablespace_obj::serialize()");

  os << *get_description();

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////

bool Tablespace_obj::materialize(uint serialization_version,
                                 const String *serialization)
{
  if (Abstract_obj::materialize(serialization_version, serialization))
    return TRUE;

  List_iterator_fast<String> it(m_stmt_lst);
  String *desc= it++;

  DBUG_ASSERT(desc);

  m_description.set(desc->ptr(), desc->length(), desc->charset());

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

/**
  Get a description of the tablespace object.

  This method returns the description of the object which is currently
  the serialization string.

  @returns Serialization string.
*/

const String *Tablespace_obj::get_description()
{
  DBUG_ENTER("Tablespace_obj::get_description()");

  DBUG_ASSERT(m_description.length() ||
              m_ts_name.length() && m_data_file_name.length());

  if (m_description.length())
    DBUG_RETURN(&m_description);

  /* Construct the CREATE TABLESPACE command from the variables. */

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "CREATE TABLESPACE `%.*s` ADD DATAFILE '%.*s' ",
    (int) m_ts_name.length(),
    (const char *) m_ts_name.ptr(),
    (int) m_data_file_name.length(),
    (const char *) m_data_file_name.ptr());

  m_description.length(0);
  m_description.append(query.str, query.length);

  if (m_comment.length())
  {
    m_description.append("COMMENT = '");
    m_description.append(m_comment);
    m_description.append("' ");
  }

  m_description.append("ENGINE = ");
  m_description.append(m_engine);

  DBUG_RETURN(&m_description);
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

void Grant_obj::generate_unique_id(const String *user_name,
                                   const String *host_name,
                                   String *id)
{
  static unsigned long id_counter= 0;

  id->length(0);

  if (user_name->length() && host_name->length())
  {
    id->append(*user_name);
    id->append('@');
    id->append(*host_name);
  }
  else
    id->append("<no_name>");

  char buf[10];
  snprintf(buf, 10, " %08lu", ++id_counter);

  id->append(buf, 10);
}

Grant_obj::Grant_obj(const char *user_name_str, int user_name_length,
                     const char *host_name_str, int host_name_length,
                     const char *priv_type_str, int priv_type_length,
                     const char *db_name_str, int db_name_length,
                     const char *table_name_str, int table_name_length,
                     const char *column_name_str, int column_name_length)
{
  m_user_name.copy(user_name_str, user_name_length, system_charset_info);
  m_host_name.copy(host_name_str, host_name_length, system_charset_info);

  /* Grant info. */

  m_grant_info.append(priv_type_str, priv_type_length);

  if (column_name_length)
  {
    m_grant_info.append('(');
    m_grant_info.append(column_name_str, column_name_length);
    m_grant_info.append(')');
  }

  m_grant_info.append(" ON ");

  m_grant_info.append(db_name_str, db_name_length);
  m_grant_info.append('.');

  if (table_name_length)
    m_grant_info.append(table_name_str, table_name_length);
  else
    m_grant_info.append('*');

  /* Id. */

  generate_unique_id(&m_user_name, &m_host_name, &m_id);
}

Grant_obj::Grant_obj(const char *name_str, int name_length)
{
  m_user_name.length(0);
  m_host_name.length(0);
  m_grant_info.length(0);

  m_id.copy(name_str, name_length, system_charset_info);
}

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] os  Output stream.

  @note this method will return an error if the db_name is either
        mysql or information_schema as these are not objects that
        should be recreated using this interface.

  @returns Error status.
    @retval FALSE on success
    @retval TRUE on error
*/

bool Grant_obj::do_serialize(THD *thd, Out_stream &os)
{
  DBUG_ENTER("Grant_obj::do_serialize()");

  os <<
    m_user_name <<
    m_host_name <<
    m_grant_info <<
    "SET @saved_cs_client = @@character_set_client" <<
    "SET character_set_client= binary" <<
    Fmt("GRANT %.*s TO '%.*s'@'%.*s'",
        STR(m_grant_info), STR(m_user_name), STR(m_host_name)) <<
    "SET character_set_client= @saved_cs_client";

  DBUG_RETURN(FALSE);
}

bool Grant_obj::do_materialize(In_stream *is)
{
  LEX_STRING user_name;
  LEX_STRING host_name;
  LEX_STRING grant_info;

  if (is->next(&user_name))
    return TRUE; /* Can not decode user name. */

  if (is->next(&host_name))
    return TRUE; /* Can not decode host name. */

  if (is->next(&grant_info))
    return TRUE; /* Can not decode grant info. */

  m_user_name.copy(user_name.str, user_name.length, system_charset_info);
  m_host_name.copy(host_name.str, host_name.length, system_charset_info);
  m_grant_info.copy(grant_info.str, grant_info.length, system_charset_info);

  return Abstract_obj::do_materialize(is);
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

Obj *get_database(const String *db_name)
{
  return new Database_obj(db_name->ptr(), db_name->length());
}

Obj *get_table(const String *db_name,
               const String *table_name)
{
  return new Table_obj(db_name->ptr(), db_name->length(),
                      table_name->ptr(), table_name->length());
}

Obj *get_view(const String *db_name,
              const String *view_name)
{
  return new View_obj(db_name->ptr(), db_name->length(),
                     view_name->ptr(), view_name->length());
}

Obj *get_trigger(const String *db_name,
                 const String *trigger_name)
{
  return new Trigger_obj(db_name->ptr(), db_name->length(),
                        trigger_name->ptr(), trigger_name->length());
}

Obj *get_stored_procedure(const String *db_name,
                          const String *sp_name)
{
  return new Stored_proc_obj(db_name->ptr(), db_name->length(),
                           sp_name->ptr(), sp_name->length());
}

Obj *get_stored_function(const String *db_name,
                         const String *sf_name)
{
  return new Stored_func_obj(db_name->ptr(), db_name->length(),
                           sf_name->ptr(), sf_name->length());
}

Obj *get_event(const String *db_name,
               const String *event_name)
{
#ifdef HAVE_EVENT_SCHEDULER
  return new Event_obj(db_name->ptr(), db_name->length(),
                      event_name->ptr(), event_name->length());
#else
  return NULL;
#endif
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Obj_iterator *get_databases(THD *thd)
{
  return Database_iterator::create(thd);
}

Obj_iterator *get_db_tables(THD *thd, const String *db_name)
{
  return Db_tables_iterator::create(thd, db_name);
}

Obj_iterator *get_db_views(THD *thd, const String *db_name)
{
  return Db_views_iterator::create(thd, db_name);
}

Obj_iterator *get_db_triggers(THD *thd, const String *db_name)
{
  return Db_trigger_iterator::create(thd, db_name);
}

Obj_iterator *get_db_stored_procedures(THD *thd, const String *db_name)
{
  return Db_stored_proc_iterator::create(thd, db_name);
}

Obj_iterator *get_db_stored_functions(THD *thd, const String *db_name)
{
  return Db_stored_func_iterator::create(thd, db_name);
}

Obj_iterator *get_db_events(THD *thd, const String *db_name)
{
  return Db_event_iterator::create(thd, db_name);
}

Obj_iterator *get_all_db_grants(THD *thd, const String *db_name)
{
  return Grant_iterator::create(thd, db_name);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Obj_iterator* get_view_base_tables(THD *thd,
                                   const String *db_name,
                                   const String *view_name)
{
  return View_base_table_iterator::create(thd, db_name, view_name);
}

Obj_iterator* get_view_base_views(THD *thd,
                                  const String *db_name,
                                  const String *view_name)
{
  return View_base_view_iterator::create(thd, db_name, view_name);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

Obj *materialize_database(const String *db_name,
                          uint serialization_version,
                          const String *serialization)
{
  Obj *obj= new Database_obj(db_name->ptr(), db_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

Obj *materialize_table(const String *db_name,
                       const String *table_name,
                       uint serialization_version,
                       const String *serialization)
{
  Obj *obj= new Table_obj(db_name->ptr(), db_name->length(),
                          table_name->ptr(), table_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

Obj *materialize_view(const String *db_name,
                      const String *view_name,
                      uint serialization_version,
                      const String *serialization)
{
  Obj *obj= new View_obj(db_name->ptr(), db_name->length(),
                         view_name->ptr(), view_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

Obj *materialize_trigger(const String *db_name,
                         const String *trigger_name,
                         uint serialization_version,
                         const String *serialization)
{
  Obj *obj= new Trigger_obj(db_name->ptr(), db_name->length(),
                            trigger_name->ptr(), trigger_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

Obj *materialize_stored_procedure(const String *db_name,
                                  const String *sp_name,
                                  uint serialization_version,
                                  const String *serialization)
{
  Obj *obj= new Stored_proc_obj(db_name->ptr(), db_name->length(),
                                sp_name->ptr(), sp_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

Obj *materialize_stored_function(const String *db_name,
                                 const String *sf_name,
                                 uint serialization_version,
                                 const String *serialization)
{
  Obj *obj= new Stored_func_obj(db_name->ptr(), db_name->length(),
                                sf_name->ptr(), sf_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

#ifdef HAVE_EVENT_SCHEDULER

Obj *materialize_event(const String *db_name,
                       const String *event_name,
                       uint serialization_version,
                       const String *serialization)
{
  Obj *obj= new Event_obj(db_name->ptr(), db_name->length(),
                          event_name->ptr(), event_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

#endif

Obj *materialize_tablespace(const String *ts_name,
                            uint serialization_version,
                            const String *serialization)
{
  Obj *obj= new Tablespace_obj(ts_name->ptr(), ts_name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

Obj *materialize_db_grant(const String *db_name,
                          const String *name,
                          uint serialization_version,
                          const String *serialization)
{
  Obj *obj= new Grant_obj(name->ptr(), name->length());
  obj->materialize(serialization_version, serialization);

  return obj;
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

bool is_internal_db_name(const String *db_name)
{
  return
    my_strcasecmp(lower_case_table_names ? system_charset_info :
                  &my_charset_bin, ((String *) db_name)->c_ptr_safe(),
                  MYSQL_SCHEMA_NAME.str) == 0 ||
    my_strcasecmp(system_charset_info,
                  ((String *) db_name)->c_ptr_safe(),
                  "information_schema") == 0 ||
    my_strcasecmp(system_charset_info,
                  ((String *) db_name)->c_ptr_safe(),
                  "performance_schema") == 0;
}

///////////////////////////////////////////////////////////////////////////

bool check_db_existence(THD *thd, const String *db_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SHOW CREATE DATABASE `%.*s`",
    (int) db_name->length(),
    (const char *) db_name->ptr());

  Ed_result result(thd->mem_root);
  int rc= run_query(thd, &query, &result);

  /* We're not interested in warnings/errors here. */

  return rc != 0;
}

///////////////////////////////////////////////////////////////////////////

bool check_user_existence(THD *thd, const Obj *obj)
{
#ifdef EMBEDDED_LIBRARY
  return TRUE;
#else
  Grant_obj *grant_obj= (Grant_obj *) obj;

  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT 1 "
    "FROM mysql.user "
    "WHERE user = '%.*s' AND host = '%.*s'",
    (int) grant_obj->get_user_name()->length(),
    (const char *) grant_obj->get_user_name()->ptr(),
    (int) grant_obj->get_host_name()->length(),
    (const char *) grant_obj->get_host_name()->ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return FALSE;
  }

  Ed_result_set *rs= result.get_cur_result_set();

  if (!rs)
    return FALSE;

  return rs->data()->elements > 0;
#endif
}

///////////////////////////////////////////////////////////////////////////

const String *grant_get_user_name(const Obj *obj)
{
  return ((Grant_obj *) obj)->get_user_name();
}

///////////////////////////////////////////////////////////////////////////

const String *grant_get_host_name(const Obj *obj)
{
  return ((Grant_obj *) obj)->get_host_name();
}

///////////////////////////////////////////////////////////////////////////

const String *grant_get_grant_info(const Obj *obj)
{
  return ((Grant_obj *) obj)->get_grant_info();
}

///////////////////////////////////////////////////////////////////////////

Obj *find_tablespace(THD *thd, const String *ts_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT t1.tablespace_comment, t2.file_name, t1.engine "
    "FROM INFORMATION_SCHEMA.TABLESPACES AS t1, "
         "INFORMATION_SCHEMA.FILES AS t2 "
    "WHERE t1.tablespace_name = t2.tablespace_name AND "
         "t1.tablespace_name = '%.*s'",
    (int) ts_name->length(),
    (const char *) ts_name->ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return NULL;
  }

  if (!result.elements)
    return NULL;

  Ed_result_set *rs= result.get_cur_result_set();

  DBUG_ASSERT(rs->data()->elements == 1);

  Ed_row *row= rs->get_cur_row();

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 3);

  const Ed_column *comment= row->get_column(0);
  const Ed_column *data_file_name= row->get_column(1);
  const Ed_column *engine= row->get_column(2);

  return new Tablespace_obj(ts_name->ptr(), ts_name->length(),
                            comment->str, comment->length,
                            data_file_name->str, data_file_name->length,
                            engine->str, engine->length);
}

///////////////////////////////////////////////////////////////////////////

/**
  Retrieve the tablespace for a table if it exists

  This method returns a @c Tablespace_obj object if the table has a tablespace.

  @param[in]  thd         Thread context.
  @param[in]  db_name     The database name for the table.
  @param[in]  table_name  The table name.

  @note Caller is responsible for destroying the object.

  @retval Tablespace object if table uses a tablespace
  @retval NULL if table does not use a tablespace
*/

Obj *find_tablespace_for_table(THD *thd,
                               const String *db_name,
                               const String *table_name)
{
  char query_buffer[QUERY_BUFFER_SIZE];
  LEX_STRING query;

  query.str= query_buffer;
  query.length= my_snprintf(query_buffer, QUERY_BUFFER_SIZE,
    "SELECT t1.tablespace_name, t1.engine, t1.tablespace_comment, t2.file_name "
    "FROM INFORMATION_SCHEMA.TABLESPACES AS t1, "
         "INFORMATION_SCHEMA.FILES AS t2, "
         "INFORMATION_SCHEMA.TABLES AS t3 "
    "WHERE t1.tablespace_name = t2.tablespace_name AND "
         "t2.tablespace_name = t3.tablespace_name AND "
         "t3.table_schema = '%.*s' AND "
         "t3.table_name = '%.*s'",
    (int) db_name->length(),
    (const char *) db_name->ptr(),
    (int) table_name->length(),
    (const char *) table_name->ptr());

  Ed_result result(thd->mem_root);

  if (run_query(thd, &query, &result) ||
      result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return NULL;
  }

  if (!result.elements)
    return NULL;

  Ed_result_set *rs= result.get_cur_result_set();

  if (!rs->data()->elements)
    return NULL;

  DBUG_ASSERT(rs->data()->elements == 1);

  Ed_row *row= rs->get_cur_row();

  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 4);

  const Ed_column *ts_name= row->get_column(0);
  const Ed_column *engine= row->get_column(1);
  const Ed_column *comment= row->get_column(2);
  const Ed_column *data_file_name= row->get_column(3);

  return new Tablespace_obj(ts_name->str, ts_name->length,
                            comment->str, comment->length,
                            data_file_name->str, data_file_name->length,
                            engine->str, engine->length);
}

///////////////////////////////////////////////////////////////////////////

bool compare_tablespace_attributes(Obj *ts1, Obj *ts2)
{
  DBUG_ENTER("obs::compare_tablespace_attributes()");

  Tablespace_obj *o1= (Tablespace_obj *) ts1;
  Tablespace_obj *o2= (Tablespace_obj *) ts2;

  DBUG_RETURN(my_strcasecmp(system_charset_info,
                            o1->get_description()->ptr(),
                            o2->get_description()->ptr()) == 0);
}

///////////////////////////////////////////////////////////////////////////

//
// Implementation: DDL Blocker.
//

///////////////////////////////////////////////////////////////////////////

/*
  DDL Blocker methods
*/

/**
   Turn on the ddl blocker

   This method is used to start the ddl blocker blocking DDL commands.

   @param[in] thd  current thread

   @retval FALSE on success.
   @retval TRUE on error.
  */
bool ddl_blocker_enable(THD *thd)
{
  DBUG_ENTER("ddl_blocker_enable()");
  if (!DDL_blocker->block_DDL(thd))
    DBUG_RETURN(TRUE);
  DBUG_RETURN(FALSE);
}

/**
   Turn off the ddl blocker

   This method is used to stop the ddl blocker from blocking DDL commands.
  */
void ddl_blocker_disable()
{
  DBUG_ENTER("ddl_blocker_disable()");
  DDL_blocker->unblock_DDL();
  DBUG_VOID_RETURN;
}

/**
   Turn on the ddl blocker exception

   This method is used to allow the exception allowing a restore operation to
   perform DDL operations while the ddl blocker blocking DDL commands.

   @param[in] thd  current thread
  */
void ddl_blocker_exception_on(THD *thd)
{
  DBUG_ENTER("ddl_blocker_exception_on()");
  thd->DDL_exception= TRUE;
  DBUG_VOID_RETURN;
}

/**
   Turn off the ddl blocker exception

   This method is used to suspend the exception allowing a restore operation to
   perform DDL operations while the ddl blocker blocking DDL commands.

   @param[in] thd  current thread
  */
void ddl_blocker_exception_off(THD *thd)
{
  DBUG_ENTER("ddl_blocker_exception_off()");
  thd->DDL_exception= FALSE;
  DBUG_VOID_RETURN;
}

/**
  Build a table list from a list of tables as class Obj.

  This method creates a TABLE_LIST from a List<> of type Obj.

  param[IN]  tables    The list of tables
  param[IN]  lock      The desired lock type

  @returns TABLE_LIST *

  @note Caller must free memory.
*/
TABLE_LIST *Name_locker::build_table_list(List<Obj> *tables,
                                          thr_lock_type lock)
{
  TABLE_LIST *tl= NULL;
  Obj *tbl= NULL;
  DBUG_ENTER("Name_locker::build_table_list()");

  List_iterator<Obj> it(*tables);
  while ((tbl= it++))
  {
    TABLE_LIST *ptr= (TABLE_LIST*)my_malloc(sizeof(TABLE_LIST), MYF(MY_WME));
    DBUG_ASSERT(ptr);  // FIXME: report error instead
    bzero(ptr, sizeof(TABLE_LIST));

    ptr->alias= ptr->table_name= const_cast<char*>(tbl->get_name()->ptr());
    ptr->db= const_cast<char*>(tbl->get_db_name()->ptr());
    ptr->lock_type= lock;

    // and add it to the list

    ptr->next_global= ptr->next_local=
      ptr->next_name_resolution_table= tl;
    tl= ptr;
    tl->table= ptr->table;
  }

  DBUG_RETURN(tl);
}

void Name_locker::free_table_list(TABLE_LIST *tl)
{
  TABLE_LIST *ptr= tl;

  while (ptr)
  {
    tl= tl->next_global;
    my_free(ptr, MYF(0));
    ptr= tl;
  }
}

/**
  Gets name locks on table list.

  This method attempts to take an exclusive name lock on each table in the
  list. It does nothing if the table list is empty.

  @param[IN] tables  The list of tables to lock.
  @param[IN] lock    The type of lock to take.

  @returns 0 if success, 1 if error
*/
int Name_locker::get_name_locks(List<Obj> *tables, thr_lock_type lock)
{
  TABLE_LIST *ltable= 0;
  int ret= 0;
  DBUG_ENTER("Name_locker::get_name_locks()");
  /*
    Convert List<Obj> to TABLE_LIST *
  */
  m_table_list= build_table_list(tables, lock);
  if (m_table_list)
  {
    if (lock_table_names(m_thd, m_table_list))
      ret= 1;
    pthread_mutex_lock(&LOCK_open);
    for (ltable= m_table_list; ltable; ltable= ltable->next_local)
      tdc_remove_table(m_thd, TDC_RT_REMOVE_ALL, ltable->db,
                       ltable->table_name);
  }
  DBUG_RETURN(ret);
}

/*
  Releases name locks on table list.

  This method releases the name locks on the table list. It does nothing if
  the table list is empty.

  @returns 0 if success, 1 if error
*/
int Name_locker::release_name_locks()
{
  DBUG_ENTER("Name_locker::release_name_locks()");
  if (m_table_list)
  {
    pthread_mutex_unlock(&LOCK_open);
    unlock_table_names(m_thd);
  }
  DBUG_RETURN(0);
}

///////////////////////////////////////////////////////////////////////////

} // obs namespace
