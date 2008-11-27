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
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA */

/** @file Server Service Interface for Backup: the implementation.  */

#include "mysql_priv.h"

#include "si_objects.h"
#include "ddl_blocker.h"
#include "sql_show.h"
#include "sql_trigger.h"
#include "sp.h"
#include "sp_head.h" // for sp_add_to_query_tables().

#ifdef HAVE_EVENT_SCHEDULER
#include "events.h"
#include "event_data_objects.h"
#include "event_db_repository.h"
#endif

DDL_blocker_class *DDL_blocker= NULL;

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

#define STR(x) (int) (x).length(), (x).ptr()

#define LXS_INIT(x) {((char *) (x)), ((size_t) (sizeof (x) - 1))}

///////////////////////////////////////////////////////////////////////////

namespace {

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

bool
run_service_interface_sql(THD *thd, const LEX_STRING *query,
                          Ed_result *ed_result)
{
  ulong sql_mode_saved= thd->variables.sql_mode;
  CHARSET_INFO *client_cs_saved= thd->variables.character_set_client;
  CHARSET_INFO *results_cs_saved= thd->variables.character_set_results;
  CHARSET_INFO *connection_cl_saved= thd->variables.collation_connection;
  TABLE *tmp_tables_saved= thd->temporary_tables;

  DBUG_ENTER("run_service_interface_sql");
  DBUG_PRINT("run_service_interface_sql",
             ("query: %.*s",
              (int) query->length, (const char *) query->str));

  thd->variables.sql_mode= 0;

  /*
    Temporary tables should be ignored while looking for table structures.
    Backup wants to backup ordinary tables, not temporary ones.
  */
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

  bool rc= mysql_execute_direct(thd, *query, ed_result);

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
  LEX_STRING db_name;
  LEX_STRING table_name;
  LEX_STRING key;
  void init_from_mdl_key(const char *key_arg, size_t key_length_arg,
                         char *key_buff_arg);
};


void
Table_name_key::init_from_mdl_key(const char *key_arg, size_t key_length_arg,
                                  char *key_buff_arg)
{
  key.str= key_buff_arg;
  key.length= key_length_arg - 4; /* Skip MDL object type */
  memcpy(key.str, key_arg + 4, key.length);

  db_name.str= key.str;
  db_name.length= strlen(db_name.str);
  table_name.str= db_name.str + db_name.length + 1; /* Skip \0 */
  table_name.length= strlen(table_name.str);
}

///////////////////////////////////////////////////////////////////////////
extern "C" {

uchar *
get_table_name_key(const uchar *record,
                   size_t *key_length,
                   my_bool not_used __attribute__((unused)))
{
  Table_name_key *table_name_key= (Table_name_key *) record;
  *key_length= table_name_key->key.length;
  return (uchar *) table_name_key->key.str;
}

///////////////////////////////////////////////////////////////////////////

static void
free_table_name_key(void *data)
{
  Table_name_key *table_name_key= (Table_name_key *) data;
  my_free(table_name_key, MYF(0));
}

} // end of extern "C"

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

struct Int_value
{
  unsigned long m_value;

  Int_value(unsigned long value_arg)
    :m_value(value_arg)
  { }
};


struct C_str
{
  LEX_STRING lex_string;

  C_str(const char *str, size_t length)
  {
    lex_string.str= (char *) str;
    lex_string.length= length;
  }
};


class String_stream
{
public:
  String_stream()
    : m_buffer(&m_container)
  { }

  String_stream(String *dst)
    : m_buffer(dst)
  { }

public:
  String *str() { return m_buffer; }

  const LEX_STRING *lex_string()
  {
    m_lex_string= m_buffer->lex_string();
    return &m_lex_string;
  }

  void reset() { m_buffer->length(0); }

public:
  String_stream &operator <<(const Int_value &v);
  String_stream &operator <<(const C_str &v);
  String_stream &operator <<(const LEX_STRING *query);
  String_stream &operator <<(const String *query);
  String_stream &operator <<(const char *str);

private:
  String m_container;
  String *m_buffer;
  LEX_STRING m_lex_string;
};

///////////////////////////////////////////////////////////////////////////

String_stream &String_stream::operator <<(const Int_value &int_value)
{
  char buffer[13];
  my_snprintf(buffer, sizeof (buffer), "%lu", int_value.m_value);

  m_buffer->append(buffer);
  return *this;
}


String_stream &String_stream::operator <<(const C_str &v)
{
  m_buffer->append(v.lex_string.str, v.lex_string.length);
  return *this;
}


String_stream &String_stream::operator <<(const LEX_STRING *str)
{
  m_buffer->append(str->str, str->length);
  return *this;
}


String_stream &String_stream::operator <<(const String *str)
{
  m_buffer->append(str->ptr(), str->length());
  return *this;
}


String_stream &String_stream::operator <<(const char *str)
{
  m_buffer->append(str);
  return *this;
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Out_stream
{
public:
  Out_stream(String *image) :
    m_image(image)
  { }

public:
  Out_stream &operator <<(const char *query);
  Out_stream &operator <<(const LEX_STRING *query);
  Out_stream &operator <<(const String *query);
  Out_stream &operator <<(String_stream &s_stream);

private:
  String *m_image;
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
  String_stream s_stream(m_image);

  s_stream <<
    Int_value(query->length) << " " << query << "\n";

  return *this;
}

///////////////////////////////////////////////////////////////////////////

Out_stream &Out_stream::operator <<(const String *query)
{
  LEX_STRING str= { (char *) query->ptr(), query->length() };
  return Out_stream::operator <<(&str);
}

///////////////////////////////////////////////////////////////////////////

Out_stream &Out_stream::operator <<(String_stream &s_stream)
{
  return Out_stream::operator <<(s_stream.str());
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class In_stream
{
public:
  In_stream(uint image_version,
            const String *image)
  : m_image_version(image_version),
    m_image(image),
    m_read_ptr(m_image->ptr()),
    m_end_ptr(m_image->ptr() + m_image->length())
  { }

public:
  uint image_version() const { return m_image_version; }
public:
  bool next(LEX_STRING *chunk);

private:
  uint m_image_version;
  const String *m_image;
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

} // end of anonymous namespace

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

namespace obs {

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Abstract_obj : public Obj
{
public:
  virtual inline const String *get_name() const    { return &m_id; }
  virtual inline const String *get_db_name() const { return NULL; }

public:
  /**
    Serialize an object to an image. The serialization image is opaque
    object for the client.

    @param[in]  thd     Thread context.
    @param[out] image   Serialization image.

    @return Error status.
  */
  virtual bool serialize(THD *thd, String *image);

  /**
    Create an object persistently in the database.

    @param[in]  thd     Thread context.

    @return Error status.
  */
  virtual bool create(THD *thd);

  /**
    Drop an object in the database.

    @param[in]  thd     Thread context.

    @return Error status.
  */
  virtual bool drop(THD *thd);

public:
  /**
    Read the object state from a given serialization image and restores
    object state to the point, where it can be created persistently in the
    database.

    @param[in] image_version The version of the serialization format.
    @param[in] image         Buffer contained serialized object.

    @return Error status.
      @retval FALSE on success.
      @retval TRUE on error.
  */
  virtual bool init_from_image(uint image_version, const String *image);

protected:
  /**
    A primitive implementing @c serialize() method.
  */
  virtual bool do_serialize(THD *thd, Out_stream &out_stream) = 0;

  /**
    A primitive implementing @c init_from_image() method.
  */
  virtual bool do_init_from_image(In_stream *is);

  virtual void build_drop_statement(String_stream &s_stream) const = 0;

protected:
  MEM_ROOT m_mem_root; /* This mem-root is for keeping stmt list. */
  List<String> m_stmt_list;

protected:
  /* These attributes are to be used only for serialization. */
  String m_id; //< identify object

protected:
  Abstract_obj(LEX_STRING id);

  virtual ~Abstract_obj();

private:
  Abstract_obj(const Abstract_obj &);
  Abstract_obj &operator =(const Abstract_obj &);
};

///////////////////////////////////////////////////////////////////////////

Abstract_obj::Abstract_obj(LEX_STRING id)
{
  init_sql_alloc(&m_mem_root, ALLOC_ROOT_MIN_BLOCK_SIZE, 0);

  if (id.str && id.length)
    m_id.copy(id.str, id.length, system_charset_info);
  else
    m_id.length(0);
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
  @param[in] image Buffer to serialize the object

  @return Error status.
    @retval FALSE on success.
    @retval TRUE on error.

  @note The real work is done inside @c do_serialize() primitive which should be
  defied in derived classes. This method prepares appropriate context and calls
  the primitive.
*/

bool Abstract_obj::serialize(THD *thd, String *image)
{
  ulong sql_mode_saved= thd->variables.sql_mode;
  thd->variables.sql_mode= 0;

  Out_stream out_stream(image);

  bool ret= do_serialize(thd, out_stream);

  thd->variables.sql_mode= sql_mode_saved;

  return ret;
}

///////////////////////////////////////////////////////////////////////////

/**
  Create the object in the database.

  @param[in] thd              Server thread context.

  @return Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Abstract_obj::create(THD *thd)
{
  bool rc= FALSE;
  List_iterator_fast<String> it(m_stmt_list);
  String *sql_text;

  /*
    Drop the object if it exists first of all.

    @note this semantics will be changed in the future.
    That's why drop() is a public separate operation of Obj.
  */
  drop(thd);

  /*
    Preserve the following session attributes:
    - sql_mode;
    - character_set_client;
    - character_set_results;
    - collation_connection;
    - time_zone;

    @note other session variables are not preserved, so serialization image
    must take care to clean up the environment after itself.
  */
  ulong sql_mode_saved= thd->variables.sql_mode;
  Time_zone *tz_saved= thd->variables.time_zone;
  CHARSET_INFO *client_cs_saved= thd->variables.character_set_client;
  CHARSET_INFO *results_cs_saved= thd->variables.character_set_results;
  CHARSET_INFO *connection_cl_saved= thd->variables.collation_connection;
  TABLE *tmp_tables_saved= thd->temporary_tables;

  /*
    Reset session state to the following:
    - sql_mode: 0
    - character_set_client: utf8
    - character_set_results: binary
    - collation_connection: utf8
  */
  thd->variables.sql_mode= 0;
  thd->variables.character_set_client= system_charset_info;
  thd->variables.character_set_results= &my_charset_bin;
  thd->variables.collation_connection= system_charset_info;
  thd->update_charset();

  /*
    Temporary tables should be ignored while looking for table structures.
    Backup wants to deal with ordinary tables, not temporary ones.
  */
  thd->temporary_tables= NULL;

  /* Allow to execute DDL operations. */
  ddl_blocker_exception_on(thd);

  /* Run queries from the serialization image. */
  while ((sql_text= it++))
  {
    Ed_result ed_result;

    rc= mysql_execute_direct(thd, sql_text->lex_string(), &ed_result);

    /* Ignore warnings from materialization for now. */
    if (rc)
      break;
  }

  ddl_blocker_exception_off(thd);

  thd->variables.sql_mode= sql_mode_saved;
  thd->variables.time_zone= tz_saved;
  thd->variables.collation_connection= connection_cl_saved;
  thd->variables.character_set_results= results_cs_saved;
  thd->variables.character_set_client= client_cs_saved;
  thd->update_charset();

  thd->temporary_tables= tmp_tables_saved;

  return rc;
}

///////////////////////////////////////////////////////////////////////////

/**
  Create the object in the database.

  @param[in] thd              Server thread context.

  @return Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Abstract_obj::drop(THD *thd)
{
  bool rc= FALSE;

  /*
    Preserve the following session attributes:
    - sql_mode;
    - character_set_client;
    - character_set_results;
    - collation_connection;

    Remember also session temporary tables.
  */
  ulong sql_mode_saved= thd->variables.sql_mode;
  CHARSET_INFO *client_cs_saved= thd->variables.character_set_client;
  CHARSET_INFO *results_cs_saved= thd->variables.character_set_results;
  CHARSET_INFO *connection_cl_saved= thd->variables.collation_connection;
  TABLE *tmp_tables_saved= thd->temporary_tables;

  /*
    Reset session state to the following:
    - sql_mode: 0
    - character_set_client: utf8
    - character_set_results: binary
    - collation_connection: utf8
  */
  thd->variables.sql_mode= 0;
  thd->variables.character_set_client= system_charset_info;
  thd->variables.character_set_results= &my_charset_bin;
  thd->variables.collation_connection= system_charset_info;
  thd->update_charset();

  /*
    Temporary tables should be ignored while looking for table structures.
    Backup wants to deal with ordinary tables, not temporary ones.
  */
  thd->temporary_tables= NULL;

  /* Allow to execute DDL operations. */
  ddl_blocker_exception_on(thd);

  /* Run queries from the serialization image. */
  {
    Ed_result ed_result;
    String_stream s_stream;
    const LEX_STRING *sql_text;

    build_drop_statement(s_stream);
    sql_text= s_stream.lex_string();

    if (sql_text->str && sql_text->length)
      rc= mysql_execute_direct(thd, *sql_text, &ed_result);

    /* Ignore warnings. */
  }

  ddl_blocker_exception_off(thd);

  thd->variables.sql_mode= sql_mode_saved;
  thd->variables.collation_connection= connection_cl_saved;
  thd->variables.character_set_results= results_cs_saved;
  thd->variables.character_set_client= client_cs_saved;
  thd->update_charset();

  thd->temporary_tables= tmp_tables_saved;

  return rc;
}

///////////////////////////////////////////////////////////////////////////

bool Abstract_obj::init_from_image(uint image_version, const String *image)
{
  m_stmt_list.delete_elements();

  In_stream is(image_version, image);

  return do_init_from_image(&is);
}

///////////////////////////////////////////////////////////////////////////

bool Abstract_obj::do_init_from_image(In_stream *is)
{
  LEX_STRING sql_text;
  while (! is->next(&sql_text))
  {
    String *str_sql_text= new (&m_mem_root) String();

    if (!str_sql_text ||
        str_sql_text->copy(sql_text.str, sql_text.length,
                           system_charset_info) ||
        m_stmt_list.push_back(str_sql_text))
    {
      return TRUE;
    }
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
  Database_obj(const Ed_row &ed_row)
    : Abstract_obj(ed_row[0] /* database name */)
  { }

  Database_obj(LEX_STRING db_name)
    : Abstract_obj(db_name)
  { }

public:
  virtual inline const String *get_db_name() const { return get_name(); }

private:
  virtual bool do_serialize(THD *thd, Out_stream &out_stream);
  virtual void build_drop_statement(String_stream &s_stream) const;
};

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Database_item_obj

  This class is a base class for all classes representing objects, which
  belong to a Database.
*/

class Database_item_obj : public Abstract_obj
{
public:
  Database_item_obj(LEX_STRING db_name, LEX_STRING object_name)
    : Abstract_obj(object_name)
  {
    if (db_name.str && db_name.length)
      m_db_name.copy(db_name.str, db_name.length, system_charset_info);
    else
      m_db_name.length(0);
  }

public:
  virtual inline const String *get_db_name() const { return &m_db_name; }

protected:
  virtual const LEX_STRING *get_type_name() const = 0;
  virtual void build_drop_statement(String_stream &s_stream) const;

protected:
  String m_db_name;
};

///////////////////////////////////////////////////////////////////////////

void Database_item_obj::build_drop_statement(String_stream &s_stream) const
{
  s_stream <<
    "DROP " << get_type_name() << " IF EXISTS `" << get_name() << "`";
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Table_obj

  This class provides an abstraction to a table object for creation and
  capture of the creation data.
*/

class Table_obj : public Database_item_obj
{
private:
  static const LEX_STRING TYPE_NAME;

public:
  Table_obj(const Ed_row &ed_row)
    : Database_item_obj(ed_row[0], /* database name */
                        ed_row[1]) /* table name */
  { }

  Table_obj(LEX_STRING db_name, LEX_STRING table_name)
    : Database_item_obj(db_name, table_name)
  { }

private:
  virtual bool do_serialize(THD *thd, Out_stream &out_stream);

  virtual const LEX_STRING *get_type_name() const
  { return &Table_obj::TYPE_NAME; }
};

///////////////////////////////////////////////////////////////////////////

const LEX_STRING Table_obj::TYPE_NAME= LXS_INIT("TABLE");

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class View_obj

  This class provides an abstraction to a view object for creation and
  capture of the creation data.
*/

class View_obj : public Database_item_obj
{
private:
  static const LEX_STRING TYPE_NAME;

public:
  View_obj(const Ed_row &ed_row)
    : Database_item_obj(ed_row[0], /* schema name */
                        ed_row[1]) /* view name */
  { }

  View_obj(LEX_STRING db_name, LEX_STRING view_name)
    : Database_item_obj(db_name, view_name)
  { }

private:
  virtual bool do_serialize(THD *thd, Out_stream &out_stream);

  virtual const LEX_STRING *get_type_name() const
  { return &View_obj::TYPE_NAME; }
};

///////////////////////////////////////////////////////////////////////////

const LEX_STRING View_obj::TYPE_NAME= LXS_INIT("VIEW");

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Stored_program_obj : public Database_item_obj
{
public:
  Stored_program_obj(LEX_STRING db_name, LEX_STRING sp_name)
    : Database_item_obj(db_name, sp_name)
  { }

protected:
  virtual const LEX_STRING *get_create_stmt(Ed_row *row) = 0;
  virtual void dump_header(Ed_row *row, Out_stream &out_stream) = 0;

private:
  virtual bool do_serialize(THD *thd, Out_stream &out_stream);
};

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

class Stored_routine_obj : public Stored_program_obj
{
public:
  Stored_routine_obj(LEX_STRING db_name, LEX_STRING sr_name)
    : Stored_program_obj(db_name, sr_name)
  { }

protected:
  virtual const LEX_STRING *get_create_stmt(Ed_row *row);
  virtual void dump_header(Ed_row *row, Out_stream &out_stream);
};

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Trigger_obj

  This class provides an abstraction to a trigger object for creation and
  capture of the creation data.
*/

class Trigger_obj : public Stored_routine_obj
{
private:
  static const LEX_STRING TYPE_NAME;

public:
  Trigger_obj(const Ed_row &ed_row)
    : Stored_routine_obj(ed_row[0], ed_row[1])
  { }

  Trigger_obj(LEX_STRING db_name, LEX_STRING trigger_name)
    : Stored_routine_obj(db_name, trigger_name)
  { }

private:
  virtual const LEX_STRING *get_type_name() const
  { return &Trigger_obj::TYPE_NAME; }
};

///////////////////////////////////////////////////////////////////////////

const LEX_STRING Trigger_obj::TYPE_NAME= LXS_INIT("TRIGGER");

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Stored_proc_obj

  This class provides an abstraction to a stored procedure object for creation
  and capture of the creation data.
*/

class Stored_proc_obj : public Stored_routine_obj
{
private:
  static const LEX_STRING TYPE_NAME;

public:
  Stored_proc_obj(const Ed_row &ed_row)
    : Stored_routine_obj(ed_row[0], ed_row[1])
  { }

  Stored_proc_obj(LEX_STRING db_name, LEX_STRING sp_name)
    : Stored_routine_obj(db_name, sp_name)
  { }

private:
  virtual const LEX_STRING *get_type_name() const
  { return &Stored_proc_obj::TYPE_NAME; }
};

///////////////////////////////////////////////////////////////////////////

const LEX_STRING Stored_proc_obj::TYPE_NAME= LXS_INIT("PROCEDURE");

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Stored_func_obj

  This class provides an abstraction to a stored function object for creation
  and capture of the creation data.
*/

class Stored_func_obj : public Stored_routine_obj
{
private:
  static const LEX_STRING TYPE_NAME;

public:
  Stored_func_obj(const Ed_row &ed_row)
    : Stored_routine_obj(ed_row[0], ed_row[1])
  { }

  Stored_func_obj(LEX_STRING db_name, LEX_STRING sf_name)
    : Stored_routine_obj(db_name, sf_name)
  { }

private:
  virtual const LEX_STRING *get_type_name() const
  { return &Stored_func_obj::TYPE_NAME; }
};

///////////////////////////////////////////////////////////////////////////

const LEX_STRING Stored_func_obj::TYPE_NAME= LXS_INIT("FUNCTION");

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

#ifdef HAVE_EVENT_SCHEDULER

/**
  @class Event_obj

  This class provides an abstraction to a event object for creation and capture
  of the creation data.
*/

class Event_obj : public Stored_program_obj
{
private:
  static const LEX_STRING TYPE_NAME;

public:
  Event_obj(const Ed_row &ed_row)
    : Stored_program_obj(ed_row[0], ed_row[1])
  { }

  Event_obj(LEX_STRING db_name, LEX_STRING event_name)
    : Stored_program_obj(db_name, event_name)
  { }

private:
  virtual const LEX_STRING *get_type_name() const
  { return &Event_obj::TYPE_NAME; }

  virtual const LEX_STRING *get_create_stmt(Ed_row *row);
  virtual void dump_header(Ed_row *row, Out_stream &out_stream);
};

///////////////////////////////////////////////////////////////////////////

const LEX_STRING Event_obj::TYPE_NAME= LXS_INIT("EVENT");

///////////////////////////////////////////////////////////////////////////

const LEX_STRING *Event_obj::get_create_stmt(Ed_row *row)
{
  return row->get_column(3);
}

#endif // HAVE_EVENT_SCHEDULER

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Tablespace_obj

  This class provides an abstraction to a user object for creation and
  capture of the creation data.
*/

class Tablespace_obj : public Abstract_obj
{
public:
  Tablespace_obj(LEX_STRING ts_name,
                 LEX_STRING comment,
                 LEX_STRING data_file_name,
                 LEX_STRING engine_name);

  Tablespace_obj(LEX_STRING ts_name);

public:
  const String *get_description();

public:
  virtual bool init_from_image(uint image_version, const String *image);

private:
  virtual bool do_serialize(THD *thd, Out_stream &out_stream);
  virtual void build_drop_statement(String_stream &s_stream) const;

private:
  /* These attributes are to be used only for serialization. */
  String m_comment;
  String m_data_file_name;
  String m_engine;

private:
  String m_description;
};

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  @class Grant_obj

  This class provides an abstraction to grants. This class will permit the
  recording and replaying of these grants.
*/

class Grant_obj : public Abstract_obj
{
public:
  static void generate_unique_grant_id(const String *user_name, String *id);

public:
  Grant_obj(LEX_STRING grant_id);
  Grant_obj(const Ed_row &ed_row);

public:
  inline const String *get_user_name() const { return &m_user_name; }
  inline const String *get_grant_info() const { return &m_grant_info; }

private:
  virtual bool do_init_from_image(In_stream *is);
  inline virtual void build_drop_statement(String_stream &s_stream) const
  { }

private:
  /* These attributes are to be used only for serialization. */
  String m_user_name;
  String m_grant_info; //< contains privilege definition

private:
  virtual bool do_serialize(THD *thd, Out_stream &out_stream);
};

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

template <typename Iterator>
Iterator *create_row_set_iterator(THD *thd, const LEX_STRING *query)
{
  Ed_result *ed_result= new Ed_result();

  if (run_service_interface_sql(thd, query, ed_result) ||
      ed_result->get_warnings().elements > 0)
  {
    /* There should be no warnings. */
    delete ed_result;
    return NULL;
  }

  /* The result must contain only one result-set. */
  DBUG_ASSERT(ed_result->elements == 1);

  return new Iterator(ed_result);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

template <typename Obj_type>
class Ed_result_set_iterator : public Obj_iterator
{
public:
  inline Ed_result_set_iterator(Ed_result *ed_result);
  inline ~Ed_result_set_iterator();
  virtual Obj *next();
private:
  Ed_result *m_ed_result;
  List_iterator_fast<Ed_row> m_row_it;
};

///////////////////////////////////////////////////////////////////////////

template <typename Obj_type>
inline
Ed_result_set_iterator<Obj_type>::
Ed_result_set_iterator(Ed_result *ed_result)
  : m_ed_result(ed_result),
    m_row_it(*ed_result->get_cur_result_set()->data())
{ }

///////////////////////////////////////////////////////////////////////////

template <typename Obj_type>
inline
Ed_result_set_iterator<Obj_type>::~Ed_result_set_iterator()
{
  delete m_ed_result;
}


template <typename Obj_type>
Obj *
Ed_result_set_iterator<Obj_type>::next()
{
  Ed_row *ed_row= m_row_it++;

  if (!ed_row)
    return NULL;

  return new Obj_type(*ed_row);
}

///////////////////////////////////////////////////////////////////////////

typedef Ed_result_set_iterator<Database_obj>    Database_iterator;
typedef Ed_result_set_iterator<Table_obj>       Db_tables_iterator;
typedef Ed_result_set_iterator<View_obj>        Db_views_iterator;
typedef Ed_result_set_iterator<Trigger_obj>     Db_trigger_iterator;
typedef Ed_result_set_iterator<Stored_proc_obj> Db_stored_proc_iterator;
typedef Ed_result_set_iterator<Stored_func_obj> Db_stored_func_iterator;
#ifdef HAVE_EVENT_SCHEDULER
typedef Ed_result_set_iterator<Event_obj>       Db_event_iterator;
#endif
typedef Ed_result_set_iterator<Grant_obj>       Grant_iterator;

///////////////////////////////////////////////////////////////////////////

#ifdef HAVE_EXPLICIT_TEMPLATE_INSTANTIATION
template class Ed_result_set_iterator<Database_obj>;
template class Ed_result_set_iterator<Table_obj>;
template class Ed_result_set_iterator<View_obj>;
template class Ed_result_set_iterator<Trigger_obj>;
template class Ed_result_set_iterator<Stored_proc_obj>;
template class Ed_result_set_iterator<Stored_func_obj>;
#ifdef HAVE_EVENT_SCHEDULER
template class Ed_result_set_iterator<Event_obj>;
#endif
template class Ed_result_set_iterator<Grant_obj>;

template
Database_iterator *
create_row_set_iterator<Database_iterator>(THD *thd, const LEX_STRING *query);

template
Db_tables_iterator *
create_row_set_iterator<Db_tables_iterator>(THD *thd, const LEX_STRING *query);

template
Db_views_iterator *
create_row_set_iterator<Db_views_iterator>(THD *thd, const LEX_STRING *query);

template
Db_trigger_iterator *
create_row_set_iterator<Db_trigger_iterator>(THD *thd, const LEX_STRING *query);

template
Db_stored_proc_iterator *
create_row_set_iterator<Db_stored_proc_iterator>(THD *thd,
                                                 const LEX_STRING *query);

template
Db_stored_func_iterator *
create_row_set_iterator<Db_stored_func_iterator>(THD *thd,
                                                 const LEX_STRING *query);

#ifdef HAVE_EVENT_SCHEDULER
template
Db_event_iterator *
create_row_set_iterator<Db_event_iterator>(THD *thd, const LEX_STRING *query);
#endif

template
Grant_iterator *
create_row_set_iterator<Grant_iterator>(THD *thd, const LEX_STRING *query);

#endif // HAVE_EXPLICIT_TEMPLATE_INSTANTIATION

///////////////////////////////////////////////////////////////////////////

class View_base_obj_iterator : public Obj_iterator
{
public:
  View_base_obj_iterator();
  virtual ~View_base_obj_iterator();

public:
  virtual Obj *next();

  enum enum_base_obj_kind { BASE_TABLE= 0, VIEW };

  virtual enum_base_obj_kind get_base_obj_kind() const= 0;

protected:
  template <typename Iterator>
  static Iterator *create(THD *thd,
                          const String *db_name, const String *view_name);

protected:
  bool init(THD *thd, const String *db_name, const String *view_name);

  virtual Obj *create_obj(const LEX_STRING *db_name,
                          const LEX_STRING *obj_name)= 0;

private:
  HASH m_table_names;
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

View_base_obj_iterator::View_base_obj_iterator()
  :m_cur_idx(0)
{
  hash_init(&m_table_names, system_charset_info, 16, 0, 0,
            get_table_name_key, free_table_name_key,
            MYF(0));

}

///////////////////////////////////////////////////////////////////////////

View_base_obj_iterator::~View_base_obj_iterator()
{
  hash_free(&m_table_names);
}


/**
  Find all base tables or views of a view.
*/

class Find_view_underlying_tables: public Server_runnable
{
public:
  Find_view_underlying_tables(const View_base_obj_iterator *base_obj_it,
                              HASH *table_names,
                              const String *db_name,
                              const String *view_name);

  bool execute_server_code(THD *thd);
private:
  /* Store the resulting unique here */
  const View_base_obj_iterator *m_base_obj_it;
  HASH *m_table_names;
  const String *m_db_name;
  const String *m_view_name;
};


Find_view_underlying_tables::
Find_view_underlying_tables(const View_base_obj_iterator *base_obj_it,
                            HASH *table_names,
                            const String *db_name,
                            const String *view_name)
  :m_base_obj_it(base_obj_it),
   m_table_names(table_names),
   m_db_name(db_name),
   m_view_name(view_name)
{
}


bool
Find_view_underlying_tables::execute_server_code(THD *thd)
{
  bool res= TRUE;
  uint counter_not_used; /* Passed to open_tables(). Not used. */

  TABLE_LIST *table_list;

  DBUG_ENTER("Find_view_underlying_tables::execute_server_code");

  table_list= sp_add_to_query_tables(thd, thd->lex,
                                     ((String *) m_db_name)->c_ptr_safe(),
                                     ((String *) m_view_name)->c_ptr_safe(),
                                     TL_READ);

  if (table_list == NULL)                      /* out of memory, reported */
    DBUG_RETURN(TRUE);

  if (open_tables(thd, &table_list, &counter_not_used,
                  MYSQL_OPEN_SKIP_TEMPORARY))
    goto end;

  if (table_list->view_tables)
  {
    TABLE_LIST *table;
    List_iterator_fast<TABLE_LIST> it(*table_list->view_tables);

    /*
      Iterate over immediate underlying tables.
      The list doesn't include views or tables referenced indirectly,
      through other views, or stored functions or triggers.
    */
    while ((table= it++))
    {
      Table_name_key *table_name_key;
      char *key_buff;

      /* If we expect a view, and it's a table, or vice versa, continue */
      if ((int) m_base_obj_it->get_base_obj_kind() != test(table->view))
        continue;

      if (! my_multi_malloc(MYF(MY_WME),
                            &table_name_key, sizeof(*table_name_key),
                            &key_buff, table->mdl_lock_data->key_length,
                            NullS))
        goto end;

      table_name_key->init_from_mdl_key(table->mdl_lock_data->key,
                                        table->mdl_lock_data->key_length,
                                        key_buff);

      if (my_hash_insert(m_table_names, (uchar*) table_name_key))
      {
        my_free(table_name_key, MYF(0));
        goto end;
      }
    }
  }
  res= FALSE;
  my_ok(thd);

end:
  close_thread_tables(thd);
  DBUG_RETURN(res);
}

///////////////////////////////////////////////////////////////////////////

bool View_base_obj_iterator::init(THD *thd,
                                  const String *db_name,
                                  const String *view_name)
{
  Find_view_underlying_tables find_tables(this, &m_table_names,
                                          db_name, view_name);
  Ed_result ed_result; /* Just to grab OK or ERROR */

  if (mysql_execute_direct(thd, &find_tables, &ed_result))
    return TRUE;

  /* The table list is filled with unique underlying table names. */

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

Obj *View_base_obj_iterator::next()
{
  if (m_cur_idx >= m_table_names.records)
    return NULL;

  Table_name_key *table_name_key=
    (Table_name_key *) hash_element(&m_table_names, m_cur_idx);

  ++m_cur_idx;

  return create_obj(&table_name_key->db_name, &table_name_key->table_name);
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
  virtual enum_base_obj_kind get_base_obj_kind() const { return BASE_TABLE; }

  virtual Obj *create_obj(const LEX_STRING *db_name,
                          const LEX_STRING *obj_name)
  {
    return new Table_obj(*db_name, *obj_name);
  }
};

///////////////////////////////////////////////////////////////////////////

template
View_base_table_iterator *
View_base_obj_iterator::
create<View_base_table_iterator>(THD *thd, const String *db_name,
                                 const String *view_name);

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
  virtual enum_base_obj_kind get_base_obj_kind() const { return VIEW; }

  virtual Obj *create_obj(const LEX_STRING *db_name,
                          const LEX_STRING *obj_name)
  {
    return new View_obj(*db_name, *obj_name);
  }
};

///////////////////////////////////////////////////////////////////////////

template
View_base_view_iterator *
View_base_obj_iterator::
create<View_base_view_iterator>(THD *thd, const String *db_name,
                                const String *view_name);

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd         Thread context.
  @param[out] out_stream  Output stream.

  @note this method will return an error if the db_name is either
  mysql or information_schema as these are not objects that
  should be recreated using this interface.

  @returns Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Database_obj::do_serialize(THD *thd, Out_stream &out_stream)
{
  DBUG_ENTER("Database_obj::do_serialize");
  DBUG_PRINT("Database_obj::do_serialize",
             ("name: %.*s", STR(*get_name())));

  if (is_internal_db_name(get_name()))
  {
    DBUG_PRINT("backup",
               (" Skipping internal database %.*s", STR(*get_name())));

    DBUG_RETURN(TRUE);
  }

  /* Run 'SHOW CREATE' query. */

  Ed_result ed_result;

  {
    String_stream s_stream;
    s_stream <<
      "SHOW CREATE DATABASE `" << get_name() << "`";

    if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
        ed_result.get_warnings().elements > 0)
    {
      /*
        There should be no warnings. A warning means that serialization has
        failed.
      */
      DBUG_RETURN(TRUE);
    }
  }

  /* Check result. */

  /* The result must contain only one result-set... */
  DBUG_ASSERT(ed_result.elements == 1);

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  /* ... which is not NULL. */
  DBUG_ASSERT(ed_result_set);

  if (ed_result_set->data()->elements == 0)
    DBUG_RETURN(TRUE);

  /* There must be one row. */
  DBUG_ASSERT(ed_result_set->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*ed_result_set->data());
  Ed_row *row= row_it++;

  /* There must be two columns: database name and create statement. */
  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  /* Generate image. */

  out_stream << row->get_column(1);

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////

void Database_obj::build_drop_statement(String_stream &s_stream) const
{
  s_stream <<
    "DROP DATABASE IF EXISTS `" << get_name() << "`";
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd         Thread context.
  @param[out] out_stream  Output stream.

  @returns Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Table_obj::do_serialize(THD *thd, Out_stream &out_stream)
{
  DBUG_ENTER("Table_obj::do_serialize");
  DBUG_PRINT("Table_obj::do_serialize",
             ("name: %.*s.%.*s",
              STR(m_db_name), STR(m_id)));

  Ed_result ed_result;
  String_stream s_stream;

  s_stream <<
    "SHOW CREATE TABLE `" << &m_db_name << "`.`" << &m_id << "`";

  if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
      ed_result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  /* Check result. */

  /* The result must contain only one result-set... */
  DBUG_ASSERT(ed_result.elements == 1);

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  /* ... which is not NULL. */
  DBUG_ASSERT(ed_result_set);

  if (ed_result_set->data()->elements == 0)
    DBUG_RETURN(TRUE);

  /* There must be one row. */
  DBUG_ASSERT(ed_result_set->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*ed_result_set->data());
  Ed_row *row= row_it++;

  /* There must be two columns: database name and create statement. */
  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 2);

  /* Generate serialization image. */

  {
    s_stream.reset();
    s_stream << "USE `" << &m_db_name << "`";
    out_stream << s_stream;
  }

  out_stream << row->get_column(1);

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

static bool
get_view_create_stmt(THD *thd,
                     View_obj *view,
                     LEX_STRING *create_stmt,
                     LEX_STRING *client_cs_name,
                     LEX_STRING *connection_cl_name)
{
  /* Get a create statement for a view. */
  Ed_result ed_result;
  String_stream s_stream;

  s_stream <<
    "SHOW CREATE VIEW `" << view->get_db_name() << "`."
    "`" << view->get_name() << "`";

  if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
      ed_result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    return TRUE;
  }

  /* The result must contain only one result-set... */
  DBUG_ASSERT(ed_result.elements == 1);

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  /* ... which is not NULL. */
  DBUG_ASSERT(ed_result_set);

  if (ed_result_set->data()->elements == 0)
    return TRUE;

  /* There must be one row. */
  DBUG_ASSERT(ed_result_set->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*ed_result_set->data());
  Ed_row *row= row_it++;

  /* There must be four columns. */
  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 4);

  const LEX_STRING *c1= row->get_column(1);
  const LEX_STRING *c2= row->get_column(2);
  const LEX_STRING *c3= row->get_column(3);

  create_stmt->str= thd->strmake(c1->str, c1->length);
  create_stmt->length= c1->length;

  client_cs_name->str= thd->strmake(c2->str, c2->length);
  client_cs_name->length= c2->length;

  connection_cl_name->str= thd->strmake(c3->str, c3->length);
  connection_cl_name->length= c3->length;

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd         Thread context.
  @param[out] out_stream  Output stream.

  @returns Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/
bool View_obj::do_serialize(THD *thd, Out_stream &out_stream)
{
  DBUG_ENTER("View_obj::do_serialize");
  DBUG_PRINT("View_obj::do_serialize",
             ("name: %.*s.%.*s",
              STR(m_db_name), STR(m_id)));

  String_stream s_stream;

  LEX_STRING create_stmt;
  LEX_STRING client_cs_name;
  LEX_STRING connection_cl_name;

  if (get_view_create_stmt(thd, this, &create_stmt,
                           &client_cs_name, &connection_cl_name))
  {
    DBUG_RETURN(TRUE);
  }

  s_stream << "USE `" << &m_db_name << "`";
  out_stream << s_stream;

  s_stream.reset();
  s_stream << "SET character_set_client = " << &client_cs_name;
  out_stream << s_stream;

  s_stream.reset();
  s_stream << "SET collation_connection = " << &connection_cl_name;
  out_stream << s_stream;

  out_stream << &create_stmt;

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

bool Stored_program_obj::do_serialize(THD *thd, Out_stream &out_stream)
{
  DBUG_ENTER("Stored_program_obj::do_serialize");
  DBUG_PRINT("Stored_program_obj::do_serialize",
             ("name: %.*s.%.*s",
              STR(m_db_name), STR(m_id)));

  DBUG_EXECUTE_IF("backup_fail_add_trigger", DBUG_RETURN(TRUE););

  String_stream s_stream;
  Ed_result ed_result;

  s_stream <<
    "SHOW CREATE " << get_type_name() <<
    " `" << &m_db_name << "`.`" << &m_id << "`";

  if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
      ed_result.get_warnings().elements > 0)
  {
    /*
      There should be no warnings. A warning means that serialization has
      failed.
    */
    DBUG_RETURN(TRUE);
  }

  /* The result must contain only one result-set... */
  DBUG_ASSERT(ed_result.elements == 1);

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  /* ... which is not NULL. */
  DBUG_ASSERT(ed_result_set);

  if (ed_result_set->data()->elements == 0)
    DBUG_RETURN(TRUE);

  /* There must be one row. */
  DBUG_ASSERT(ed_result_set->data()->elements == 1);

  List_iterator_fast<Ed_row> row_it(*ed_result_set->data());
  Ed_row *row= row_it++;

  s_stream.reset();
  s_stream << "USE `" << &m_db_name << "`";
  out_stream << s_stream;

  dump_header(row, out_stream);
  out_stream << get_create_stmt(row);

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

const LEX_STRING *Stored_routine_obj::get_create_stmt(Ed_row *row)
{
  return row->get_column(2);
}

///////////////////////////////////////////////////////////////////////////

void Stored_routine_obj::dump_header(Ed_row *row, Out_stream &out_stream)
{
  String_stream s_stream;

  s_stream <<
    "SET character_set_client = " << row->get_column(3);
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET collation_connection = " << row->get_column(4);
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET collation_database = " << row->get_column(5);
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET sql_mode = '" << row->get_column(1) << "'";
  out_stream << s_stream;
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#ifdef HAVE_EVENT_SCHEDULER

void Event_obj::dump_header(Ed_row *row, Out_stream &out_stream)
{
  String_stream s_stream;

  s_stream <<
    "SET character_set_client = " << row->get_column(4);
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET collation_connection = " << row->get_column(5);
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET collation_database = " << row->get_column(6);
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET sql_mode = '" << row->get_column(1) << "'";
  out_stream << s_stream;

  s_stream.reset();
  s_stream <<
    "SET time_zone = '" << row->get_column(2) << "'";
  out_stream << s_stream;
}

#endif // HAVE_EVENT_SCHEDULER

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

Tablespace_obj::
Tablespace_obj(LEX_STRING ts_name,
               LEX_STRING comment,
               LEX_STRING data_file_name,
               LEX_STRING engine)
  : Abstract_obj(ts_name)
{
  m_comment.copy(comment.str, comment.length, system_charset_info);
  m_data_file_name.copy(data_file_name.str, data_file_name.length,
                        system_charset_info);
  m_engine.copy(engine.str, engine.length, system_charset_info);

  m_description.length(0);
}


Tablespace_obj::Tablespace_obj(LEX_STRING ts_name)
  : Abstract_obj(ts_name)
{
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
  @param[out] out_stream  Output stream.

  @returns Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Tablespace_obj::do_serialize(THD *thd, Out_stream &out_stream)
{
  DBUG_ENTER("Tablespace_obj::do_serialize");

  out_stream << get_description();

  DBUG_RETURN(FALSE);
}

///////////////////////////////////////////////////////////////////////////

bool Tablespace_obj::init_from_image(uint image_version, const String *image)
{
  if (Abstract_obj::init_from_image(image_version, image))
    return TRUE;

  List_iterator_fast<String> it(m_stmt_list);
  String *desc= it++;

  /* Tablespace description must not be NULL. */
  DBUG_ASSERT(desc);

  m_description.set(desc->ptr(), desc->length(), desc->charset());

  return FALSE;
}

///////////////////////////////////////////////////////////////////////////

/**
  Get a description of the tablespace object.

  This method returns the description of the object which is currently
  the serialization image.

  @returns Serialization string.
*/

const String *Tablespace_obj::get_description()
{
  DBUG_ENTER("Tablespace_obj::get_description");

  /* Either description or id and data file name must be not empty. */
  DBUG_ASSERT(m_description.length() ||
              m_id.length() && m_data_file_name.length());

  if (m_description.length())
    DBUG_RETURN(&m_description);

  /* Construct the CREATE TABLESPACE command from the variables. */

  m_description.length(0);

  String_stream s_stream(&m_description);

  s_stream <<
    "CREATE TABLESPACE `" << &m_id << "` "
    "ADD DATAFILE '" << &m_data_file_name << "' ";

  if (m_comment.length())
    s_stream << "COMMENT = '" << &m_comment << "' ";

  s_stream << "ENGINE = " << &m_engine;

  DBUG_RETURN(&m_description);
}

///////////////////////////////////////////////////////////////////////////

void Tablespace_obj::build_drop_statement(String_stream &s_stream) const
{
  s_stream <<
    "DROP TABLESPACE `" << &m_id << "` ENGINE = " << &m_engine;
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

void
Grant_obj::
generate_unique_grant_id(const String *user_name, String *id)
{
  static unsigned long id_counter= 0;

  id->length(0);

  String_stream s_stream(id);

  if (user_name->length())
    s_stream << "<empty>";
  else
    s_stream << user_name;

  s_stream << " " << Int_value(++id_counter);
}

/////////////////////////////////////////////////////////////////////////////

Grant_obj::Grant_obj(const Ed_row &row)
  : Abstract_obj(null_lex_str)
{
  const LEX_STRING *user_name= row.get_column(0);
  const LEX_STRING *privilege_type= row.get_column(1);
  const LEX_STRING *db_name= row.get_column(2);
  const LEX_STRING *tbl_name= row.get_column(3);
  const LEX_STRING *col_name= row.get_column(4);

  LEX_STRING table_name= { C_STRING_WITH_LEN("") };
  LEX_STRING column_name= { C_STRING_WITH_LEN("") };

  if (tbl_name)
    table_name= *tbl_name;

  if (col_name)
    column_name= *col_name;

  m_user_name.copy(user_name->str, user_name->length, system_charset_info);

  /* Grant info. */

  String_stream s_stream(&m_grant_info);
  s_stream << privilege_type;

  if (column_name.length)
    s_stream << "(" << &column_name << ")";

  s_stream << " ON " << db_name << ".";

  if (table_name.length)
    s_stream << &table_name;
  else
    s_stream << "*";

  /* Id. */

  generate_unique_grant_id(&m_user_name, &m_id);
}

/////////////////////////////////////////////////////////////////////////////

Grant_obj::Grant_obj(LEX_STRING name)
  : Abstract_obj(null_lex_str)
{
  m_user_name.length(0);
  m_grant_info.length(0);

  m_id.copy(name.str, name.length, system_charset_info);
}

/////////////////////////////////////////////////////////////////////////////

/**
  Serialize the object.

  This method produces the data necessary for materializing the object
  on restore (creates object).

  @param[in]  thd Thread context.
  @param[out] out_stream  Output stream.

  @note this method will return an error if the db_name is either
  mysql or information_schema as these are not objects that
  should be recreated using this interface.

  @returns Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/

bool Grant_obj::do_serialize(THD *thd, Out_stream &out_stream)
{
  DBUG_ENTER("Grant_obj::do_serialize");

  out_stream <<
    &m_user_name <<
    &m_grant_info <<
    "SET character_set_client= binary";

  String_stream s_stream;
  s_stream << "GRANT " << &m_grant_info << " TO " << &m_user_name;

  out_stream << s_stream;

  DBUG_RETURN(FALSE);
}

/////////////////////////////////////////////////////////////////////////////

bool Grant_obj::do_init_from_image(In_stream *is)
{
  LEX_STRING user_name;
  LEX_STRING grant_info;

  if (is->next(&user_name))
    return TRUE; /* Can not decode user name. */

  if (is->next(&grant_info))
    return TRUE; /* Can not decode grant info. */

  m_user_name.copy(user_name.str, user_name.length, system_charset_info);
  m_grant_info.copy(grant_info.str, grant_info.length, system_charset_info);

  return Abstract_obj::do_init_from_image(is);
}

///////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

Obj *get_database_stub(const String *db_name)
{
  return new Database_obj(db_name->lex_string());
}

///////////////////////////////////////////////////////////////////////////

Obj_iterator *get_databases(THD *thd)
{
  LEX_STRING query=
  { C_STRING_WITH_LEN(
                      "SELECT schema_name "
                      "FROM INFORMATION_SCHEMA.SCHEMATA "
                      "WHERE LCASE(schema_name) != 'mysql' AND "
                      "LCASE(schema_name) != 'information_schema'") };

  return create_row_set_iterator<Database_iterator>(thd, &query);
}

Obj_iterator *get_db_tables(THD *thd, const String *db_name)
{
  String_stream s_stream;

  s_stream <<
    "SELECT '" << db_name << "', table_name "
    "FROM INFORMATION_SCHEMA.TABLES "
    "WHERE table_schema = '" << db_name << "' AND "
    "table_type = 'BASE TABLE'";

  return create_row_set_iterator<Db_tables_iterator>(thd, s_stream.lex_string());
}


Obj_iterator *get_db_views(THD *thd, const String *db_name)
{
  String_stream s_stream;
  s_stream <<
    "SELECT '" << db_name << "', table_name "
    "FROM INFORMATION_SCHEMA.TABLES "
    "WHERE table_schema = '" << db_name << "' AND table_type = 'VIEW'";

  return create_row_set_iterator<Db_views_iterator>(thd, s_stream.lex_string());
}


Obj_iterator *get_db_triggers(THD *thd, const String *db_name)
{
  String_stream s_stream;
  s_stream <<
    "SELECT '" << db_name << "', trigger_name "
    "FROM INFORMATION_SCHEMA.TRIGGERS "
    "WHERE trigger_schema = '" << db_name << "'";

  return create_row_set_iterator<Db_trigger_iterator>(thd, s_stream.lex_string());
}


Obj_iterator *get_db_stored_procedures(THD *thd, const String *db_name)
{
  String_stream s_stream;
  s_stream <<
    "SELECT '" << db_name << "', routine_name "
    "FROM INFORMATION_SCHEMA.ROUTINES "
    "WHERE routine_schema = '" << db_name << "' AND "
    "routine_type = 'PROCEDURE'";

  return create_row_set_iterator<Db_stored_proc_iterator>(thd, s_stream.lex_string());
}


Obj_iterator *get_db_stored_functions(THD *thd, const String *db_name)
{
  String_stream s_stream;
  s_stream <<
    "SELECT '" << db_name << "', routine_name "
    "FROM INFORMATION_SCHEMA.ROUTINES "
    "WHERE routine_schema = '" << db_name <<"' AND "
    "routine_type = 'FUNCTION'";

  return create_row_set_iterator<Db_stored_func_iterator>(thd, s_stream.lex_string());
}


Obj_iterator *get_db_events(THD *thd, const String *db_name)
{
#ifdef HAVE_EVENT_SCHEDULER
  String_stream s_stream;
  s_stream <<
    "SELECT '" << db_name << "', event_name "
    "FROM INFORMATION_SCHEMA.EVENTS "
    "WHERE event_schema = '" << db_name <<"'";

  return create_row_set_iterator<Db_event_iterator>(thd, s_stream.lex_string());
#else
  return NULL;
#endif
}


Obj_iterator *get_all_db_grants(THD *thd, const String *db_name)
{
  String_stream s_stream;
  s_stream <<
    "(SELECT t1.grantee AS c1, "
    "t1.privilege_type AS c2, "
    "t1.table_schema AS c3, "
    "NULL AS c4, "
    "NULL AS c5 "
    "FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES AS t1, "
    "INFORMATION_SCHEMA.USER_PRIVILEGES AS t2 "
    "WHERE t1.table_schema = '" << db_name << "' AND "
    "t1.grantee = t2.grantee) "
    "UNION "
    "(SELECT t1.grantee, "
    "t1.privilege_type, "
    "t1.table_schema, "
    "t1.table_name, "
    "NULL "
    "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES AS t1, "
    "INFORMATION_SCHEMA.USER_PRIVILEGES AS t2 "
    "WHERE t1.table_schema = '" << db_name << "' AND "
    "t1.grantee = t2.grantee) "
    "UNION "
    "(SELECT t1.grantee, "
    "t1.privilege_type, "
    "t1.table_schema, "
    "t1.table_name, "
    "t1.column_name "
    "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES AS t1, "
    "INFORMATION_SCHEMA.USER_PRIVILEGES AS t2 "
    "WHERE t1.table_schema = '" << db_name << "' AND "
    "t1.grantee = t2.grantee) "
    "ORDER BY c1 ASC, c2 ASC, c3 ASC, c4 ASC, c5 ASC";

  return create_row_set_iterator<Grant_iterator>(thd, s_stream.lex_string());
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

Obj *get_database(const String *db_name,
                  uint image_version,
                  const String *image)
{
  Database_obj *obj= new Database_obj(db_name->lex_string());
  obj->init_from_image(image_version, image);

  return obj;
}

Obj *get_table(const String *db_name,
               const String *table_name,
               uint image_version,
               const String *image)
{
  Table_obj *obj= new Table_obj(db_name->lex_string(),
                                table_name->lex_string());
  obj->init_from_image(image_version, image);

  return obj;
}

Obj *get_view(const String *db_name,
              const String *view_name,
              uint image_version,
              const String *image)
{
  View_obj *obj= new View_obj(db_name->lex_string(), view_name->lex_string());
  obj->init_from_image(image_version, image);

  return obj;
}

Obj *get_trigger(const String *db_name,
                 const String *trigger_name,
                 uint image_version,
                 const String *image)
{
  Trigger_obj *obj= new Trigger_obj(db_name->lex_string(),
                                    trigger_name->lex_string());
  if (obj->init_from_image(image_version, image))
  {
    delete obj;
    obj= 0;
  }

  return obj;
}

Obj *get_stored_procedure(const String *db_name,
                          const String *sp_name,
                          uint image_version,
                          const String *image)
{
  Stored_proc_obj *obj= new Stored_proc_obj(db_name->lex_string(),
                                            sp_name->lex_string());
  if (obj->init_from_image(image_version, image))
  {
    delete obj;
    obj= 0;
  }

  return obj;
}

Obj *get_stored_function(const String *db_name,
                         const String *sf_name,
                         uint image_version,
                         const String *image)
{
  Stored_func_obj *obj= new Stored_func_obj(db_name->lex_string(),
                                            sf_name->lex_string());
  if (obj->init_from_image(image_version, image))
  {
    delete obj;
    obj= 0;
  }

  return obj;
}

#ifdef HAVE_EVENT_SCHEDULER

Obj *get_event(const String *db_name,
               const String *event_name,
               uint image_version,
               const String *image)
{
  Event_obj *obj= new Event_obj(db_name->lex_string(),
                                event_name->lex_string());
  if (obj->init_from_image(image_version, image))
  {
    delete obj;
    obj= 0;
  }

  return obj;
}

#endif

Obj *get_tablespace(const String *ts_name,
                    uint image_version,
                    const String *image)
{
  Tablespace_obj *obj= new Tablespace_obj(ts_name->lex_string());
  if (obj->init_from_image(image_version, image))
  {
    delete obj;
    obj= 0;
  }

  return obj;
}

Obj *get_db_grant(const String *db_name,
                  const String *name,
                  uint image_version,
                  const String *image)
{
  Grant_obj *obj= new Grant_obj(name->lex_string());
  if (obj->init_from_image(image_version, image))
  {
    delete obj;
    obj= 0;
  }

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
  String_stream s_stream;
  int rc;

  s_stream << "SHOW CREATE DATABASE `" << db_name << "`";

  Ed_result ed_result;
  rc= run_service_interface_sql(thd, s_stream.lex_string(), &ed_result);

  /* We're not interested in warnings/errors here. */

  return test(rc);
}

///////////////////////////////////////////////////////////////////////////

bool check_user_existence(THD *thd, const Obj *obj)
{
#ifdef EMBEDDED_LIBRARY
  return TRUE;
#else
  Grant_obj *grant_obj= (Grant_obj *) obj;
  Ed_result ed_result;
  String_stream s_stream;

  s_stream <<
    "SELECT 1 "
    "FROM INFORMATION_SCHEMA.USER_PRIVILEGES "
    "WHERE grantee = \"" << grant_obj->get_user_name() << "\"";


  if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
      ed_result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return FALSE;
  }

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  if (!ed_result_set)
    return FALSE;

  return ed_result_set->data()->elements > 0;
#endif
}

///////////////////////////////////////////////////////////////////////////

const String *grant_get_user_name(const Obj *obj)
{
  return ((Grant_obj *) obj)->get_user_name();
}

///////////////////////////////////////////////////////////////////////////

const String *grant_get_grant_info(const Obj *obj)
{
  return ((Grant_obj *) obj)->get_grant_info();
}

///////////////////////////////////////////////////////////////////////////

Obj *find_tablespace(THD *thd, const String *ts_name)
{
  Ed_result ed_result;
  String_stream s_stream;

  s_stream <<
    "SELECT t1.tablespace_comment, t2.file_name, t1.engine "
    "FROM INFORMATION_SCHEMA.TABLESPACES AS t1, "
    "INFORMATION_SCHEMA.FILES AS t2 "
    "WHERE t1.tablespace_name = t2.tablespace_name AND "
    "t1.tablespace_name = '" << ts_name << "'";


  if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
      ed_result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return NULL;
  }

  if (!ed_result.elements)
    return NULL;

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  /* The result must contain only one result-set. */
  DBUG_ASSERT(ed_result_set->data()->elements == 1);

  Ed_row *row= ed_result_set->get_cur_row();

  /* There must be 3 columns. */
  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 3);

  const LEX_STRING *comment= row->get_column(0);
  const LEX_STRING *data_file_name= row->get_column(1);
  const LEX_STRING *engine= row->get_column(2);

  return new Tablespace_obj(ts_name->lex_string(),
                            *comment, *data_file_name, *engine);
}

///////////////////////////////////////////////////////////////////////////

/**
  Retrieve the tablespace for a table if it exists

  This method returns a @c Tablespace_obj object if the table has a tablespace.

  @param[in]  thd         Thread context.
  @param[in]  db_name     The database name for the table.
  @param[in]  table_name  The table name.

  @note Caller is responsible for destroying the object.

  @return Tablespace object.
    @retval Tablespace object if table uses a tablespace.
    @retval NULL if table does not use a tablespace.
*/

Obj *find_tablespace_for_table(THD *thd,
                               const String *db_name,
                               const String *table_name)
{
  Ed_result ed_result;
  String_stream s_stream;

  s_stream <<
    "SELECT t1.tablespace_name, t1.engine, t1.tablespace_comment, t2.file_name "
    "FROM INFORMATION_SCHEMA.TABLESPACES AS t1, "
    "INFORMATION_SCHEMA.FILES AS t2, "
    "INFORMATION_SCHEMA.TABLES AS t3 "
    "WHERE t1.tablespace_name = t2.tablespace_name AND "
    "t2.tablespace_name = t3.tablespace_name AND "
    "t3.table_schema = '" << db_name << "' AND "
    "t3.table_name = '" << table_name << "'";


  if (run_service_interface_sql(thd, s_stream.lex_string(), &ed_result) ||
      ed_result.get_warnings().elements > 0)
  {
    /* Should be no warnings. */
    return NULL;
  }

  if (!ed_result.elements)
    return NULL;

  Ed_result_set *ed_result_set= ed_result.get_cur_result_set();

  if (!ed_result_set->data()->elements)
    return NULL;

  /* The result must contain only one result-set. */
  DBUG_ASSERT(ed_result_set->data()->elements == 1);

  Ed_row *row= ed_result_set->get_cur_row();

  /* There must be 4 columns. */
  DBUG_ASSERT(row->get_metadata()->get_num_columns() == 4);

  const LEX_STRING *ts_name= row->get_column(0);
  const LEX_STRING *engine= row->get_column(1);
  const LEX_STRING *comment= row->get_column(2);
  const LEX_STRING *data_file_name= row->get_column(3);

  return new Tablespace_obj(*ts_name, *comment, *data_file_name, *engine);
}

///////////////////////////////////////////////////////////////////////////

bool compare_tablespace_attributes(Obj *ts1, Obj *ts2)
{
  DBUG_ENTER("obs::compare_tablespace_attributes");

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

  @return Error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/
bool ddl_blocker_enable(THD *thd)
{
  DBUG_ENTER("obs::ddl_blocker_enable");
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
  DBUG_ENTER("obs::ddl_blocker_disable");
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
  DBUG_ENTER("obs::ddl_blocker_exception_on");
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
  DBUG_ENTER("obs::ddl_blocker_exception_off");
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
  DBUG_ENTER("Name_locker::build_table_list");

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


void Name_locker::free_table_list(TABLE_LIST *table_list)
{
  TABLE_LIST *ptr= table_list;

  while (ptr)
  {
    table_list= table_list->next_global;
    my_free(ptr, MYF(0));
    ptr= table_list;
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
  DBUG_ENTER("Name_locker::get_name_locks");
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
  DBUG_ENTER("Name_locker::release_name_locks");
  if (m_table_list)
  {
    pthread_mutex_unlock(&LOCK_open);
    unlock_table_names(m_thd);
  }
  DBUG_RETURN(0);
}

///////////////////////////////////////////////////////////////////////////

} // obs namespace
