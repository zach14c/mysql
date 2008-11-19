/* Copyright (C) 2002-2006 MySQL AB

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

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "sql_error.h"

class i_string;
class THD;
class Item_param;
typedef struct st_mysql_field MYSQL_FIELD;
typedef struct st_mysql_rows MYSQL_ROWS;

class Protocol
{
protected:
  THD	 *thd;
  String *packet;
  String *convert;
  uint field_pos;
#ifndef DBUG_OFF
  enum enum_field_types *field_types;
#endif
  uint field_count;
#ifndef EMBEDDED_LIBRARY
  bool net_store_data(const uchar *from, size_t length);
#else
  virtual bool net_store_data(const uchar *from, size_t length);
  char **next_field;
  MYSQL_FIELD *next_mysql_field;
  MEM_ROOT *alloc;
#endif
  bool net_store_data(const uchar *from, size_t length,
                      CHARSET_INFO *fromcs, CHARSET_INFO *tocs);
  bool store_string_aux(const char *from, size_t length,
                        CHARSET_INFO *fromcs, CHARSET_INFO *tocs);

  virtual void send_ok(uint server_status, uint statement_warn_count,
                       ha_rows affected_rows, ulonglong last_insert_id,
                       const char *message);

  virtual void send_eof(uint server_status, uint statement_warn_count);

  virtual void send_error(uint sql_errno, const char *err_msg);

public:
  Protocol() {}
  Protocol(THD *thd_arg) { init(thd_arg); }
  virtual ~Protocol() {}
  void init(THD* thd_arg);

  enum { SEND_NUM_ROWS= 1, SEND_DEFAULTS= 2, SEND_EOF= 4 };
  virtual bool send_result_set_metadata(List<Item> *list, uint flags);
  bool send_result_set_row(List<Item> *row_items);

  bool store(I_List<i_string> *str_list);
  bool store(const char *from, CHARSET_INFO *cs);
  String *storage_packet() { return packet; }
  inline void free() { packet->free(); }
  virtual bool write();
  inline  bool store(int from)
  { return store_long((longlong) from); }
  inline  bool store(uint32 from)
  { return store_long((longlong) from); }
  inline  bool store(longlong from)
  { return store_longlong((longlong) from, 0); }
  inline  bool store(ulonglong from)
  { return store_longlong((longlong) from, 1); }
  inline bool store(String *str)
  { return store((char*) str->ptr(), str->length(), str->charset()); }

  virtual bool prepare_for_send(uint num_columns)
  {
    field_count= num_columns;
    return 0;
  }
  virtual bool flush();
  virtual void end_partial_result_set(THD *thd);
  virtual void prepare_for_resend()=0;

  virtual bool store_null()=0;
  virtual bool store_tiny(longlong from)=0;
  virtual bool store_short(longlong from)=0;
  virtual bool store_long(longlong from)=0;
  virtual bool store_longlong(longlong from, bool unsigned_flag)=0;
  virtual bool store_decimal(const my_decimal *)=0;
  virtual bool store(const char *from, size_t length, CHARSET_INFO *cs)=0;
  virtual bool store(const char *from, size_t length, 
  		     CHARSET_INFO *fromcs, CHARSET_INFO *tocs)=0;
  virtual bool store(float from, uint32 decimals, String *buffer)=0;
  virtual bool store(double from, uint32 decimals, String *buffer)=0;
  virtual bool store(MYSQL_TIME *time)=0;
  virtual bool store_date(MYSQL_TIME *time)=0;
  virtual bool store_time(MYSQL_TIME *time)=0;
  virtual bool store(Field *field)=0;

  virtual bool send_out_parameters(List<Item_param> *sp_params)=0;
#ifdef EMBEDDED_LIBRARY
  int begin_dataset();
  virtual void remove_last_row() {}
#else
  void remove_last_row() {}
#endif
  enum enum_protocol_type
  {
    PROTOCOL_TEXT= 0, PROTOCOL_BINARY= 1
    /*
      before adding here or change the values, consider that it is cast to a
      bit in sql_cache.cc.
    */
  };
  virtual enum enum_protocol_type type()= 0;

  void end_statement();
};


/** Class used for the old (MySQL 4.0 protocol). */

class Protocol_text :public Protocol
{
public:
  Protocol_text() {}
  Protocol_text(THD *thd_arg) :Protocol(thd_arg) {}
  virtual void prepare_for_resend();
  virtual bool store_null();
  virtual bool store_tiny(longlong from);
  virtual bool store_short(longlong from);
  virtual bool store_long(longlong from);
  virtual bool store_longlong(longlong from, bool unsigned_flag);
  virtual bool store_decimal(const my_decimal *);
  virtual bool store(const char *from, size_t length, CHARSET_INFO *cs);
  virtual bool store(const char *from, size_t length,
  		     CHARSET_INFO *fromcs, CHARSET_INFO *tocs);
  virtual bool store(MYSQL_TIME *time);
  virtual bool store_date(MYSQL_TIME *time);
  virtual bool store_time(MYSQL_TIME *time);
  virtual bool store(float nr, uint32 decimals, String *buffer);
  virtual bool store(double from, uint32 decimals, String *buffer);
  virtual bool store(Field *field);

  virtual bool send_out_parameters(List<Item_param> *sp_params);
#ifdef EMBEDDED_LIBRARY
  void remove_last_row();
#endif
  virtual enum enum_protocol_type type() { return PROTOCOL_TEXT; };
};


class Protocol_binary :public Protocol
{
private:
  uint bit_fields;
public:
  Protocol_binary() {}
  Protocol_binary(THD *thd_arg) :Protocol(thd_arg) {}
  virtual bool prepare_for_send(uint num_columns);
  virtual void prepare_for_resend();
#ifdef EMBEDDED_LIBRARY
  virtual bool write();
  bool net_store_data(const uchar *from, size_t length);
#endif
  virtual bool store_null();
  virtual bool store_tiny(longlong from);
  virtual bool store_short(longlong from);
  virtual bool store_long(longlong from);
  virtual bool store_longlong(longlong from, bool unsigned_flag);
  virtual bool store_decimal(const my_decimal *);
  virtual bool store(const char *from, size_t length, CHARSET_INFO *cs);
  virtual bool store(const char *from, size_t length,
  		     CHARSET_INFO *fromcs, CHARSET_INFO *tocs);
  virtual bool store(MYSQL_TIME *time);
  virtual bool store_date(MYSQL_TIME *time);
  virtual bool store_time(MYSQL_TIME *time);
  virtual bool store(float nr, uint32 decimals, String *buffer);
  virtual bool store(double from, uint32 decimals, String *buffer);
  virtual bool store(Field *field);

  virtual bool send_out_parameters(List<Item_param> *sp_params);

  virtual enum enum_protocol_type type() { return PROTOCOL_BINARY; };
};

void send_warning(THD *thd, uint sql_errno, const char *err=0);
void net_send_error(THD *thd, uint sql_errno=0, const char *err=0);
bool send_old_password_request(THD *thd);
uchar *net_store_data(uchar *to,const uchar *from, size_t length);
uchar *net_store_data(uchar *to,int32 from);
uchar *net_store_data(uchar *to,longlong from);

///////////////////////////////////////////////////////////////////////////

/**
  Ed_column -- a class representing a column data in a row. Used with
  Ed_row and Protocol_local.
*/
class Ed_column : public LEX_STRING, public Sql_alloc
{
public:
  inline Ed_column()
  {
    str= NULL;
    length= 0;
  }

  inline void set_data(MEM_ROOT *mem_root, const void *p_str, int p_length)
  {
    str= (char *) memdup_root(mem_root, p_str, p_length);
    length= p_length;
  }
};

///////////////////////////////////////////////////////////////////////////

/**
  Ed_result_set_metadata -- a class representing result set metadata. Used
  with Ed_result_set and Protocol_local.
*/

class Ed_result_set_metadata : public Sql_alloc
{
public:
  static Ed_result_set_metadata *create(MEM_ROOT *mem_root,
                                        List<Item> *col_metadata);

public:
  inline int get_num_columns() const { return m_num_columns; }

  inline const Send_field *get_column(int idx) const
  { return &m_metadata[idx]; }

private:
  inline Ed_result_set_metadata() :
    m_num_columns(0),
    m_metadata(NULL)
  { }

private:
  bool init(MEM_ROOT *mem_root, List<Item> *col_metadata);

private:
  int m_num_columns;
  Send_field *m_metadata;
};

///////////////////////////////////////////////////////////////////////////

/**
  Ed_row -- a class representing a row in a result set. Used with
  Ed_result_set and Protocol_local.
*/
class Ed_row : public Sql_alloc
{
public:
  static Ed_row *create(MEM_ROOT *mem_root,
                        const Ed_result_set_metadata *metadata);

public:
  bool add_null();
  bool add_column(const void *data_ptr, int data_length);

public:
  inline const Ed_result_set_metadata *get_metadata() const
  { return m_metadata; }

  inline const Ed_column *get_column(int idx) const
  { return &m_columns[idx]; }

  inline const Ed_column *operator [](int idx) const
  { return get_column(idx); }

  inline int get_current_column_index() const
  { return m_current_column_index; }

private:
  inline Ed_row(MEM_ROOT *mem_root,
                const Ed_result_set_metadata *metadata) :
    m_mem_root(mem_root),
    m_metadata(metadata),
    m_current_column_index(0)
  { }

  bool init();

private:
  MEM_ROOT *m_mem_root;
  const Ed_result_set_metadata *m_metadata;
  Ed_column *m_columns;
  int m_current_column_index;
};

///////////////////////////////////////////////////////////////////////////

/**
  Ed_result_set -- a class representing one result set. Used with Ed_result
  and Protocol_local.
*/
class Ed_result_set : public Sql_alloc
{
public:
  static Ed_result_set *create(MEM_ROOT *mem_root,
                               List<Item> *col_metadata);

private:
  inline Ed_result_set(MEM_ROOT *mem_root) :
    m_mem_root(mem_root),
    m_metadata(NULL),
    m_current_row(NULL)
  { }

private:
  bool init(List<Item> *col_metadata);

public:
  inline const Ed_result_set_metadata *get_metadata() const
  { return m_metadata; }

  inline List<Ed_row> *data()
  { return &m_data; }

  inline Ed_row *get_cur_row()
  { return m_current_row; }

  Ed_row *add_row();

private:
  MEM_ROOT *m_mem_root;

  Ed_result_set_metadata *m_metadata;
  List<Ed_row> m_data;

  Ed_row *m_current_row;
};

///////////////////////////////////////////////////////////////////////////

/*
  Ed_result -- a class representing results for an SQL statement execution.
  Used with Protocol_local.
*/
class Ed_result : public List<Ed_result_set>
{
public:
  Ed_result(MEM_ROOT *mem_root);

  inline ~Ed_result()
  { }

public:
  inline Ed_result_set *get_cur_result_set()
  { return m_current_result_set; }

  bool add_result_set(List<Item> *col_metadata);

public:
  void send_ok(THD *thd, uint server_status, uint statement_warn_count,
               ha_rows affected_rows, ulonglong last_insert_id,
               const char *message);

  void send_eof(THD *thd, uint server_status, uint statement_warn_count);

  void send_error(THD *thd, uint sql_errno, const char *err_msg);

  void begin_statement(THD *thd);
  void end_statement(THD *thd);

public:
  inline uint get_status() const { return m_status; }
  inline uint get_server_status() const { return m_server_status; }
  inline ha_rows get_affected_rows() const { return m_affected_rows; }
  inline ulonglong get_last_insert_id() const { return m_last_insert_id; }
  inline uint get_sql_errno() const { return m_sql_errno; }
  inline const char *get_message() const { return m_message; }

  inline uint get_statement_warn_count() const
  { return m_warning_info.statement_warn_count(); }

  inline List<MYSQL_ERROR> &get_warnings()
  { return m_warning_info.warn_list(); }

private:
  MEM_ROOT *m_mem_root;
  Ed_result_set *m_current_result_set;

private:
  uint m_status;
  uint m_server_status;
  ha_rows m_affected_rows;
  ulonglong m_last_insert_id;
  uint m_sql_errno;
  char m_message[MYSQL_ERRMSG_SIZE];

  Warning_info m_warning_info;
  Warning_info *m_warning_info_saved;
};

///////////////////////////////////////////////////////////////////////////

/**
  Protocol_local: a protocol for retrieving result sets from the server
  locally.
*/
class Protocol_local :public Protocol
{
public:
  inline Protocol_local(THD *thd, Ed_result *result)
    : Protocol(thd),
      m_result(result)
  { }

public:
  virtual void prepare_for_resend();
  virtual bool write();
  virtual bool store_null();
  virtual bool store_tiny(longlong from);
  virtual bool store_short(longlong from);
  virtual bool store_long(longlong from);
  virtual bool store_longlong(longlong from, bool unsigned_flag);
  virtual bool store_decimal(const my_decimal *);
  virtual bool store(const char *from, size_t length, CHARSET_INFO *cs);
  virtual bool store(const char *from, size_t length,
                     CHARSET_INFO *fromcs, CHARSET_INFO *tocs);
  virtual bool store(MYSQL_TIME *time);
  virtual bool store_date(MYSQL_TIME *time);
  virtual bool store_time(MYSQL_TIME *time);
  virtual bool store(float value, uint32 decimals, String *buffer);
  virtual bool store(double value, uint32 decimals, String *buffer);
  virtual bool store(Field *field);

  virtual bool send_result_set_metadata(List<Item> *list, uint flags);
  virtual bool send_out_parameters(List<Item_param> *sp_params);
#ifdef EMBEDDED_LIBRARY
  void remove_last_row();
#endif
  virtual enum enum_protocol_type type() { return PROTOCOL_TEXT; /* FIXME */ };

protected:
  virtual void send_ok(uint server_status, uint statement_warn_count,
                       ha_rows affected_rows, ulonglong last_insert_id,
                       const char *message);

  virtual void send_eof(uint server_status, uint statement_warn_count);

  virtual void send_error(uint sql_errno, const char *err_msg);

private:
  bool store_string(const char *str, int length,
                    CHARSET_INFO *src_cs, CHARSET_INFO *dst_cs);

private:
  Ed_result *m_result;
};
