#ifndef SI_OBJECTS_H_
#define SI_OBJECTS_H_

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

/**
  @file

  An object in this file refers to SQL language database objects
  such as tables, views, stored programs, events or users,
  databases and tablespaces.
  This file defines an API for the following object services:
    - serialize object definition (metadata) into a string,
    - materialize (de-serialize) object from a string,
    - enumerate all objects of a database,
    - find dependencies between objects, such as underlying tables of
    views, tablespace of a table.

  Additionally, the interface provides two helper services
  for backup:
    - execute an arbitrary SQL statement
    - lock and unlock all metadata, so called "DDL blocker"
*/

namespace obs {

///////////////////////////////////////////////////////////////////////////

/**
  Obj defines the basic set of operations for each database object.
*/
class Obj
{
public:
  /**
    Return the name of the object.

    @return object name.
  */
  virtual const String *get_name() const = 0;

  /**
    Return the database name of the object.

    @note this is a subject to remove.
  */
  virtual const String *get_db_name() const = 0;

public:
  /**
    Serialize an object to an image. The serialization image is opaque
    object for the client.

    @param[in]  thd     Thread context.
    @param[out] image   Serialization image.

    @return Error status.
  */
  virtual bool serialize(THD *thd, String *image) = 0;

  /**
    Create an object persistently in the database.

    @param[in]  thd     Thread context.

    @return Error status.
  */
  virtual bool create(THD *thd) = 0;

  /**
    Drop an object in the database.

    @param[in]  thd     Thread context.

    @return Error status.
  */
  virtual bool drop(THD *thd) = 0;

public:
  virtual ~Obj()
  { }
};

///////////////////////////////////////////////////////////////////////////

/**
  Obj_iterator is a basic interface to enumerate objects.
*/
class Obj_iterator
{
public:
  Obj_iterator()
  { }

  /**
    This operation returns a pointer to the next object in an enumeration.
    It returns NULL if there is no more objects.
    Results of an attempt to continue iteration when there is no
    more objects are undefined.

    The client is responsible for destruction of the returned object.

    @return a pointer to the object
      @retval NULL if there is no more objects in an enumeration.
  */
  virtual Obj *next() = 0;

public:
  virtual ~Obj_iterator()
  { }
};

///////////////////////////////////////////////////////////////////////////

// Functions in this section are intended to construct an instance of Obj
// class for any particular database object. These functions do not interact
// with the server to validate requested names. So, it is possible to
// construct instances for non-existing objects.
//
// The client is responsible for destroying the returned object.

/**
  Construct an instance of Obj representing a database.

  No actions are performed in the server. An object can be created
  even for an invalid database name or for a non-existing database.

  The client is responsible for destruction of the created object.

  @param[in] db_name Database name.

  @return a pointer to an instance of Obj representing the given database.
*/
Obj *get_database_stub(const String *db_name);

///////////////////////////////////////////////////////////////////////////

// Functions in this section provide a way to iterate over all objects in
// the server or in a particular database.
//
// The client is responsible for destruction of the returned iterator.

///////////////////////////////////////////////////////////////////////////

/**
  Create an iterator over all databases in the server.
  Includes system databases, such as "mysql" and "information_schema".

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
*/
Obj_iterator *get_databases(THD *thd);

/**
  Create an iterator over all base tables in a particular database.
  Temporary tables are not included.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_tables(THD *thd, const String *db_name);

/**
  Create an iterator over all views in a particular database.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_views(THD *thd, const String *db_name);

/**
  Create an iterator over all triggers in a particular database.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_triggers(THD *thd, const String *db_name);

/**
  Create an iterator over all stored procedures in a particular database.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_stored_procedures(THD *thd, const String *db_name);

/**
  Create an iterator over all stored functions in a particular database.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_stored_functions(THD *thd, const String *db_name);

/**
  Create an iterator over all events in a particular database.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_events(THD *thd, const String *db_name);

/*
  Creates a high-level iterator that iterates over database-, table-,
  routine-, and column-level- privileges. This allows to retrieve all
  privileges of a given database using a single iterator.
*/
Obj_iterator *get_all_db_grants(THD *thd, const String *db_name);

///////////////////////////////////////////////////////////////////////////

// The functions are intended to enumerate dependent objects.
//
// The client is responsible for destruction of the returned iterator.

///////////////////////////////////////////////////////////////////////////

/**
  Create an iterator over all base tables of a view.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator* get_view_base_tables(THD *thd,
                                   const String *db_name,
                                   const String *view_name);

/**
  Create an iterator over all base views of a particular view.

  The client is responsible for destruction of the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator* get_view_base_views(THD *thd,
                                  const String *db_name,
                                  const String *view_name);

///////////////////////////////////////////////////////////////////////////

// Functions in this section provide a way to materialize objects from their
// serialized form (serialization image). In order to do that, the client
// creates an object handle, by means of one of the functions below, and
// then calls "create()" method on it.
//
// The client is responsible for destruction of the returned object handle.

///////////////////////////////////////////////////////////////////////////

Obj *get_database(const String *db_name,
                  uint image_version,
                  const String *image);

Obj *get_table(const String *db_name,
               const String *table_name,
               uint image_version,
               const String *image);

Obj *get_view(const String *db_name,
              const String *view_name,
              uint image_version,
              const String *image);

Obj *get_trigger(const String *db_name,
                 const String *trigger_name,
                 uint image_version,
                 const String *image);

Obj *get_stored_procedure(const String *db_name,
                          const String *stored_proc_name,
                          uint image_version,
                          const String *image);

Obj *get_stored_function(const String *db_name,
                         const String *stored_func_name,
                         uint image_version,
                         const String *image);

Obj *get_event(const String *db_name,
               const String *event_name,
               uint image_version,
               const String *image);

Obj *get_tablespace(const String *ts_name,
                    uint image_version,
                    const String *image);

Obj *get_db_grant(const String *db_name,
                  const String *name,
                  uint image_version,
                  const String *image);

///////////////////////////////////////////////////////////////////////////

/**
  Check if the given database name is reserved for internal use.

  @return
    @retval TRUE if the given database name is reserved for internal use.
    @retval FALSE otherwise.
*/
bool is_internal_db_name(const String *db_name);

/**
  Check if the given directory actually exists.

  @return Error status.
    @retval FALSE on success (the database exists and accessible).
    @retval TRUE on error (the database either not exists, or not accessible).
*/
bool check_db_existence(THD *thd, const String *db_name);

/**
  Check if the user is defined on the system.

  @return
    @retval TRUE if user is defined
    @retval FALSE otherwise
*/
bool check_user_existence(THD *thd, const Obj *obj);

/**
  Return user name of materialized grant object.
*/
const String *grant_get_user_name(const Obj *obj);

/**
  Return grant info of materialized grant object.
*/
const String *grant_get_grant_info(const Obj *obj);

/**
  Determine if the tablespace referenced by name exists on the system.

  @return a Tablespace_obj if it exists or NULL if it doesn't.
*/
Obj *find_tablespace(THD *thd, const String *ts_name);

/**
  This method returns a @c Tablespace_obj object if the table has a
  tablespace.
*/
Obj *find_tablespace_for_table(THD *thd,
                               const String *db_name,
                               const String *table_name);

/**
  This method determines if a materialized tablespace exists on the system.
  This compares the name and all saved attributes of the tablespace. A
  FALSE return would mean either the tablespace does not exist or the
  tablespace attributes are different.
*/
bool compare_tablespace_attributes(Obj *ts1, Obj *ts2);

///////////////////////////////////////////////////////////////////////////

//
// DDL blocker methods.
//

///////////////////////////////////////////////////////////////////////////

/**
  Turn on the ddl blocker.

  This method is used to start the ddl blocker blocking DDL commands.

  @param[in] thd  Thread context.

  @return error status.
    @retval FALSE on success.
    @retval TRUE on error.
*/
bool ddl_blocker_enable(THD *thd);

/**
  Turn off the ddl blocker.

  This method is used to stop the ddl blocker from blocking DDL commands.
*/
void ddl_blocker_disable();

/**
  Turn on the ddl blocker exception

  This method is used to allow the exception allowing a restore operation to
  perform DDL operations while the ddl blocker blocking DDL commands.

  @param[in] thd  Thread context.
*/
void ddl_blocker_exception_on(THD *thd);

/**
  Turn off the ddl blocker exception.

  This method is used to suspend the exception allowing a restore operation to
  perform DDL operations while the ddl blocker blocking DDL commands.

  @param[in] thd  Thread context.
*/
void ddl_blocker_exception_off(THD *thd);

/*
  The following class is used to manage name locks on a list of tables.

  This class uses a list of type List<Obj> to establish the table list
  that will be used to manage locks on the tables.
*/
class Name_locker
{
public:
  Name_locker(THD *thd) { m_thd= thd; }
  ~Name_locker()
  {
    free_table_list(m_table_list);
    m_table_list= NULL;
  }

  /*
    Gets name locks on table list.
  */
  int get_name_locks(List<Obj> *tables, thr_lock_type lock);

  /*
    Releases name locks on table list.
  */
  int release_name_locks();

private:
  TABLE_LIST *m_table_list; ///< The list of tables to obtain locks on.
  THD *m_thd;               ///< Thread context.

  /*
    Builds a table list from the list of objects passed to constructor.
  */
  TABLE_LIST *build_table_list(List<Obj> *tables, thr_lock_type lock);
  void free_table_list(TABLE_LIST*);
};

} // obs namespace

#endif // SI_OBJECTS_H_
