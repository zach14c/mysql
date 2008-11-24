#ifndef SI_OBJECTS_H_
#define SI_OBJECTS_H_

/**
   @file

   This file defines the API for the following object services:
     - serialize database objects into a string;
     - materialize (deserialize) object from a string;
     - enumerating objects;
     - finding dependencies for objects;
     - executor for SQL statements;
     - wrappers for controlling the DDL Blocker;
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
    Serialize an object to a string. The serialization string is opaque
    object for the client.

    @return Error status.
  */
  virtual bool serialize(THD *thd, String *serialization) = 0;

  /**
    Materialize an object in the database.

    @return Error status.
  */
  virtual bool execute(THD *thd) = 0;

public:
  virtual ~Obj()
  { }

protected:
  /**
    Read the object state from a given buffer and restores object state to
    the point, where it can be executed.

    @param[in] serialization_version The version of the serialization format.
    @param[in] serialization         Buffer contained serialized object.

    @return error status.
      @retval FALSE on success.
      @retval TRUE on error.
  */
  virtual bool materialize(uint serialization_version,
                           const String *serialization) = 0;

private:
  friend Obj *materialize_database(const String *,
                                   uint,
                                   const String *);

  friend Obj *materialize_table(const String *,
                                const String *,
                                uint,
                                const String *);

  friend Obj *materialize_view(const String *,
                               const String *,
                               uint,
                               const String *);

  friend Obj *materialize_trigger(const String *,
                                  const String *,
                                  uint,
                                  const String *);

  friend Obj *materialize_stored_procedure(const String *,
                                           const String *,
                                           uint,
                                           const String *);

  friend Obj *materialize_stored_function(const String *,
                                          const String *,
                                          uint,
                                          const String *);

  friend Obj *materialize_event(const String *,
                                const String *,
                                uint,
                                const String *);

  friend Obj *materialize_tablespace(const String *,
                                     uint,
                                     const String *);

  friend Obj *materialize_db_grant(const String *,
                                   const String *,
                                   uint,
                                   const String *);

};

///////////////////////////////////////////////////////////////////////////

/**
  Obj_iterator is a basic interface to enumerate the objects.
*/
class Obj_iterator
{
public:
  Obj_iterator()
  { }

  /**
    This operation returns a pointer to the next object in an enumeration.
    It returns NULL if there is no more objects.

    The client is responsible to destroy the returned object.

    @return a pointer to the object
      @retval NULL if there is no more objects in an enumeration.
  */
  virtual Obj *next() = 0;

public:
  virtual ~Obj_iterator()
  { }
};

///////////////////////////////////////////////////////////////////////////

// The functions in this section are intended to construct an instance of
// Obj class for any particular database object. These functions do not
// interact with the server to validate requested names. So, it is possible
// to construct instances for non-existing objects.
//
// The client is responsible for destroying the returned object.

/**
  Construct an instance of Obj representing a database.

  No actual actions are performed in the server. An object can be created
  even for invalid database name or for non-existing database.

  The client is responsible to destroy the created object.

  @param[in] db_name Database name.

  @return a pointer to an instance of Obj representing given database.
*/
Obj *get_database(const String *db_name);

///////////////////////////////////////////////////////////////////////////

// The functions in this section provides a way to iterator over all
// objects in the server or in the particular database.
//
// The client is responsible for destroying the returned iterator.

///////////////////////////////////////////////////////////////////////////

/**
  Create an iterator over all databases in the server.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
*/
Obj_iterator *get_databases(THD *thd);

/**
  Create an iterator over all tables in the particular database.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_tables(THD *thd, const String *db_name);

/**
  Create an iterator over all views in the particular database.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_views(THD *thd, const String *db_name);

/**
  Create an iterator over all triggers in the particular database.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_triggers(THD *thd, const String *db_name);

/**
  Create an iterator over all stored procedures in the particular database.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_stored_procedures(THD *thd, const String *db_name);

/**
  Create an iterator over all stored functions in the particular database.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_stored_functions(THD *thd, const String *db_name);

/**
  Create an iterator over all events in the particular database.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator *get_db_events(THD *thd, const String *db_name);

/*
  Creates a high-level iterator that iterates over database-, table-,
  routine-, and column-level privileges which shall permit a single
  iterator from the si_objects to retrieve all of the privileges for
  a given database.
*/
Obj_iterator *get_all_db_grants(THD *thd, const String *db_name);

///////////////////////////////////////////////////////////////////////////

// The functions are intended to enumerate dependent objects.
//
// The client is responsible for destroying the returned iterator.

///////////////////////////////////////////////////////////////////////////

/**
  Create an iterator overl all base tables in the particular view.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator* get_view_base_tables(THD *thd,
                                   const String *db_name,
                                   const String *view_name);

/**
  Create an iterator overl all base tables in the particular view.

  The client is responsible to destroy the returned iterator.

  @return a pointer to an iterator object.
    @retval NULL in case of error.
*/
Obj_iterator* get_view_base_views(THD *thd,
                                  const String *db_name,
                                  const String *view_name);

///////////////////////////////////////////////////////////////////////////

// The functions in this section provides a way to materialize objects from
// the serialized form.
//
// The client is responsible for destroying the returned iterator.

///////////////////////////////////////////////////////////////////////////

Obj *materialize_database(const String *db_name,
                          uint serialization_version,
                          const String *serialization);

Obj *materialize_table(const String *db_name,
                       const String *table_name,
                       uint serialization_version,
                       const String *serialization);

Obj *materialize_view(const String *db_name,
                      const String *view_name,
                      uint serialization_version,
                      const String *serialization);

Obj *materialize_trigger(const String *db_name,
                         const String *trigger_name,
                         uint serialization_version,
                         const String *serialization);

Obj *materialize_stored_procedure(const String *db_name,
                                  const String *stored_proc_name,
                                  uint serialization_version,
                                  const String *serialization);

Obj *materialize_stored_function(const String *db_name,
                                 const String *stored_func_name,
                                 uint serialization_version,
                                 const String *serialization);

Obj *materialize_event(const String *db_name,
                       const String *event_name,
                       uint serialization_version,
                       const String *serialization);

Obj *materialize_tablespace(const String *ts_name,
                            uint serialization_version,
                            const String *serialization);

Obj *materialize_db_grant(const String *db_name,
                          const String *name,
                          uint serialization_version,
                          const String *serialization);

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
