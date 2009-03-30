/**
  @file

  Header file for Backup Metadata Lock.
 */
#include "mysql_priv.h"

/**
   @class BML_class
 
   @brief Implements a simple Backup Metadata Lock (BML) mechanism.
 
   The BML_class is a singleton class designed to allow blocking statements 
   changing metadata which should be constant during backup/restore operation.
   Only one thread can hold the lock but there is no restriction on number
   of blocked statements which can run in parallel. 

   If a thread has acquired BML and another thread attempts to activate it,
   the second thread will wait until the first one is complete.
 
   Checking for Block
   Any statement that needs to be blocked by BML should call @c bml_enter() at
   the beginning of its execution. This method will return when BML is not active
   or wait until it becomes inactive. Once the method returns, the statement
   which called the method is registered and while it is running, it will be not
   possible to activate BML. When the statement is complete, you must unregister
   it by calling bml_leave(). All this is done inside the parser
   (@c mysql_execute_command()).

   Blocking Statements
   To prevent metadata changes, call bml_get(). This activates the lock and
   prevents any statements which use bml_enter() from executing. To remove the
   lock bml_release().

   Singleton Methods
   The creation of the singleton is accomplished using 
   get_BML_class_instance(). This method is called from mysqld.cc
   and creates and initializes all of the private mutex, condition, and
   controlling variables. The method destroy_BML_class_instance()
   destroys the mutex and condition variables. 

   Calling the Singleton
   To call the singleton class, you must declare an external variable
   to the global variable BML_instance as shown below.

   @c extern BML_class *BML_instance;

   Calling methods on the singleton is accomplished using the BML_instance
   variable such as: @c BML_instance->bml_get().

   @note: This class is currently only used in MySQL backup. If you would
          like to use it elsewhere and have questions, please contact
          Chuck Bell (cbell@mysql.com) for more details and how to setup
          a test case to test the BML mechanism for your use.
  */
class BML_class
{
  public:

    /*
      Singleton class
    */
    static BML_class *get_BML_class_instance();
    static void destroy_BML_class_instance();

    /*
      Check to see if BML is active. If so, wait until it is deactivated.
      When BML is not active, register the operation with BML.
    */
    my_bool bml_enter(THD *thd);

    /*
      Unregister operation which checked for BML with bml_enter(). 
    */
    void bml_leave();

    /*
      This method is used to activate BML. It waits for any operations which
      registered with bml_enter() to unregister using bml_leave().
      The method also prevents any other thread from activating the lock until
      bml_release() is called.
    */
    my_bool bml_get(THD *thd);

    /*
      This method is used to deactivate BML. All operations which are waiting
      in bml_enter() call (if any) will be allowed to continue.
    */
    void bml_release();

  private:

    BML_class();
    ~BML_class();

    /*
      Registers operation which obeys BML.
    */
    void do_enter();

    /*
      These variables are used to implement the Backup Metadata Lock.
    */

    /// Mutex for protecting BML_registered counter.
    pthread_mutex_t THR_LOCK_BML;
    /// Mutex for proteting BML_active falg.
    pthread_mutex_t THR_LOCK_BML_active;
    /// Mutex for serializing BML usage. 
    pthread_mutex_t THR_LOCK_BML_get;
    /// Signals deactivation of BML for statements waiting in bml_enter(). 
    pthread_cond_t COND_BML;
    /// Signals deactivation of BML for threads waiting in bml_get().
    pthread_cond_t COND_BML_release;
    /// Signals that BML_reagistered count dropped to 0.
    pthread_cond_t COND_BML_registered;

    my_bool BML_active;           ///< Is BML activated.
    int BML_registered;           ///< Number of statements registered with BML.
    static BML_class *m_instance; ///< instance var for singleton 
};
