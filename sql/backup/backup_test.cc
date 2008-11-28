/**
  @file

  Implementation of the backup test function.

  @todo Implement code to test service interface(s).
 */

#include "../mysql_priv.h"
#include "si_objects.h"
#include "backup_aux.h"

using namespace obs;

/**
   Call backup kernel API to execute backup related SQL statement.

   @param[in] thd  current thread
   @param[in] lex  results of parsing the statement.
  */
int execute_backup_test_command(THD *thd, List<LEX_STRING> *db_list)
{
  int res= 0;

  DBUG_ENTER("execute_backup_test_command");
  DBUG_ASSERT(thd);

  Protocol *protocol= thd->protocol;    // client comms
  List<Item> field_list;                // list of fields to send
  String     op_str;                    // operations string
  String str;

  {
    String tmp_db_name("qqq", 3, system_charset_info);
    DBUG_ASSERT(obs::check_db_existence(&tmp_db_name));
  }

  /*
    Send field list.
  */
  field_list.push_back(new Item_empty_string("db", 2));
  field_list.push_back(new Item_empty_string("name", 5));
  field_list.push_back(new Item_empty_string("type", 4));
  field_list.push_back(new Item_empty_string("serialization", 13));
  protocol->send_result_set_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);

  //obs::Obj_iterator *it= obs::get_databases(thd);
  List_iterator<LEX_STRING> it(*db_list);
  
  //if (it)
  {
    obs::Obj *db;

    //while ((db= it->next()))
    LEX_STRING *dbname;
    while ((dbname= it++))
    {
      String dir;
      dir.copy(dbname->str, dbname->length, system_charset_info);
      db= get_database(&dir);

      if (is_internal_db_name(db->get_db_name()))
          continue;

      DBUG_ASSERT(!obs::check_db_existence(db->get_db_name()));

      //
      // List tables..
      //

      obs::Obj_iterator *tit= obs::get_db_tables(thd, db->get_name());

      if (tit)
      {
        obs::Obj *table;

        while ((table= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(table->get_name()));
          protocol->store("TABLE", 5, system_charset_info);
          table->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete table;
        }
      }

      //
      // List views.
      //

      tit= obs::get_db_views(thd, db->get_name());

      if (tit)
      {
        obs::Obj *table;

        while ((table= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(table->get_name()));
          protocol->store("VIEW", 5, system_charset_info);
          table->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete table;
        }

        delete tit;
      }

      //
      // List triggers.
      //

      tit= obs::get_db_triggers(thd, db->get_name());

      if (tit)
      {
        obs::Obj *trigger;

        while ((trigger= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(trigger->get_name()));
          protocol->store(C_STRING_WITH_LEN("TRIGGER"), system_charset_info);
          trigger->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete trigger;
        }

        delete tit;
      }

      //
      // List stored procedures.
      //

      tit= obs::get_db_stored_procedures(thd, db->get_name());

      if (tit)
      {
        obs::Obj *sp;

        while ((sp= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(sp->get_name()));
          protocol->store(C_STRING_WITH_LEN("STORED PROCEDURE"),
                          system_charset_info);
          sp->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete sp;
        }

        delete tit;
      }

      //
      // List stored functions.
      //

      tit= obs::get_db_stored_functions(thd, db->get_name());

      if (tit)
      {
        obs::Obj *sf;

        while ((sf= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(sf->get_name()));
          protocol->store(C_STRING_WITH_LEN("STORED FUNCTION"),
                          system_charset_info);
          sf->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete sf;
        }

        delete tit;
      }

      //
      // List events.
      //

      tit= obs::get_db_events(thd, db->get_name());

      if (tit)
      {
        obs::Obj *event;

        while ((event= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(event->get_name()));
          protocol->store(C_STRING_WITH_LEN("EVENT"),
                          system_charset_info);
          event->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete event;
        }
      }

      //
      // List db grants.
      //
      tit= obs::get_all_db_grants(thd, db->get_name());

      if (tit)
      {
        obs::Obj *grant;

        while ((grant= tit->next()))
        {
          String serial;
          serial.length(0);
          protocol->prepare_for_resend();
          protocol->store(const_cast<String*>(db->get_name()));
          protocol->store(const_cast<String*>(grant->get_name()));
          String user;
          String host;
          String *user_host= (String *)grant->get_name();
          check_user_existence(thd, user_host);
          protocol->store(C_STRING_WITH_LEN("GRANT"),
                          system_charset_info);
          grant->serialize(thd, &serial);
          protocol->store(&serial);
          protocol->write();

          delete grant;
        }

      delete tit;
      }
    }
  }
  thd->main_da.reset_diagnostics_area();
  my_eof(thd);
  DBUG_RETURN(res);
}

