#ifndef RESTORE_INFO_H_
#define RESTORE_INFO_H_

#include <backup/image_info.h>
#include <backup/stream_services.h>

class Backup_restore_ctx;
class Restore_info;

namespace backup {

class Logger;
class Input_stream;

int restore_table_data(THD*, Restore_info&, 
                       backup::Input_stream&);

} // backup namespace


/**
  Specialization of @c Image_info which is in restore operation.

  An instance of this class is created by 
  @c Backup_restore_ctx::prepare_for_restore() method, which reads the 
  catalogue from a backup image.
  
  Currently it is not possible to select which objects will be restored. This
  class can only be used to examine what is going to be restored.
 */

class Restore_info: public backup::Image_info
{
 public:

  Backup_restore_ctx &m_ctx;

  Restore_info(Backup_restore_ctx&);
  ~Restore_info();

  bool is_valid() const;

 private:

  friend int backup::restore_table_data(THD*, Restore_info&, 
                                        backup::Input_stream&);
  friend int ::bcat_add_item(st_bstream_image_header*,
                             struct st_bstream_item_info*);
};

inline
Restore_info::Restore_info(Backup_restore_ctx &ctx)
  :m_ctx(ctx)
{}

inline
Restore_info::~Restore_info()
{
  /*
    Delete Snapshot_info instances - they are created in bcat_reset(). 
   */
  for (ushort n=0; n < snap_count(); ++n)
    delete m_snap[n];
}

inline
bool Restore_info::is_valid() const
{
  return TRUE; 
}

#endif /*RESTORE_INFO_H_*/
