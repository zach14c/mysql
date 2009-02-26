/* Copyright (C) 2008 Sun Microsystems Inc.

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

/*
  MySQL Backup Image Stream reading
*/

/*
  Include from mysys functions.
*/
#include "my_global.h"
#include "my_sys.h"

/*
  Include from the low-level stream access functions.
*/
#include "stream_v1.h"
#include "stream_v1_services.h"

#ifdef HAVE_COMPRESS
/* zlib functions, required for struct st_stream and in backup_stream.c */
#include "zlib.h"
#define ZBUF_SIZE 65536 /* compression I/O buffer size */
#endif


/*
  This needs to be provided by the application.
*/
C_MODE_START /* It shall shall also be providable by C++. */
void errm(const char *format, ...) ATTRIBUTE_FORMAT(printf, 1, 2);
C_MODE_END   /* It shall shall also be providable by C++. */


/*
  =====================
  Catalog declarations.
  =====================
*/

/*
  The below declarations in C language try to borrow kind of inheritance
  from C++. By having a sub-structure as the first element of a new
  structure one can cast to the new structure when a reference to the
  sub-structure is given.

  In the dynamic arrays, we store pointers to catalog items only.
  Some items reference others. These pointers would become invalid
  when the array is reallocated on insert of a new element.
*/

/*
  Catalog.

  The dynamic arrays hold pointers to items of the following types:

  struct st_backup_global               cat_charsets
  struct st_backup_global               cat_users
  struct st_backup_global               cat_tablespaces
  struct st_backup_database             cat_databases
  struct st_backup_snapshot             cat_snapshots
  struct st_bstream_item_info           cat_image_ordered_items
  struct st_bstream_item_info           cat_image_ordered_metadata

  note: cat_header must be first element in st_backup_catalog.
*/
struct st_backup_catalog
{
  struct st_bstream_image_header        cat_header;     /* must be 1st */
  const char                            *cat_zalgo;
  const char                            *cat_image_path;
  my_off_t                              cat_image_size;
  DYNAMIC_ARRAY                         cat_charsets;
  DYNAMIC_ARRAY                         cat_users;
  DYNAMIC_ARRAY                         cat_tablespaces;
  DYNAMIC_ARRAY                         cat_databases;
  DYNAMIC_ARRAY                         cat_snapshots;
  DYNAMIC_ARRAY                         cat_image_ordered_items;
  DYNAMIC_ARRAY                         cat_image_ordered_metadata;
};

/*
  Meta data.
*/
struct st_backup_metadata
{
  struct st_blob                        md_query;
  struct st_blob                        md_data;
};

/*
  Global objects: character sets, users, table spaces.
  Databases are handled independently.

  note: glb_item must be first element in st_backup_tablespace.
*/
struct st_backup_global
{
  struct st_bstream_item_info           glb_item;       /* must be 1st */
  struct st_backup_metadata             glb_metadata;
  const char                            *glb_typename;
};

/*
  Per database objects: views, stored procedures, stored functions,
  events, triggers, privileges.
  Tables are handled independently.

  note: perdb_item must be first element in st_backup_table.
*/
struct st_backup_perdb
{
  struct st_bstream_dbitem_info         perdb_item;     /* must be 1st */
  struct st_backup_metadata             perdb_metadata;
};

/*
  Table.

  note: tbl_item must be first element in st_backup_table.
*/
struct st_backup_table
{
  struct st_bstream_table_info          tbl_item;       /* must be 1st */
  struct st_backup_metadata             tbl_metadata;
  ulonglong                             tbl_data_size;
};

/*
  Database.

  The dynamic array holds pointers to items of the following type:

  struct st_backup_table                db_tables
  struct st_backup_perdb                db_perdbs

  note: db_item must be first element in st_backup_database.
*/
struct st_backup_database
{
  struct st_bstream_db_info             db_item;        /* must be 1st */
  struct st_backup_metadata             db_metadata;
  DYNAMIC_ARRAY                         db_tables;
  DYNAMIC_ARRAY                         db_perdbs;
};

/*
  Snapshot.

  Tables belong to databases. But in the table data chunks they are
  numbered by snapshot number and table number. The table number is
  relative to the snapshot. To find the table item within its database
  we need an index from the table number (pos) within the snapshot
  to the table item.

  For every snapshot there is a struct st_backup_snapshot with an
  array that has a reference per table of that snapshot.

  The dynamic array holds pointers to items of the following type:

  struct st_backup_table                snap_index_pos_to_table
*/
struct st_backup_snapshot
{
  DYNAMIC_ARRAY                         snap_index_pos_to_table;
};

/*
  =========================
  Stream reading functions.
  =========================
*/

/*
  Helper struct for stream access functions.

  This is for internal use of backup_stream.c only.
  It is here to allow the use of "struct st_stream*" as the type
  of the stream handle in access functions to the backup_stream module.
  The alternative, to use void* for the handle, has been rejected.

  The stream access functions do not read the backup image file
  directly. They call back functions provided in st_backup_stream by the
  application.

  The struct st_stream ties together the st_backup_stream
  with the data required by the I/O functions provided by this
  application. Note that 'st_backup_stream' must be the first part of
  the helper struct.

  'stream_pos' is used to describe which section/chunk of a backup
  image we have read last.
*/
struct st_stream
{
  struct st_backup_stream       bupstrm;        /* Must be first in st_stream */
  File                          fd;             /* File descriptor */
  my_off_t                      pos;            /* Byte position in file */
  my_off_t                      size;           /* File size */
  const char                    *path;          /* File name */
  const char                    *stream_pos;    /* Verbal stream position */
  const char                    *zalgo;         /* Compression algorithm */
#ifdef HAVE_COMPRESS
  uchar                         *zbuf;          /* Buffer for compressed data */
  z_stream                      zstream;        /* zlib helper struct */
#endif
};

/* The below shall also be usable by C++. */
C_MODE_START

struct st_backup_catalog*
backup_catalog_allocate(void);

void
backup_catalog_free(struct st_backup_catalog *bup_catalog);

struct st_stream*
backup_image_open(const char *filename, struct st_backup_catalog *bup_catalog);

enum enum_bstream_ret_codes
backup_image_close(struct st_stream *strm);

enum enum_bstream_ret_codes
backup_read_catalog(struct st_stream *strm,
                    struct st_backup_catalog *bup_catalog);

enum enum_bstream_ret_codes
backup_read_metadata(struct st_stream *strm,
                     struct st_backup_catalog *bup_catalog);

enum enum_bstream_ret_codes
backup_read_snapshot(struct st_stream *strm,
                     struct st_backup_catalog *bup_catalog,
                     struct st_bstream_data_chunk *snapshot);

enum enum_bstream_ret_codes
backup_read_summary(struct st_stream *strm,
                    struct st_backup_catalog *bup_catalog);

struct st_backup_global*
backup_locate_global(const char *typnam, DYNAMIC_ARRAY *array, ulong pos);

struct st_backup_table*
backup_locate_table(struct st_backup_catalog *bup_catalog,
                    ulong snap_num, ulong pos);

struct st_backup_perdb*
backup_locate_perdb(struct st_backup_catalog *bup_catalog,
                    ulong db_pos, ulong pos);

/*
  =================================
  Convenience functions and macros.
  =================================
*/

#if !defined(DBUG_OFF)

/* In a debug version go through functions to type-check the arguments. */
ulong backup_blob_length(struct st_blob *blob);
char *backup_blob_string(struct st_blob *blob);

#define BBL(_b_) backup_blob_length(_b_)
#define BBS(_b_) backup_blob_string(_b_)

#else /* !defined(DBUG_OFF) */

#define BBL(_b_) ((ulong) (blob->end - blob->begin))
#define BBS(_b_) ((char*) blob->begin)

#endif /* !defined(DBUG_OFF) */

/*
  Backup blob length and string.
  This is for use with printf format '%.*s'.
  This must be a macro because it produces two comma separated values!
*/
#define BBLS(_b_) ((uint) BBL(_b_)), BBS(_b_)

/* The above shall also be usable by C++. */
C_MODE_END

