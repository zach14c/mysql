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

#include "backup_stream.h"

/* MY_STAT */
#include "my_dir.h"

/* strerror, memcmp */
#include <string.h>

/* isdigit(), isspace() */
#include <ctype.h>

/* bzero for Windows */
#include "m_string.h"

/*
  Error injection. Built on DBUG.
  This is similar to error injection in the server.
  In case that one becomes globally available, we undefine it first.
*/
#ifdef ERROR_INJECT
#undef ERROR_INJECT
#endif
#define ERROR_INJECT(_keyword_, _action_) \
  DBUG_EXECUTE_IF((_keyword_), errm("ERROR_INJECT(\"%s\")\n", (_keyword_)); \
                  DBUG_PRINT("bupstrm", ("ERROR_INJECT(\"%s\")\n",          \
                                         (_keyword_))); _action_)

/* For easier error tracking we do not pre-allocate in DBUG mode. */
#if !defined(DBUG_OFF)
#define DYN_ALLOC_INIT 0
#define DYN_ALLOC_INCR 1
#else
#define DYN_ALLOC_INIT 1000
#define DYN_ALLOC_INCR 1000
#endif

/*
  ====================================================
  Convenience functions for access to st_blob objects.
  ====================================================
*/

#if !defined(DBUG_OFF)

/**
  Backup blob length.

  In a debug version this is a function, not a macro, for better type checking.

  @param[in]    blob            blob

  @return       length
*/

ulong backup_blob_length(struct st_blob *blob)
{
  DBUG_ASSERT(blob);
  DBUG_ASSERT(blob->end >= blob->begin);
  return ((ulong) (blob->end - blob->begin));
}


/**
  Backup blob string.

  In a debug version this is a function, not a macro, for better type checking.

  @param[in]    blob            blob

  @return       string
*/

char *backup_blob_string(struct st_blob *blob)
{
  DBUG_ASSERT(blob);
  DBUG_ASSERT(blob->end >= blob->begin);
  return ((char*) blob->begin);
}

#endif /* !defined(DBUG_OFF) */


/*
  ====================================================
  Memory allocation.
  These are callback functions for the stream library.
  ====================================================
*/

/**
  Allocate given amount of memory and return pointer to it.

  @param[in]    size            amount of memory to allocate

  @return       pointer to allocated memory
*/

bstream_byte* bstream_alloc(unsigned long int size)
{
  return my_malloc(size, MYF(MY_WME));
}


/**
  Free previously allocated memory.

  @param[in]    ptr             pointer to allocated memory
*/

void bstream_free(bstream_byte *ptr)
{
  my_free(ptr, MYF(MY_ALLOW_ZERO_PTR));
}


/*
  ====================================================
  Helper functions for the stream library.
  These are callback functions for the stream library.
  Low-level stream access.
  ====================================================
*/

/*
  Verbal stream positions for the stream_pos member of struct st_stream.
*/
const char *STREAM_POS_PREFIX=          "prefix";
const char *STREAM_POS_HEADER=          "header";
const char *STREAM_POS_CATALOG=         "catalog";
const char *STREAM_POS_META_DATA=       "meta data";
const char *STREAM_POS_TABLE_DATA=      "table data";
const char *STREAM_POS_SUMMARY=         "summary";


/**
  Read from the stream/image.

  @param[in,out]    strm        stream handle, updating position
  @param[in,out]    data        data container, updating contents and ptrs
  @param            envelope    not used

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     BSTREAM_EOS     end of stream
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.
*/

static int
str_read(struct st_stream *strm, struct st_blob *data,
         struct st_blob envelope __attribute__((unused)))
{
  size_t        lgt;
  int           rc= BSTREAM_OK;
  DBUG_ENTER("str_read");
  DBUG_ASSERT(strm && strm->path);
  DBUG_ASSERT(data && data->begin && data->end);

  /*
    Compute wanted length.
  */
  lgt= BBL(data);
  DBUG_PRINT("bupstrm", ("want bytes: %lu", (ulong) lgt));

  /*
    Read.
  */
#ifdef HAVE_COMPRESS
  if (strm->zbuf)
  {
    int zerr;
    z_stream *zstream= &strm->zstream;

    /* Set output buffer pointer. */
    zstream->next_out= data->begin;
    /*
      Zstream can process in one go a block whose size fits into uInt
      type. If we have more space available in the buffer, we ignore the
      extra bytes.
    */
    if (lgt > ~((uInt)0))
      lgt= ~((uInt)0); /* purecov: inspected */
    /* Set output buffer size. */
    zstream->avail_out= (uInt) lgt;

    lgt= 0; /* In case zbuf is empty and we are at EOF. */
    do
    {
      /* If input buffer is empty, load data from compressed image. */
      if (!zstream->avail_in)
      {
        zstream->avail_in= (uInt) my_read(strm->fd, strm->zbuf, ZBUF_SIZE,
                                           MYF(0));
        ERROR_INJECT("str_read_z_io_error",
                     zstream->avail_in= (uInt) MY_FILE_ERROR;
                     my_errno= EIO;);
        ERROR_INJECT("str_read_z_eof",
                     zstream->avail_in= 0;);
        if (zstream->avail_in == (uInt) MY_FILE_ERROR)
        {
          errm("cannot read compressed image '%s': %s\n",
               strm->path, strerror(my_errno));
          rc= BSTREAM_ERROR;
          goto end;
        }
        else if (!zstream->avail_in)
        {
          /* EOF. If first read, we leave with lgt == 0 here. */
          break;
        }
        zstream->next_in= strm->zbuf;
      }
      /* Decompress, */
      zerr= inflate(zstream, Z_NO_FLUSH);
      ERROR_INJECT("str_read_zlib_error",
                   zerr= Z_STREAM_ERROR;
                   zstream->msg= (char*) "error injected";);
      /* Set output length. */
      lgt= zstream->next_out - data->begin;
      if (zerr == Z_STREAM_END)
      {
        /* EOF. If first decompress, we leave with lgt == 0 here. */
        break;
      }
      else if (zerr != Z_OK)
      {
        errm("cannot decompress image '%s': %d: %s\n",
             strm->path, zerr, zstream->msg);
        rc= BSTREAM_ERROR;
        goto end;
      }
    } while (zstream->avail_out);
    /* lgt contains the number of bytes decompressed. */
  }
  else
#endif
  {
    lgt= my_read(strm->fd, data->begin, lgt, MYF(0));
    ERROR_INJECT("str_read_io_error",
                 lgt= MY_FILE_ERROR; my_errno= EIO;);
    if (lgt == MY_FILE_ERROR)
    {
      errm("cannot read image '%s': %s\n", strm->path, strerror(my_errno));
      rc= BSTREAM_ERROR;
      goto end;
    }
  }
  DBUG_PRINT("bupstrm", ("read bytes: %lu", (ulong) lgt));

  /*
    Check for end of stream.
  */
  if (lgt == 0)
  {
    rc= BSTREAM_EOS;
    goto end;
  }

  /*
    Update stream handle and data container.
  */
  strm->pos+= lgt;
  data->begin+= lgt;

 end:
  DBUG_RETURN(rc);
}


#ifdef NOTUSED
/**
  Skip part of the stream/image.

  @param[in,out]    strm        stream handle, updating position
  @param[in,out]    len         number of bytes to skip, skipped

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.
*/

static int
str_forward(struct st_stream *strm, size_t *len)
{
  int           rc= BSTREAM_OK;
  DBUG_ENTER("str_forward");
  DBUG_ASSERT(strm && strm->path);
  DBUG_ASSERT(len);

#ifdef HAVE_COMPRESS
  if (strm->zbuf)
  {
    /* Need to read *len bytes from decompressed stream. */
    z_stream    *zstream= &strm->zstream;
    size_t      lgt= *len;
    int         zerr;
    uchar       iobuff[IO_SIZE];

    /*
      Zstream can process in one go a block whose size fits into uInt
      type. If we have more space available in the buffer, we ignore
      the extra bytes.
    */
    DBUG_ASSERT(IO_SIZE <= ~((uInt)0));

    do
    {
      /* Set output buffer pointer and size. */
      zstream->next_out= iobuff;
      if (lgt > IO_SIZE)
        zstream->avail_out= IO_SIZE;
      else
        zstream->avail_out= (uInt) lgt;

      /* If input buffer is empty, load data from compressed image. */
      if (!zstream->avail_in)
      {
        zstream->avail_in= (uInt) my_read(strm->fd, strm->zbuf, ZBUF_SIZE,
                                          MYF(0));
        if (zstream->avail_in == (uInt) MY_FILE_ERROR)
        {
          errm("cannot read compressed image '%s': %s\n",
               strm->path, strerror(my_errno));
          rc= BSTREAM_ERROR;
          goto end;
        }
        else if (!zstream->avail_in)
        {
          /* EOF. This is an error on seek. */
          errm("end of stream in seek on image '%s'\n", strm->path);
          rc= BSTREAM_EOS;
          goto end;
        }
        zstream->next_in= strm->zbuf;
      }
      /* Decompress, */
      zerr= inflate(zstream, Z_NO_FLUSH);
      /* Reduce wanted length by what we got. */
      lgt-= zstream->next_out - iobuff;
      if (zerr == Z_STREAM_END)
      {
        /* EOF. This is an error on seek. */
        errm("end of stream in seek on image '%s'\n", strm->path);
        rc= BSTREAM_EOS;
        goto end;
      }
      else if (zerr != Z_OK)
      {
        errm("cannot decompress image '%s': %d: %s\n",
             strm->path, zerr, zstream->msg);
        rc= BSTREAM_ERROR;
        goto end;
      }
    } while (lgt);

    /* Remember current file position. */
    strm->pos+= *len;
  }
  else
#endif
  {
    my_off_t      off_new;

#if !defined(DBUG_OFF)
    /*
      Check if internal counter matches real file position.
    */
    my_off_t off_cur= my_seek(strm->fd, (my_off_t) 0, SEEK_CUR, MYF(0));
    if (off_cur == MY_FILEPOS_ERROR)
    {
      errm("cannot seek in image '%s': %s\n", strm->path, strerror(my_errno));
      rc= BSTREAM_ERROR;
      goto end;
    }
    DBUG_ASSERT(off_cur == strm->pos);
#endif

    /*
      Advance file pointer by *len bytes.
    */
    off_new= my_seek(strm->fd, (my_off_t) *len, SEEK_CUR, MYF(0));
    if (off_new == MY_FILEPOS_ERROR)
    {
      errm("cannot seek in image '%s': %s\n", strm->path, strerror(my_errno));
      rc= BSTREAM_ERROR;
      goto end;
    }

    /* Compute actual skip length from old and new file positions. */
    *len= (size_t) (off_new - strm->pos);
    /* Remember current file position. */
    strm->pos= off_new;
  }

 end:
  DBUG_RETURN(rc);
}
#endif /*NOTUSED*/


/**
  Open the stream/image for reading.

  A backup stream has a prefix, consisting of 8 bytes magic number
  and 2 bytes version number.

  A compressed image has a gzip magic number.

  TODO An encrypted image has yet another magic number.

  @param[in,out]    strm        stream handle, updating path, fd, bupstrm
  @param[in]        path        image path name
  @param[out]       version_p   detected image version

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.
*/

static int
str_open_rd(struct st_stream *strm, const char *path, uint *version_p)
{
  MY_STAT               stat_area;
  size_t                lgt;
  int                   rc= BSTREAM_OK;
  int                   errpos= 0;
  uint16                version;
  const unsigned char   backup_magic_bytes[8]=
    {
      0xE0, // ###.....
      0xF8, // #####...
      0x7F, // .#######
      0x7E, // .######.
      0x7E, // .######.
      0x5F, // .#.#####
      0x0F, // ....####
      0x03  // ......##
    };
#ifdef HAVE_COMPRESS
  const unsigned char   gzip_magic_bytes[3]=
    {
      0x1f,
      0x8b,
      0x08
    };
#endif
  uchar  prefix[sizeof(backup_magic_bytes) + sizeof(version)];
  DBUG_ENTER("str_open_rd");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(path);
  DBUG_ASSERT(version_p);
  /* These are required to match the backup image prefix. */
  DBUG_ASSERT(sizeof(backup_magic_bytes) == 8);
  DBUG_ASSERT(sizeof(version) == 2);
  DBUG_ASSERT(sizeof(prefix) == 10);

  /*
    Initialize stream struct.
  */
  bzero(strm, sizeof(*strm));

  /*
    Set image path name.
  */
  strm->path= path;
  DBUG_PRINT("bupstrm", ("opening image file: '%s'", strm->path));

  /*
    Open the image file.
  */
  strm->fd= my_open(strm->path, O_RDONLY, MYF(0));
  if (strm->fd < 0)
  {
    errm("cannot open backup image '%s': %s\n", strm->path, strerror(my_errno));
    goto err;
  }
  errpos= 10;
  DBUG_PRINT("bupstrm", ("opened  image file: '%s'  fd: %d",
                         strm->path, strm->fd));

  ERROR_INJECT("str_open_rd_stat_error", my_close(strm->fd, MYF(0)););
  if (my_fstat(strm->fd, &stat_area, MYF(0)))
  {
    errm("cannot fstat open backup image '%s': %s\n",
         strm->path, strerror(my_errno= errno));
    goto err;
  }
  strm->size= stat_area.st_size;

  /*
    Read prefix with magic number and image version.
  */
  lgt= my_read(strm->fd, prefix, sizeof(prefix), MYF(0));
  ERROR_INJECT("str_open_rd_read_error", lgt= MY_FILE_ERROR; my_errno= EIO;);
  ERROR_INJECT("str_open_rd_read_length", lgt= sizeof(prefix) - 1;);
  if (lgt != sizeof(prefix))
  {
    if (lgt == MY_FILE_ERROR)
      errm("cannot read image '%s': %s\n", strm->path, strerror(my_errno));
    else
      errm("image '%s' has only %lu bytes of at least %lu required\n",
           strm->path, (ulong) lgt, (ulong) sizeof(prefix));
    goto err;
  }
  DBUG_PRINT("bupstrm", ("read magic number and version"));

#ifdef HAVE_COMPRESS
  /*
    Check for compression.
  */
  if (!memcmp(prefix, gzip_magic_bytes, sizeof(gzip_magic_bytes)))
  {
    int                 zerr;
    bstream_blob        blob;

    ERROR_INJECT("str_open_rd_zbuf_malloc", my_malloc_error_inject= 1;);
    strm->zbuf= (uchar*) my_malloc(ZBUF_SIZE, MYF(MY_WME));
    if (!strm->zbuf)
    {
      /* Error message reported by mysys. */
      goto err;
    }
    errpos= 20;
    strm->zstream.zalloc= 0;
    strm->zstream.zfree= 0;
    strm->zstream.opaque= 0;
    strm->zstream.msg= 0;
    strm->zstream.next_in= strm->zbuf;
    strm->zstream.avail_in= 10;
    memcpy(strm->zbuf, prefix, 10);

    zerr= inflateInit2(&strm->zstream, MAX_WBITS + 16);
    ERROR_INJECT("str_open_rd_zlib_init",
                 zerr= Z_STREAM_ERROR;
                 strm->zstream.msg= (char*) "error injected";);
    if (zerr != Z_OK)
    {
      errm("cannot init zlib on image '%s': %d: %s\n",
           strm->path, zerr, strm->zstream.msg);
      goto err;
    }
    errpos= 30;

    blob.begin= prefix;
    blob.end= prefix + sizeof(prefix);
    zerr= str_read(strm, &blob, blob);
    if (zerr == BSTREAM_EOS)
    {
      errm("end of stream within header on image '%s'\n", strm->path);
      goto err;
    }
    if (zerr != BSTREAM_OK || (blob.begin != blob.end))
    {
      /* Error message reported by function. */
      goto err;
    }
    strm->zalgo= "gzip";
  }
  else
#endif
    strm->zalgo= "none";

  /*
    Check magic number and image version.
  */
  if (memcmp(prefix, backup_magic_bytes, sizeof(backup_magic_bytes)))
  {
    errm("not a backup image file: '%s'. Magic number mismatch.\n", strm->path);
    goto err;
  }
  ERROR_INJECT("str_open_rd_version", prefix[8]= '\0';);
  version= uint2korr(prefix+8);
  if (version != 1)
  {
    errm("backup image version %u is not supported\n", version);
    goto err;
  }
  *version_p= version;

  /*
    Set callback functions in struct st_backup_stream.
  */
  strm->bupstrm.stream.write=   (as_write_m) NULL;
  strm->bupstrm.stream.read=    (as_read_m) str_read;
  strm->bupstrm.stream.forward= (as_forward_m) NULL; /* str_forward; */

  /*
    Open the stream.
  */
  DBUG_PRINT("bupstrm", ("opening stream: '%s'  fd: %d", strm->path, strm->fd));
  rc= bstream_open_rd(&strm->bupstrm, sizeof(prefix));
  if (rc != BSTREAM_OK)
  {
    errm("cannot open stream library on '%s'.\n", strm->path);
    goto err;
  }
  DBUG_PRINT("bupstrm", ("opened  stream: '%s'  fd: %d  rc: %d",
                         strm->path, strm->fd, rc));

  goto end;

err:
  switch (errpos) {
#ifdef HAVE_COMPRESS
  case 30:
    (void) inflateEnd(&strm->zstream);
  case 20:
    my_free(strm->zbuf, MYF(0));
    strm->zbuf= NULL;
#endif
  case 10:
    (void) my_close(strm->fd, MYF(0));
    strm->fd= -1;
  }
  rc= BSTREAM_ERROR;

 end:
  DBUG_RETURN(rc);
}


/**
  Close the stream/image.

  @param[in]    strm            stream handle

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.
*/

static int
str_close(struct st_stream *strm)
{
  int rc;
  int rc1;
  DBUG_ENTER("str_close");
  DBUG_ASSERT(strm && strm->path);

  /* Close the stream. */
  rc= bstream_close(&strm->bupstrm);
  ERROR_INJECT("str_close_bstream", rc= BSTREAM_ERROR;);
  if (rc != BSTREAM_OK)
  {
    errm("cannot close stream library on '%s'.\n",
         strm->path);
  }

#ifdef HAVE_COMPRESS
  /* Close zlib. */
  if (strm->zbuf)
  {
    int zerr= inflateEnd(&strm->zstream);
    ERROR_INJECT("str_close_zlib",
                 zerr= Z_STREAM_ERROR;
                 strm->zstream.msg= (char*) "error injected";);
    if (zerr != Z_OK)
    {
      errm("cannot close zlib on image '%s': %d: %s\n",
           strm->path, zerr, strm->zstream.msg);
      rc= BSTREAM_ERROR;
    }
    my_free(strm->zbuf, (MYF(0)));
    strm->zbuf= NULL;
  }
#endif

  /* Close the image file. */
  rc1= my_close(strm->fd, MYF(0));
  ERROR_INJECT("str_close_file", rc1= 1; my_errno= EIO;);
  if (rc1)
  {
    errm("cannot close image '%s': %s\n",
         strm->path, strerror(my_errno));
    rc= BSTREAM_ERROR;
  }
  strm->fd= -1;

  DBUG_RETURN(rc);
}


/*
  ====================================================
  Backup image item handling.
  These are callback functions for the stream library.
  ====================================================
*/

/*
  Helper struct for iterators.
*/
struct st_backup_iterator
{
  DYNAMIC_ARRAY                 *it_array;      /* Array to iterate */
  int                           it_index;       /* Iterator value */
  enum enum_bstream_item_type   it_type;        /* Iterator type */
};


/**
  Clear catalogue and prepare it for populating with items.

  @param[in]    hdr             catalog reference

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.

  @note This is empty because backup_catalog_allocate() initializes
  the catalog properly.
*/

int
bcat_reset(struct st_bstream_image_header *hdr __attribute__((unused)))
{
  DBUG_ENTER("bcat_reset");
  DBUG_RETURN(BSTREAM_OK);
}


/**
  Close catalogue after all items have been added to it.

  This allows for finalizing operations. It is not meant for
  deletion of the catalog. There is no "open" action. The
  approximate counterpart to bcat_close() is bcat_reset().

  @param[in]    hdr             catalog reference

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.

  @note This is empty because there is no finalization required.
*/

int
bcat_close(struct st_bstream_image_header *hdr __attribute__((unused)))
{
  DBUG_ENTER("bcat_close");
  DBUG_RETURN(BSTREAM_OK);
}


/**
  Add item to the catalog.

  For items that belong to a database, the base.db element points
  to the databases' catalog item. The stream library evaluates
  that pointer using an iterator provided by bcat_iterator_get().

  The item name is allocated by the stream library and must be freed
  by the application later.

  @param[in,out]    hdr         catalog ref, updating catalog
  @param[in]        item        item reference

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note item->pos should be set to indicate the position of the item in
  their container. This is a global position in the catalog for global
  items, including databases, a position relative to a database for
  items that belong to a database, except of tables, which are numbered
  relative to the snapshot, which stores their data.

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.
*/

int
bcat_add_item(struct st_bstream_image_header *hdr,
              struct st_bstream_item_info *item)
{
  struct st_backup_catalog      *bup_catalog= (struct st_backup_catalog*) hdr;
  struct st_bstream_item_info   *cat_item;
  enum enum_bstream_ret_codes   brc= BSTREAM_OK;
  int                           err;
  DBUG_ENTER("bcat_add_item");
  DBUG_ASSERT(hdr);
  DBUG_ASSERT(item);
  DBUG_PRINT("bupstrm", ("adding item: 0x%lx  pos: %lu  type: %d  name: '%.*s'",
                         (long) item, item->pos, item->type,
                         BBLS(&item->name)));

  switch (item->type) {

  case BSTREAM_IT_CHARSET:
  case BSTREAM_IT_USER:
  case BSTREAM_IT_TABLESPACE:
  {
    struct st_backup_global     *bup_global;
    DYNAMIC_ARRAY               *array;

    /* Allocate memory for the item. */
    ERROR_INJECT("bcat_add_item_global_malloc", my_malloc_error_inject= 1;);
    bup_global= (struct st_backup_global*)
      my_malloc(sizeof(struct st_backup_global), MYF(MY_WME));
    if (!bup_global)
    {
      /* Error message reported by mysys. */
      brc= BSTREAM_ERROR;
      goto end;
    }
    cat_item= &bup_global->glb_item;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_global);
    /* Copy the item struct into the allocated struct. */
    bup_global->glb_item= *item;
    /* Initialize other elements. */
    bzero(&bup_global->glb_metadata, sizeof(bup_global->glb_metadata));

    switch (item->type) {
    case BSTREAM_IT_CHARSET:
      bup_global->glb_typename= "Charset";
      array= &bup_catalog->cat_charsets;
      break;
    case BSTREAM_IT_USER:
      /* purecov: begin inspected */
      bup_global->glb_typename= "User";
      array= &bup_catalog->cat_users;
      break;
      /* purecov: end */
    case BSTREAM_IT_TABLESPACE:
      bup_global->glb_typename= "Tablespace";
      array= &bup_catalog->cat_tablespaces;
      break;
    default:
      /* purecov: begin deadcode */
      DBUG_ASSERT(0);
      array= NULL; /* Avoid compiler warning: array may be used uninitialized */
      brc= BSTREAM_ERROR;
      goto end;
      /* purecov: end */
    }

    /* Check consistency of array position. */
    DBUG_ASSERT(item->pos == array->elements);
    /* Insert a reference into the catalog. */
    ERROR_INJECT("bcat_add_item_global_insert", my_malloc_error_inject= 1;);
    err= insert_dynamic(array, (uchar*) &bup_global);
    if (err)
    {
      /* Error message reported by mysys. */
      my_free(bup_global, MYF(0));
      brc= BSTREAM_ERROR;
      goto end;
    }
    DBUG_PRINT("bupstrm",
               ("Added %s: '%.*s'  item: 0x%lx", bup_global->glb_typename,
                BBLS(&item->name), (long) bup_global));
    break;
  }

  case BSTREAM_IT_DB:
  {
    struct st_backup_database *bup_database;

    /* Allocate memory for the item. */
    ERROR_INJECT("bcat_add_item_db_malloc", my_malloc_error_inject= 1;);
    bup_database= (struct st_backup_database*)
      my_malloc(sizeof(struct st_backup_database), MYF(MY_WME));
    if (!bup_database)
    {
      /* Error message reported by mysys. */
      brc= BSTREAM_ERROR;
      goto end;
    }
    cat_item= &bup_database->db_item.base;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_database);
    /* Copy the item struct into the local struct. */
    bup_database->db_item= *((struct st_bstream_db_info*) item);
    /* Initialize other elements. */
    bzero(&bup_database->db_metadata, sizeof(bup_database->db_metadata));
    /* Note that the array contains pointers only. */
    err= my_init_dynamic_array(&bup_database->db_tables,
                               sizeof(struct st_backup_table*),
                               DYN_ALLOC_INIT, DYN_ALLOC_INCR);
    ERROR_INJECT("bcat_add_item_db_tables", if (!err) {err= TRUE;
        delete_dynamic(&bup_database->db_tables);});
    if (err)
    {
      /* Error message reported by mysys. */
      my_free(bup_database, MYF(0));
      brc= BSTREAM_ERROR;
      goto end;
    }
    /* Note that the array contains pointers only. */
    err= my_init_dynamic_array(&bup_database->db_perdbs,
                               sizeof(struct st_backup_perdb*),
                               DYN_ALLOC_INIT, DYN_ALLOC_INCR);
    ERROR_INJECT("bcat_add_item_db_perdbs", if (!err) {err= TRUE;
        delete_dynamic(&bup_database->db_perdbs);});
    if (err)
    {
      /* Error message reported by mysys. */
      delete_dynamic(&bup_database->db_tables);
      my_free(bup_database, MYF(0));
      brc= BSTREAM_ERROR;
      goto end;
    }
    /* Check consistency of array position. */
    DBUG_ASSERT(item->pos == bup_catalog->cat_databases.elements);
    /* Insert a reference into the catalog. */
    ERROR_INJECT("bcat_add_item_db_insert", my_malloc_error_inject= 1;);
    err= insert_dynamic(&bup_catalog->cat_databases, (uchar*) &bup_database);
    if (err)
    {
      /* Error message reported by mysys. */
      delete_dynamic(&bup_database->db_perdbs);
      delete_dynamic(&bup_database->db_tables);
      my_free(bup_database, MYF(0));
      brc= BSTREAM_ERROR;
      goto end;
    }
    DBUG_PRINT("bupstrm",
               ("added database: '%.*s'  item: 0x%lx", BBLS(&item->name),
                (long) bup_database));
    break;
  }

  case BSTREAM_IT_TABLE:
  {
    struct st_backup_table      *bup_table;
    struct st_backup_database   *bup_database;
    struct st_backup_snapshot   *bup_snapshot;
    uint                        snap_num;

    /* Allocate memory for the item. */
    ERROR_INJECT("bcat_add_item_table_malloc", my_malloc_error_inject= 1;);
    bup_table= (struct st_backup_table*)
      my_malloc(sizeof(struct st_backup_table), MYF(MY_WME));
    if (!bup_table)
    {
      /* Error message reported by mysys. */
      brc= BSTREAM_ERROR;
      goto end;
    }
    cat_item= &bup_table->tbl_item.base.base;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_table);
    /* Copy the item struct into the local struct. */
    bup_table->tbl_item= *((struct st_bstream_table_info*) item);
    /* Initialize other elements. */
    bzero(&bup_table->tbl_metadata, sizeof(bup_table->tbl_metadata));
    bup_table->tbl_data_size= 0;
    /* Get the database reference from the item. */
    bup_database= (struct st_backup_database*) bup_table->tbl_item.base.db;
    DBUG_PRINT("bupstrm",
               ("referred db item: 0x%lx  pos: %lu  type: %d  name: '%.*s'",
                (long) bup_database, bup_database->db_item.base.pos,
                bup_database->db_item.base.type,
                BBLS(&bup_database->db_item.base.name)));
    /* No consistency check of item->pos. It doesn't count per database. */
    /* Insert a reference into its database's array. */
    ERROR_INJECT("bcat_add_item_table_insert", my_malloc_error_inject= 1;);
    err= insert_dynamic(&bup_database->db_tables, (uchar*) &bup_table);
    if (err)
    {
      /* Error message reported by mysys. */
      my_free(bup_table, MYF(0));
      brc= BSTREAM_ERROR;
      goto end;
    }
    /* Check plausibility of array position. */
    snap_num= bup_table->tbl_item.snap_num;
    DBUG_ASSERT(snap_num < bup_catalog->cat_snapshots.elements);
    /* Get a reference to the snapshot information. */
    bup_snapshot= *((struct st_backup_snapshot**)
                    dynamic_array_ptr(&bup_catalog->cat_snapshots, snap_num));
    DBUG_ASSERT(bup_snapshot); // Never fails, snap_num is checked
    /*
      Check consistency of array position.
      Table items are not numbered per database, but per snapshot.
    */
    DBUG_ASSERT(item->pos == bup_snapshot->snap_index_pos_to_table.elements);
    /* Insert a reference into the snapshot's index array. */
    ERROR_INJECT("bcat_add_item_table_index", my_malloc_error_inject= 1;);
    err= insert_dynamic(&bup_snapshot->snap_index_pos_to_table,
                        (uchar*) &bup_table);
    if (err)
    {
      /* Error message reported by mysys. */
      /* Do not free bup_table here. It's in the database array already. */
      brc= BSTREAM_ERROR;
      goto end;
    }
    DBUG_PRINT("bupstrm",
               ("added table: '%.*s'.'%.*s'  item: 0x%lx  snap_num: %u",
                BBLS(&bup_database->db_item.base.name), BBLS(&item->name),
                (long) bup_table, bup_table->tbl_item.snap_num));
    break;
  }

  case BSTREAM_IT_PRIVILEGE:
  case BSTREAM_IT_VIEW:
  case BSTREAM_IT_SPROC:
  case BSTREAM_IT_SFUNC:
  case BSTREAM_IT_EVENT:
  case BSTREAM_IT_TRIGGER:
  {
    struct st_backup_perdb      *bup_perdb;
    struct st_backup_database   *bup_database;

    /* Allocate memory for the item. */
    ERROR_INJECT("bcat_add_item_perdb_malloc", my_malloc_error_inject= 1;);
    bup_perdb= (struct st_backup_perdb*)
      my_malloc(sizeof(struct st_backup_perdb), MYF(MY_WME));
    if (!bup_perdb)
    {
      /* Error message reported by mysys. */
      brc= BSTREAM_ERROR;
      goto end;
    }
    cat_item= &bup_perdb->perdb_item.base;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_perdb);
    /* Copy the item struct into the local struct. */
    bup_perdb->perdb_item= *((struct st_bstream_dbitem_info*) item);
    /* Initialize other elements. */
    bzero(&bup_perdb->perdb_metadata, sizeof(bup_perdb->perdb_metadata));
    /* For privileges strip off the unique sequence number. */
    if (item->type == BSTREAM_IT_PRIVILEGE)
    {
      uchar *beg= bup_perdb->perdb_item.base.name.begin;
      uchar *ptr= bup_perdb->perdb_item.base.name.end;
      while ((ptr > beg) && isdigit(*(--ptr))) {} /* Strip off digits */
      while ((ptr > beg) && isspace(*(--ptr))) {} /* Strip off spaces */
      bup_perdb->perdb_item.base.name.end= ++ptr; /* Fix string end */
    }
    /* Get the database reference from the item. */
    bup_database= (struct st_backup_database*) bup_perdb->perdb_item.db;
    DBUG_PRINT("bupstrm",
               ("referred db item: 0x%lx  pos: %lu  type: %d  name: '%.*s'",
                (long) bup_database, bup_database->db_item.base.pos,
                bup_database->db_item.base.type,
                BBLS(&bup_database->db_item.base.name)));
    /* Check consistency of array position. */
    DBUG_ASSERT(item->pos == bup_database->db_perdbs.elements);
    /* Insert a reference into its database's array. */
    ERROR_INJECT("bcat_add_item_perdb_insert", my_malloc_error_inject= 1;);
    err= insert_dynamic(&bup_database->db_perdbs, (uchar*) &bup_perdb);
    if (err)
    {
      /* Error message reported by mysys. */
      my_free(bup_perdb, MYF(0));
      brc= BSTREAM_ERROR;
      goto end;
    }
#if !defined(DBUG_OFF)
    {
      const char *typ;

      switch (item->type) {
      case BSTREAM_IT_VIEW:       typ= "view"; break;
      case BSTREAM_IT_PRIVILEGE:  typ= "privilege"; break;
      default:                    typ= "unknown"; break;
      }
      DBUG_PRINT("bupstrm",
                 ("added %s: '%.*s'.'%.*s'  item: 0x%lx", typ,
                  BBLS(&bup_database->db_item.base.name),
                  BBLS(&bup_perdb->perdb_item.base.name),
                  (long) bup_perdb));
    }
#endif
    break;
  }

  /* purecov: begin inspected */
  default:
  {
    errm("backup object not yet implemented: unknown type\n");
    brc= BSTREAM_ERROR;
    DBUG_PRINT("bupstrm", ("NOTYET implemented: unknown type: '%.*s'",
                           BBLS(&item->name)));
    DBUG_ASSERT(0);
    goto end;
  }
  /* purecov: end */
  }

  /* Insert a reference to the item into the image ordered items array. */
  ERROR_INJECT("bcat_add_item_ordered_insert", my_malloc_error_inject= 1;);
  DBUG_PRINT("bupstrm", ("image ordered item added: 0x%lx", (long) cat_item));
  err= insert_dynamic(&bup_catalog->cat_image_ordered_items,
                      (uchar*) &cat_item);
  if (err)
    brc= BSTREAM_ERROR;

 end:
  DBUG_RETURN(brc);
}


/**
  Create global iterator of a given type.

  Possible iterator types.

  - BSTREAM_IT_CHARSET:    all charsets
  - BSTREAM_IT_USER:       all users
  - BSTREAM_IT_TABLESPACE: all tablespaces
  - BSTREAM_IT_DB:         all databases

  The following types of iterators iterate only over items for which
  some meta-data should be saved in the image.

  - BSTREAM_IT_GLOBAL:     all global items in create-dependency order
  - BSTREAM_IT_PERDB:      all per-db items except tables which are enumerated
                           by a table iterator (see below)
  - BSTREAM_IT_PERTABLE:   all per-table items in create-dependency orders.

  @param[in]    hdr             catalog reference
  @param[in]    it_type         iterator type

  @return       pointer to the iterator
    @retval     NULL            error
*/

void*
bcat_iterator_get(struct st_bstream_image_header *hdr, unsigned int it_type)
{
  struct st_backup_catalog      *bup_catalog= (struct st_backup_catalog*) hdr;
  struct st_backup_iterator     *iter;
  DBUG_ENTER("bcat_iterator_get");
  DBUG_ASSERT(hdr);
  DBUG_PRINT("bupstrm", ("getting iterator for type: %u", it_type));

  ERROR_INJECT("bcat_iterator_get_malloc", my_malloc_error_inject= 1;);
  iter= my_malloc(sizeof(struct st_backup_iterator), MYF(MY_WME));
  if (!iter)
  {
    /* Error message reported by mysys. */
    goto end;
  }
  iter->it_index= -1;
  iter->it_type= it_type;

  ERROR_INJECT("bcat_iterator_get_type", it_type= BSTREAM_IT_PERTABLE;);
  switch (it_type) {

    /* purecov: begin inspected */
  case BSTREAM_IT_CHARSET:
    iter->it_array= &bup_catalog->cat_charsets;
    DBUG_PRINT("bupstrm", ("charset"));
    break;

  case BSTREAM_IT_USER:
    iter->it_array= &bup_catalog->cat_users;
    DBUG_PRINT("bupstrm", ("user"));
    goto err;
    /* purecov: end */

  case BSTREAM_IT_DB:
    iter->it_array= &bup_catalog->cat_databases;
    DBUG_PRINT("bupstrm", ("database"));
    break;

    /* purecov: begin inspected */
  case BSTREAM_IT_TABLESPACE:
    iter->it_array= &bup_catalog->cat_tablespaces;
    DBUG_PRINT("bupstrm", ("tablespace"));
    goto err;

  case BSTREAM_IT_GLOBAL:
    errm("iterator type not yet implemented: global\n");
    DBUG_PRINT("bupstrm", ("NOTYET implemented: global"));
    goto err;

  case BSTREAM_IT_PERDB:
    errm("iterator type not yet implemented: perdb\n");
    DBUG_PRINT("bupstrm", ("NOTYET implemented: perdb"));
    goto err;
    /* purecov: end */

  case BSTREAM_IT_PERTABLE:
    errm("iterator type not yet implemented: pertable\n");
    DBUG_PRINT("bupstrm", ("NOTYET implemented: pertable"));
    goto err;

    /* purecov: begin inspected */
  default:
    errm("iterator type not yet implemented: type %u\n", it_type);
    DBUG_PRINT("bupstrm", ("NOTYET implemented: type %u", it_type));
    DBUG_ASSERT(0);
    goto err;
    /* purecov: end */
  }

  goto end;

 err:
  my_free(iter, MYF(0));
  iter= NULL;

 end:
  DBUG_PRINT("bupstrm", ("iterator: 0x%lx", (long) iter));
  DBUG_RETURN((void*) iter);
}


/**
  Return next item pointed by iterator.

  @param[in]    hdr             catalog reference
  @param[in]    iter_arg        iterator reference

  @return       pointer to catalog item
    @retval     NULL            end of items or error
*/

struct st_bstream_item_info*
bcat_iterator_next(struct st_bstream_image_header *hdr __attribute__((unused)),
                   void *iter_arg)
{
  struct st_backup_iterator     *iter= (struct st_backup_iterator*) iter_arg;
  struct st_bstream_item_info   *item;
  DBUG_ENTER("bcat_iterator_next");
  DBUG_PRINT("bupstrm", ("iter: 0x%lx", (long) iter));
  DBUG_ASSERT(iter);
  DBUG_ASSERT(iter->it_array);

  /* Check for end of array. */
  if (++iter->it_index >= (int) iter->it_array->elements)
  {
    /* End of array. Return NULL. This is not an error. */
    DBUG_PRINT("bupstrm", ("end of array, item: NULL"));
    item= NULL;
    goto end;
  }
  /* Note that the array contains pointers only. */
  item= *((struct st_bstream_item_info**)
          dynamic_array_ptr(iter->it_array, iter->it_index));
  DBUG_ASSERT(item); // Never fails, iter->it_index is checked

  DBUG_PRINT("bupstrm", ("next iterator for type: %u", iter->it_type));
  DBUG_PRINT("bupstrm", ("next item: 0x%lx  pos: %lu  type: %d  name: '%.*s'",
                         (long) item, item->pos, item->type,
                         BBLS(&item->name)));

 end:
  DBUG_RETURN(item);
}


/**
  Free iterator resources.

  @param[in]    hdr             catalog reference
  @param[in]    iter_arg        iterator reference

  @note
  The iterator can not be used after call to this function.
*/

void
bcat_iterator_free(struct st_bstream_image_header *hdr __attribute__((unused)),
                   void *iter_arg)
{
  DBUG_ENTER("bcat_iterator_free");
  my_free(iter_arg, MYF(0));
  DBUG_VOID_RETURN;
}


/* purecov: begin deadcode */

/**
  Create iterator for items belonging to a given database.

  @param[in]    hdr             catalog reference
  @param[in]    db              database item reference

  @return       pointer to the iterator
    @retval     NULL            error

  @note Not used when reading a backup stream.
*/

void*
bcat_db_iterator_get(struct st_bstream_image_header *hdr
                     __attribute__((unused)),
                     struct st_bstream_db_info *db
                     __attribute__((unused)))
{
  DBUG_ENTER("bcat_db_iterator_get");
  DBUG_ASSERT(0);
  DBUG_RETURN(NULL);
}


/**
  Return next item from database items iterator

  @param[in]    hdr             catalog reference
  @param[in]    db              database item reference
  @param[in]    iter_arg        iterator reference

  @return       pointer to catalog item
    @retval     NULL            error

  @note Not used when reading a backup stream.
*/

struct st_bstream_dbitem_info*
bcat_db_iterator_next(struct st_bstream_image_header *hdr
                      __attribute__((unused)),
                      struct st_bstream_db_info *db
                      __attribute__((unused)),
                      void *iter_arg
                      __attribute__((unused)))
{
  DBUG_ENTER("bcat_db_iterator_next");
  DBUG_ASSERT(0);
  DBUG_RETURN(NULL);
}


/**
  Free database items iterator resources

  @param[in]    hdr             catalog reference
  @param[in]    db              database item reference
  @param[in]    iter_arg        iterator reference

  @note Not used when reading a backup stream.
*/

void
bcat_db_iterator_free(struct st_bstream_image_header *hdr
                      __attribute__((unused)),
                      struct st_bstream_db_info *db
                      __attribute__((unused)),
                      void *iter_arg
                      __attribute__((unused)))
{
  DBUG_ENTER("bcat_db_iterator_free");
  DBUG_ASSERT(0);
  DBUG_VOID_RETURN;
}


/**
  Produce CREATE statement for a given item.

  Backup stream library calls this function when saving item's
  meta-data. If function successfully produces the statement, it becomes
  part of meta-data.

  @param[in]    hdr             catalog reference
  @param[in]    item            item reference
  @param[out]   query           query string

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.

  @note Not used when reading a backup stream.
*/

int
bcat_get_item_create_query(struct st_bstream_image_header *hdr
                           __attribute__((unused)),
                           struct st_bstream_item_info *item
                           __attribute__((unused)),
                           bstream_blob *query
                           __attribute__((unused)))
{
  DBUG_ENTER("bcat_get_item_create_query");
  DBUG_ASSERT(0);
  DBUG_RETURN(BSTREAM_OK);
}


/**
  Return meta-data (other than CREATE statement) for a given item.

  Backup stream library calls this function when saving item's
  meta-data. If function returns successfully, the bytes returned become
  part of meta-data.

  @param[in]    hdr             catalog reference
  @param[in]    item            item reference
  @param[out]   data            data blob

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.

  @note Not used when reading a backup stream.
*/

int
bcat_get_item_create_data(struct st_bstream_image_header *hdr
                          __attribute__((unused)),
                          struct st_bstream_item_info *item
                          __attribute__((unused)),
                          struct st_blob *data
                          __attribute__((unused)))
{
  DBUG_ENTER("bcat_get_item_create_data");
  DBUG_ASSERT(0);
  DBUG_RETURN(BSTREAM_ERROR);
}

/* purecov: end */


/**
  Create database object from its meta-data.

  When the meta-data section of backup image is read, items can be created
  as their meta-data is read (so that there is no need to store these
  meta-data). But this functions stores them in the catalog instead of
  creating database objects. So the application can make different use
  of the data, e.g. print it.

  @param[in]    hdr             catalog reference
  @param[in]    item            item reference
  @param[in]    query           query string
  @param[in]    data            data blob

  @note The item has set the 'type' and 'pos' elements only. No item
  name is provided. A reference to a database exists for per-db items,
  except of tables.

  @note Either query or data or both can be empty, depending
  on what was stored in the image.

  @note The blob provided by query and/or data is not guaranteed to
  exist after the call. It must be copied to become part of the catalog.

  @return       status
    @retval     BSTREAM_OK      ok
    @retval     otherwise       error

  @note The return value is specified as 'int' in stream_v1.h
  though only values from enum_bstream_ret_codes are expected.
*/

int
bcat_create_item(struct st_bstream_image_header *hdr,
                 struct st_bstream_item_info *item,
                 struct st_blob query,
                 struct st_blob data)
{
  struct st_backup_catalog      *bup_catalog= (struct st_backup_catalog*) hdr;
  struct st_bstream_item_info   *cat_item;
  struct st_backup_metadata     mdata;
  enum enum_bstream_ret_codes   brc= BSTREAM_ERROR;
  DBUG_ENTER("bcat_create_item");
  DBUG_ASSERT(hdr);
  DBUG_ASSERT(item);
  DBUG_PRINT("bupstrm", ("item: 0x%lx  pos: %lu  type: %d  name: '%.*s'",
                         (long) item, item->pos, item->type,
                         BBLS(&item->name)));
  DBUG_PRINT("bupstrm", ("query: '%.*s'", BBLS(&query)));
  DBUG_PRINT("bupstrm", ("data length: %lu", BBL(&data)));

  /*
    Create new st_blob structs. The strings from the
    stream library have a short life time.
  */
  ERROR_INJECT("bcat_create_item_malloc", my_malloc_error_inject= 1;);
  mdata.md_query.begin= (query.begin ?
                         my_memdup(query.begin, BBL(&query), MYF(MY_WME)) :
                         NULL);
  mdata.md_query.end= (mdata.md_query.begin ?
                       mdata.md_query.begin + BBL(&query) :
                       NULL);

  mdata.md_data.begin= (data.begin ?
                        my_memdup(data.begin, BBL(&data), MYF(MY_WME)) :
                        NULL);
  mdata.md_data.end= (mdata.md_data.begin ?
                      mdata.md_data.begin + BBL(&data) :
                      NULL);

  DBUG_PRINT("bupstrm", ("query.begin: 0x%lx  data.begin: 0x%lx",
                         (long) mdata.md_query.begin,
                         (long) mdata.md_data.begin));

  if ((query.begin && !mdata.md_query.begin) ||
      (data.begin && !mdata.md_data.begin))
  {
    /* Memory allocation failed. Error message reported by mysys. */
    goto end;
  }

  ERROR_INJECT("bcat_create_item_user",
               item->type= BSTREAM_IT_USER; item->pos= ULONG_MAX;);
  switch (item->type) {

  case BSTREAM_IT_CHARSET:
  case BSTREAM_IT_USER:
  case BSTREAM_IT_TABLESPACE:
  {
    struct st_backup_global     *bup_global;
    const char                  *typnam;
    DYNAMIC_ARRAY               *array;

    switch (item->type) {
    case BSTREAM_IT_CHARSET:
      /* purecov: begin inspected */
      typnam= "Charset";
      array= &bup_catalog->cat_charsets;
      break;
      /* purecov: end */
    case BSTREAM_IT_USER:
      typnam= "User";
      array= &bup_catalog->cat_users;
      break;
    case BSTREAM_IT_TABLESPACE:
      typnam= "Tablespace";
      array= &bup_catalog->cat_tablespaces;
      break;
    default:
      /* purecov: begin deadcode */
      DBUG_ASSERT(0);
      typnam= NULL; /* Avoid compiler warning: may be used uninitialized */
      array= NULL;  /* Avoid compiler warning: may be used uninitialized */
      goto end;
      /* purecov: end */
    }

    bup_global= backup_locate_global(typnam, array, item->pos);
    if (!bup_global)
    {
      /* Error message reported by function. */
      goto end;
    }
    cat_item= &bup_global->glb_item;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_global);
    /* Copy meta data. */
    bup_global->glb_metadata= mdata;
    DBUG_PRINT("bupstrm", ("Added metadata for %s", typnam));
    break;
  }

  case BSTREAM_IT_DB:
  {
    struct st_backup_database *bup_database;

    bup_database= (struct st_backup_database*)
      backup_locate_global("Database", &bup_catalog->cat_databases, item->pos);
    if (!bup_database)
    {
      /* Error message reported by function. */
      goto end;
    }
    cat_item= &bup_database->db_item.base;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_database);
    /* Copy meta data. */
    bup_database->db_metadata= mdata;
    DBUG_PRINT("bupstrm", ("Added metadata for Database"));
    break;
  }

  case BSTREAM_IT_TABLE:
  {
    struct st_bstream_table_info *item_tbl=
      (struct st_bstream_table_info*) item;
    struct st_backup_table *bup_table;
    DBUG_PRINT("bupstrm", ("table  snap_num: %u  pos: %lu  db_ref: 0x%lx",
                           item_tbl->snap_num, item_tbl->base.base.pos,
                           (long) item_tbl->base.db));
    bup_table= backup_locate_table(bup_catalog, item_tbl->snap_num,
                                   item_tbl->base.base.pos);
    if (!bup_table)
    {
      /* Error message reported by function. */
      goto end;
    }
    cat_item= &bup_table->tbl_item.base.base;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_table);
    /* Copy meta data. */
    bup_table->tbl_metadata= mdata;
    break;
  }

  case BSTREAM_IT_PRIVILEGE:
  case BSTREAM_IT_VIEW:
  case BSTREAM_IT_SPROC:
  case BSTREAM_IT_SFUNC:
  case BSTREAM_IT_EVENT:
  case BSTREAM_IT_TRIGGER:
   {
    struct st_bstream_dbitem_info *item_perdb=
      (struct st_bstream_dbitem_info*) item;
    struct st_backup_perdb *bup_perdb;
    DBUG_PRINT("bupstrm", ("perdb  db_pos: %lu  pos: %lu",
                           item_perdb->db->base.pos, item_perdb->base.pos));
    bup_perdb= backup_locate_perdb(bup_catalog, item_perdb->db->base.pos,
                                   item_perdb->base.pos);
    if (!bup_perdb)
    {
      /* Error message reported by function. */
      goto end;
    }
    cat_item= &bup_perdb->perdb_item.base;
    /* Item must be first in struct. */
    DBUG_ASSERT(cat_item == (struct st_bstream_item_info*) bup_perdb);
    /* Copy meta data. */
    bup_perdb->perdb_metadata= mdata;
    break;
  }

   /* purecov: begin inspected */
  default:
  {
    errm("meta data not yet implemented: unknown type\n");
    DBUG_PRINT("bupstrm", ("NOTYET implemented: unknown type: '%.*s'",
                           BBLS(&item->name)));
    DBUG_ASSERT(0);
    goto end;
  }
    /* purecov: end */
  }

  /* Insert a reference to the item into the image ordered metadata array. */
  ERROR_INJECT("bcat_create_item_ordered_insert", my_malloc_error_inject= 1;);
  DBUG_PRINT("bupstrm", ("image ordered metadata added: 0x%lx",
                         (long) cat_item));
  if (insert_dynamic(&bup_catalog->cat_image_ordered_metadata,
                     (uchar*) &cat_item))
  {
    /*
      If we have a problem here, skip freeing of meta data.
      They are successfully registered with an item already
      and will be freed when the item is freed.
    */
    goto end_nocleanup;
  }

  brc= BSTREAM_OK;

 end:
  if (brc != BSTREAM_OK)
  {
    /* Free what has been allocated. */
    my_free(mdata.md_data.begin, MYF(MY_ALLOW_ZERO_PTR));
    my_free(mdata.md_query.begin, MYF(MY_ALLOW_ZERO_PTR));
  }
 end_nocleanup:
  DBUG_RETURN(brc);
}


/*
  ========================================
  Functions for reading of a backup image.
  ========================================
*/

/**
  Allocate a backup catalog.

  @return       catalog reference
    @retval     NULL            error
*/

struct st_backup_catalog*
backup_catalog_allocate(void)
{
  struct st_backup_catalog      *bup_catalog;
  int                           errpos= 0;
  int                           err;
  DBUG_ENTER("backup_catalog_allocate");

  ERROR_INJECT("backup_catalog_allocate_malloc", my_malloc_error_inject= 1;);
  bup_catalog= my_malloc(sizeof(struct st_backup_catalog),
                         MYF(MY_WME | MY_ZEROFILL));
  if (!bup_catalog)
  {
    /* Error message reported by mysys. */
    goto err;
  }
  errpos= 10;

  {
    DYNAMIC_ARRAY **init_array_p;
    DYNAMIC_ARRAY *init_array_array[]= {
      &bup_catalog->cat_charsets,
      &bup_catalog->cat_users,
      &bup_catalog->cat_tablespaces,
      &bup_catalog->cat_databases,
      &bup_catalog->cat_snapshots,
      &bup_catalog->cat_image_ordered_items,
      &bup_catalog->cat_image_ordered_metadata,
      NULL
    };

    for (init_array_p= init_array_array; *init_array_p; init_array_p++)
    {
      /* Note that the array contains pointers only. */
      err= my_init_dynamic_array(*init_array_p, sizeof(void*),
                                 DYN_ALLOC_INIT, DYN_ALLOC_INCR);
      /* To cover the cleanup loop, don't error on first iteration. */
      ERROR_INJECT("backup_catalog_allocate_array",
                   if (!err && (init_array_p != init_array_array))
                   {err= TRUE; delete_dynamic(*init_array_p);});
      if(err)
      {
        /* Error message reported by mysys. */
        while (init_array_p != init_array_array)
        {
          /* Back to the last successfully initialized array. */
          init_array_p--;
          delete_dynamic(*init_array_p);
        }
        goto err;
      }
    }
  }

  goto end;

 err:
  switch(errpos) {
  case 10:
    my_free(bup_catalog, MYF(0));
    bup_catalog= NULL;
  }

 end:
  DBUG_RETURN(bup_catalog);
}


/**
  Free a backup catalog.

  Part of the data is allocated by the backup stream library.
  This applies mainly to the meta data blobs and the header parts.

  @param[in]    bup_catalog             catalog reference
*/

void
backup_catalog_free(struct st_backup_catalog *bup_catalog)
{
  struct st_bstream_image_header        *hdr= &bup_catalog->cat_header;
  ulong                                 idx;
  ulong                                 jdx;
  DBUG_ENTER("backup_catalog_free");
  DBUG_ASSERT(bup_catalog);

  /* Image ordered references. */
  DBUG_PRINT("bupstrm", ("freeing image ordered references."));
  delete_dynamic(&bup_catalog->cat_image_ordered_metadata);
  delete_dynamic(&bup_catalog->cat_image_ordered_items);

  /* Snapshots. */
  DBUG_PRINT("bupstrm", ("freeing snapshots."));
  for (idx= 0; idx < bup_catalog->cat_snapshots.elements; idx++)
  {
    struct st_backup_snapshot   *bup_snapshot;

    /* Note that the array contains pointers only. */
    bup_snapshot= *((struct st_backup_snapshot**)
                    dynamic_array_ptr(&bup_catalog->cat_snapshots, idx));
    DBUG_ASSERT(bup_snapshot); // Never fails, idx is in range
    /* The referenced items are freed with their databases. */
    delete_dynamic(&bup_snapshot->snap_index_pos_to_table);
    /* Free the item itself. */
    my_free(bup_snapshot, MYF(0));
  }
  delete_dynamic(&bup_catalog->cat_snapshots);

  /* Databases and contained objects. */
  DBUG_PRINT("bupstrm", ("freeing databases and contained objects."));
  for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
  {
    struct st_backup_database   *bup_database;
    struct st_backup_metadata   *mdata;

    bup_database= (struct st_backup_database*)
      backup_locate_global("Database", &bup_catalog->cat_databases, idx);
    if (!bup_database)
    {
      /* Error message reported by function. */
      continue;
    }

    /* Perdb items. */
    DBUG_PRINT("bupstrm", ("freeing perdbs."));
    for (jdx= 0; jdx < bup_database->db_perdbs.elements; jdx++)
    {
      struct st_backup_perdb    *bup_perdb;

      /* Note that the array contains pointers only. */
      bup_perdb= *((struct st_backup_perdb**)
                   dynamic_array_ptr(&bup_database->db_perdbs, jdx));
      DBUG_ASSERT(bup_perdb); // Never fails, jdx is in range
      mdata= &bup_perdb->perdb_metadata;
      my_free(mdata->md_data.begin, MYF(MY_ALLOW_ZERO_PTR));
      my_free(mdata->md_query.begin, MYF(MY_ALLOW_ZERO_PTR));
      my_free(bup_perdb->perdb_item.base.name.begin, MYF(MY_ALLOW_ZERO_PTR));
      /* Free the item itself. */
      my_free(bup_perdb, MYF(0));
    }
    delete_dynamic(&bup_database->db_perdbs);

    /* Tables. */
    DBUG_PRINT("bupstrm", ("freeing tables."));
    for (jdx= 0; jdx < bup_database->db_tables.elements; jdx++)
    {
      struct st_backup_table    *bup_table;

      /* Note that the array contains pointers only. */
      bup_table= *((struct st_backup_table**)
                   dynamic_array_ptr(&bup_database->db_tables, jdx));
      DBUG_ASSERT(bup_table); // Never fails, jdx is in range
      mdata= &bup_table->tbl_metadata;
      my_free(mdata->md_data.begin, MYF(MY_ALLOW_ZERO_PTR));
      my_free(mdata->md_query.begin, MYF(MY_ALLOW_ZERO_PTR));
      my_free(bup_table->tbl_item.base.base.name.begin, MYF(MY_ALLOW_ZERO_PTR));
      /* Free the item itself. */
      my_free(bup_table, MYF(0));
    }
    delete_dynamic(&bup_database->db_tables);

    DBUG_PRINT("bupstrm", ("freeing db meta data."));
    mdata= &bup_database->db_metadata;
    my_free(mdata->md_data.begin, MYF(MY_ALLOW_ZERO_PTR));
    my_free(mdata->md_query.begin, MYF(MY_ALLOW_ZERO_PTR));
    my_free(bup_database->db_item.base.name.begin, MYF(MY_ALLOW_ZERO_PTR));
    /* Free the item itself. */
    my_free(bup_database, MYF(0));
  }
  delete_dynamic(&bup_catalog->cat_databases);

  /* Global objects. */
  {
    DYNAMIC_ARRAY **init_array_p;
    DYNAMIC_ARRAY *init_array_array[]= {
      &bup_catalog->cat_charsets,
      &bup_catalog->cat_users,
      &bup_catalog->cat_tablespaces,
      NULL
    };

    for (init_array_p= init_array_array; *init_array_p; init_array_p++)
    {
      DBUG_PRINT("bupstrm", ("freeing global objects."));
      for (idx= 0; idx < (*init_array_p)->elements; idx++)
      {
        struct st_backup_global     *bup_global;
        struct st_backup_metadata   *mdata;

        /*
          Note that the array contains pointers only.
          It does not fail because idx is always in range.
        */
        bup_global= *((struct st_backup_global**)
                      dynamic_array_ptr(*init_array_p, idx));
        DBUG_ASSERT(bup_global); // Never fails, idx is in range

        DBUG_PRINT("bupstrm", ("freeing meta data."));
        mdata= &bup_global->glb_metadata;
        my_free(mdata->md_data.begin, MYF(MY_ALLOW_ZERO_PTR));
        my_free(mdata->md_query.begin, MYF(MY_ALLOW_ZERO_PTR));
        my_free(bup_global->glb_item.name.begin, MYF(MY_ALLOW_ZERO_PTR));
        /* Free the item itself. */
        my_free(bup_global, MYF(0));
      }
      delete_dynamic(*init_array_p);
    }
  }

  /* Header, snapshots. */
  DBUG_PRINT("bupstrm", ("freeing header, snapshots."));
  DBUG_ASSERT(hdr->snap_count <=
              sizeof(hdr->snapshot) / sizeof(hdr->snapshot[0]));
  for (idx= 0; idx < hdr->snap_count; ++idx)
    bstream_free(hdr->snapshot[idx].engine.name.begin);

  /* Header, binlog group file. */
  DBUG_PRINT("bupstrm", ("freeing header, binlog goup file."));
  if (hdr->binlog_group.file)
    bstream_free(hdr->binlog_group.file); /* purecov: inspected */

  /* Header, binlog file. */
  DBUG_PRINT("bupstrm", ("freeing header, binlog file."));
  if (hdr->binlog_pos.file)
    bstream_free(hdr->binlog_pos.file);

  /* Header, server version. */
  DBUG_PRINT("bupstrm", ("freeing header, server version."));
  if (hdr->server_version.extra.begin)
    bstream_free(hdr->server_version.extra.begin);

  /* Catalog. */
  DBUG_PRINT("bupstrm", ("freeing catalog."));
  my_free(bup_catalog, MYF(0));

  DBUG_VOID_RETURN;
}


/**
  Open a backup image for reading.

  @param[in]    filename                file name
  @param[in]    bup_catalog             catalog reference

  @return       image handle reference
    @retval     NULL                    error
*/

struct st_stream*
backup_image_open(const char *filename, struct st_backup_catalog *bup_catalog)
{
  struct st_stream              *strm;
  enum enum_bstream_ret_codes   brc;
  int                           rc;
  int                           errpos= 0;
  uint                          idx;
  uint                          version;
  DBUG_ENTER("backup_image_open");
  DBUG_ASSERT(filename);
  DBUG_ASSERT(bup_catalog);

  /* Allocate low-level stream info struct. */
  ERROR_INJECT("backup_image_open_malloc", my_malloc_error_inject= 1;);
  strm= my_malloc(sizeof(struct st_stream), MYF(MY_WME));
  if (!strm)
  {
    /* Error message reported by mysys. */
    goto err;
  }
  errpos= 10;

  /*
    Open stream.
  */
  rc= str_open_rd(strm, filename, &version);
  DBUG_PRINT("bupstrm", ("Opened backup image file (rc=%d)", rc));
  if (rc != BSTREAM_OK)
  {
    /* Error message reported by function. */
    goto err;
  }
  strm->stream_pos= STREAM_POS_PREFIX;
  errpos= 20;

  /* Add compression algorithm to header. */
  bup_catalog->cat_zalgo= strm->zalgo;

  /* Add image path to header. */
  bup_catalog->cat_image_path= strm->path;

  /* Add image size to header. */
  bup_catalog->cat_image_size= strm->size;

  /*
    Read backup image stream header.
  */
  rc= bstream_rd_header(&strm->bupstrm, &bup_catalog->cat_header);
  ERROR_INJECT("backup_image_open_header", rc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Read archive header (rc=%d)", rc));
  if ((rc != BSTREAM_OK) && (rc != BSTREAM_EOC))
  {
    errm("error on stream library read of header.\n");
    goto err;
  }
  strm->stream_pos= STREAM_POS_HEADER;

  /* Add image version to header. */
  bup_catalog->cat_header.version= version;
  DBUG_PRINT("bupstrm", ("set header version: %u", version));

  /*
    Now that we know, how many snapshots we have, initialize the
    snapshot pos to table index arrays.
  */
  DBUG_PRINT("bupstrm", ("allocating snapshot indexes: %u",
                         bup_catalog->cat_header.snap_count));
  DBUG_ASSERT(bup_catalog->cat_header.snap_count <=
              sizeof(bup_catalog->cat_header.snapshot) /
              sizeof(bup_catalog->cat_header.snapshot[0]));
  for (idx= 0; idx < bup_catalog->cat_header.snap_count; idx++)
  {
    struct st_backup_snapshot *bup_snapshot;

    /* Allocate memory for the item. */
    ERROR_INJECT("backup_image_open_snapshot_malloc",
                 my_malloc_error_inject= 1;);
    bup_snapshot= (struct st_backup_snapshot*)
      my_malloc(sizeof(struct st_backup_snapshot), MYF(MY_WME));
    if (!bup_snapshot)
    {
      /* Error message reported by mysys. */
      goto err;
    }
    /* Initialize a new index array. */
    rc= my_init_dynamic_array(&bup_snapshot->snap_index_pos_to_table,
                              sizeof(struct st_backup_table*),
                              DYN_ALLOC_INIT, DYN_ALLOC_INCR);
    ERROR_INJECT("backup_image_open_index", if (!rc) {rc= TRUE;
        delete_dynamic(&bup_snapshot->snap_index_pos_to_table);});
    ERROR_INJECT("backup_image_open_index1", if (idx && !rc) {rc= TRUE;
        delete_dynamic(&bup_snapshot->snap_index_pos_to_table);});
    if (rc)
    {
      /* Error message reported by mysys. */
      my_free(bup_snapshot, MYF(0));
      goto err;
    }
    /* Check consistency of array position. */
    DBUG_ASSERT(idx == bup_catalog->cat_snapshots.elements);
    /* Insert in the catalog. */
    ERROR_INJECT("backup_image_open_snapshot_insert",
                 my_malloc_error_inject= 1;);
    rc= insert_dynamic(&bup_catalog->cat_snapshots, (uchar*) &bup_snapshot);
    if (rc)
    {
      /* Error message reported by mysys. */
      delete_dynamic(&bup_snapshot->snap_index_pos_to_table);
      my_free(bup_snapshot, MYF(0));
      goto err;
    }
  }

  /*
    Read catalog requires search of next chunk start.
  */
  brc= bstream_next_chunk(&strm->bupstrm);
  ERROR_INJECT("backup_image_open_next_eos", brc= BSTREAM_EOS;);
  ERROR_INJECT("backup_image_open_next_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Find next chunk (brc=%d)", brc));
  if (brc != BSTREAM_OK)
  {
    if (brc == BSTREAM_EOS)
    {
      errm("end of stream after %s.\n", strm->stream_pos);
      goto err;
    }
    errm("cannot find catalog after %s.\n", strm->stream_pos);
    goto err;
  }

  goto end;

 err:
  switch (errpos) {
  case 20:
    (void) str_close(strm);
  case 10:
    my_free(strm, MYF(MY_ALLOW_ZERO_PTR));
    strm= NULL;
  default:
    ; /* No (further) de-initializations required. */
  }

 end:
  DBUG_RETURN(strm);
}


/**
  Close a backup image.

  @param[in]    strm            image handle reference
*/

enum enum_bstream_ret_codes
backup_image_close(struct st_stream *strm)
{
  enum enum_bstream_ret_codes brc;
  DBUG_ENTER("backup_image_close");
  DBUG_ASSERT(strm);

  brc= str_close(strm);
  my_free(strm, MYF(0));
  DBUG_RETURN(brc);
}


/**
  Read backup image catalog.

  @param[in]    strm                    image handle reference
  @param[in]    bup_catalog             catalog reference

  @return       status
    @retval     BSTREAM_OK              ok
    @retval     != BSTREAM_OK           error
*/

enum enum_bstream_ret_codes
backup_read_catalog(struct st_stream *strm,
                    struct st_backup_catalog *bup_catalog)
{
  enum enum_bstream_ret_codes brc;
  DBUG_ENTER("backup_read_catalog");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(bup_catalog);

  /*
    Read catalog.
  */
  brc= bstream_rd_catalogue(&strm->bupstrm, &bup_catalog->cat_header);
  ERROR_INJECT("backup_read_catalog_rd_eos", brc= BSTREAM_EOS;);
  ERROR_INJECT("backup_read_catalog_rd_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Read archive catalogue (brc=%d)", brc));
  if ((brc != BSTREAM_OK) && (brc != BSTREAM_EOC))
  {
    if (brc == BSTREAM_EOS)
    {
      errm("end of stream within catalog.\n");
      goto end;
    }
    errm("error on stream library read of catalog.\n");
    goto end;
  }
  strm->stream_pos= STREAM_POS_CATALOG;

  /*
    Read meta data requires search of next chunk start.
  */
  brc= bstream_next_chunk(&strm->bupstrm);
  ERROR_INJECT("backup_read_catalog_next_eos", brc= BSTREAM_EOS;);
  ERROR_INJECT("backup_read_catalog_next_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Find next chunk (brc=%d)", brc));
  if (brc != BSTREAM_OK)
  {
    if (brc == BSTREAM_EOS)
    {
      errm("end of stream after %s.\n", strm->stream_pos);
      goto end;
    }
    errm("cannot find meta data after %s.\n", strm->stream_pos);
    goto end;
  }
  brc= BSTREAM_OK;

 end:
  DBUG_RETURN(brc);
}


/**
  Read backup image meta data.

  @param[in]    strm                    image handle reference
  @param[in]    bup_catalog             catalog reference

  @return       status
    @retval     BSTREAM_OK              ok
    @retval     != BSTREAM_OK           error
*/

enum enum_bstream_ret_codes
backup_read_metadata(struct st_stream *strm,
                     struct st_backup_catalog *bup_catalog)
{
  enum enum_bstream_ret_codes brc;
  DBUG_ENTER("backup_read_metadata");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(bup_catalog);

  /*
    Read meta data.
  */
  brc= bstream_rd_meta_data(&strm->bupstrm, &bup_catalog->cat_header);
  ERROR_INJECT("backup_read_metadata_rd_eos", brc= BSTREAM_EOS;);
  ERROR_INJECT("backup_read_metadata_rd_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Read meta data (brc=%d)", brc));
  if ((brc != BSTREAM_OK) && (brc != BSTREAM_EOC))
  {
    if (brc == BSTREAM_EOS)
    {
      errm("end of stream within meta data.\n");
      goto end;
    }
    errm("error on stream library read of meta data.\n");
    goto end;
  }
  strm->stream_pos= STREAM_POS_META_DATA;
  brc= BSTREAM_OK;

  /*
    Before reading the first data chunk, we need to find the next chunk
    start. We must not do it between data chunks.
  */
  brc= bstream_next_chunk(&strm->bupstrm);
  ERROR_INJECT("backup_read_metadata_next_eos", brc= BSTREAM_EOS;);
  ERROR_INJECT("backup_read_metadata_next_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Find next chunk (brc=%d)", brc));
  if (brc != BSTREAM_OK)
  {
    if (brc == BSTREAM_EOS)
    {
      errm("end of stream after %s.\n", strm->stream_pos);
      goto end;
    }
    errm("cannot find next chunk after %s.\n", strm->stream_pos);
    goto end;
  }

 end:
  DBUG_RETURN(brc);
}


/**
  Read backup image table data.

  @param[in]    strm                    image handle reference
  @param[in]    bup_catalog             catalog reference

  @return       status
    @retval     BSTREAM_OK              ok
    @retval     != BSTREAM_OK           error
*/

enum enum_bstream_ret_codes
backup_read_snapshot(struct st_stream *strm,
                     struct st_backup_catalog *bup_catalog
                      __attribute__((unused)),
                     struct st_bstream_data_chunk *snapshot)
{
  enum enum_bstream_ret_codes brc;
  DBUG_ENTER("backup_read_snapshot");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(snapshot);

  /*
    Read data chunk. The memory is allocated by the backup stream library.
  */
  snapshot->data.begin= NULL;
  snapshot->data.end= NULL;
  brc= bstream_rd_data_chunk(&strm->bupstrm, snapshot);
  ERROR_INJECT("backup_read_snapshot_rd_eos", brc= BSTREAM_EOS;);
  ERROR_INJECT("backup_read_snapshot_rd_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Read data chunk (brc=%d)", brc));
  if ((brc != BSTREAM_OK) && (brc != BSTREAM_EOC))
  {
    if (brc == BSTREAM_EOS)
    {
      errm("end of stream after %s.\n", strm->stream_pos);
      goto end;
    }
    errm("error on stream library read of snapshot.\n");
    goto end;
  }
  strm->stream_pos= STREAM_POS_TABLE_DATA;

 end:
  DBUG_RETURN(brc);
}


/**
  Read backup image summary.

  @param[in]    strm                    image handle reference
  @param[in]    bup_catalog             catalog reference

  @return       status
    @retval     BSTREAM_OK              ok
    @retval     != BSTREAM_OK           error
*/

enum enum_bstream_ret_codes
backup_read_summary(struct st_stream *strm,
                    struct st_backup_catalog *bup_catalog)
{
  enum enum_bstream_ret_codes brc;
  DBUG_ENTER("backup_read_summary");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(bup_catalog);

  brc= bstream_rd_summary(&strm->bupstrm, &bup_catalog->cat_header);
  ERROR_INJECT("backup_read_summary_rd_ok", brc= BSTREAM_OK;);
  ERROR_INJECT("backup_read_summary_rd_err", brc= BSTREAM_ERROR;);
  DBUG_PRINT("bupstrm", ("Read summary (brc=%d)", brc));
  if (brc != BSTREAM_EOS)
  {
    if (brc == BSTREAM_ERROR)
      errm("error on stream library read of summary.\n");
    else
      errm("stream not at end after reading summary.\n");
    brc= BSTREAM_ERROR;
    goto end;
  }
  strm->stream_pos= STREAM_POS_SUMMARY;
  brc= BSTREAM_OK;

 end:
  DBUG_RETURN(brc);
}


/*
  ===================
  Catalog navigation.
  ===================
*/

/*
  Catalog items are located by "catalog coordinates". The format of the
  catalog coordinates depends on the type of item. It is specified as
  follows:

  [item position (global)]= [db no.]
  [item position (table)]= [ snap no. ! pos in snapshot's table list ]
  [item position (other per-db item)]= [ pos in db item list ! db no. ]
  [item position (per-table item)] = [ pos in table's item list !
                                       db no. ! table pos ]
*/

/**
  Locate a global object by catalog coordinates.

  Catalog coordinates for global objects are:

      array         object-type specific array in catalog
      pos           position in array

  @param[in]    typnam          object type name
  @param[in]    array           array reference
  @param[in]    pos             position in array

  @return       object reference
*/

struct st_backup_global*
backup_locate_global(const char *typnam, DYNAMIC_ARRAY *array, ulong pos)
{
  struct st_backup_global *bup_global;
  DBUG_ENTER("backup_locate_global");
  DBUG_ASSERT(typnam);
  DBUG_ASSERT(array);
  DBUG_PRINT("bupstrm", ("typename: %s  pos: %lu", typnam, pos));

  /* Check plausibility of array position. */
  ERROR_INJECT("backup_locate_global_pos", pos= array->elements;);
  if (pos >= array->elements)
  {
    errm("non-existent %s position: %lu in catalog.\n", typnam, pos);
    bup_global= NULL;
    goto end;
  }

  /*
    Get a reference to the global object.
    It does not fail because we checked that pos is in range.
    Note that the array contains pointers only.
  */
  bup_global= *((struct st_backup_global**) dynamic_array_ptr(array, pos));
  DBUG_ASSERT(bup_global);
  DBUG_PRINT("bupstrm",
             ("located %s: '%.*s'  item: 0x%lx  pos: %lu", typnam,
              BBLS(&bup_global->glb_item.name), (long) bup_global,
              bup_global->glb_item.pos));

  /*
    Check consistency of array position. It has been checked before
    insert into the array. So it is assumed to be correct here.
  */
  DBUG_ASSERT(bup_global->glb_item.pos == pos);

 end:
  DBUG_RETURN(bup_global);
}


/**
  Locate a table object by catalog coordinates.

  Catalog coordinates for tables are:

      snap_num      snapshot position in catalog
      pos           table position in snapshot

  @param[in]    bup_catalog     catalog reference
  @param[in]    snap_num        position in catalog's snapshot array
  @param[in]    pos             position in snapshot's table index array

  @return       table reference
*/

struct st_backup_table*
backup_locate_table(struct st_backup_catalog *bup_catalog,
                    ulong snap_num, ulong pos)
{
  static struct st_backup_snapshot      *bup_snapshot;
  static struct st_backup_table         *bup_table;
  DBUG_ENTER("backup_locate_table");
  DBUG_ASSERT(bup_catalog);
  DBUG_PRINT("bupstrm", ("snap_num: %lu  pos: %lu", snap_num, pos));

  /* Check plausibility of array position. */
  if (snap_num >= bup_catalog->cat_snapshots.elements)
  {
    /* purecov: begin inspected */
    errm("non-existent snapshot position: %lu in catalog.\n", snap_num);
    bup_table= NULL;
    goto end;
    /* purecov: end */
  }

  /*
    Get a reference to the snapshot object.
    It does not fail because we checked that snap_num is in range.
    Note that the array contains pointers only.
  */
  bup_snapshot= *((struct st_backup_snapshot**)
                  dynamic_array_ptr(&bup_catalog->cat_snapshots, snap_num));
  DBUG_ASSERT(bup_snapshot);

  /* Check plausibility of array position. */
  ERROR_INJECT("backup_locate_table_pos",
               pos= bup_snapshot->snap_index_pos_to_table.elements;);
  if (pos >= bup_snapshot->snap_index_pos_to_table.elements)
  {
    errm("non-existent table position: %lu "
         "in table index of snapshot: %lu\n", pos, snap_num);
    bup_table= NULL;
    goto end;
  }

  /*
    Get a reference to the table object.
    It does not fail because we checked that pos is in range.
    Note that the array contains pointers only.
  */
  bup_table= *((struct st_backup_table**)
               dynamic_array_ptr(&bup_snapshot->snap_index_pos_to_table, pos));
  DBUG_ASSERT(bup_table);
  DBUG_PRINT("bupstrm",
             ("located table: '%.*s'.'%.*s'  "
              "item: 0x%lx  snap_num: %u  pos: %lu",
              BBLS(&bup_table->tbl_item.base.db->base.name),
              BBLS(&bup_table->tbl_item.base.base.name), (long) bup_table,
              bup_table->tbl_item.snap_num, bup_table->tbl_item.base.base.pos));

  /*
    Check consistency of array positions. They have been checked before
    insert into the array. So they are assumed to be correct here.
  */
  DBUG_ASSERT(bup_table->tbl_item.snap_num == snap_num);
  DBUG_ASSERT(bup_table->tbl_item.base.base.pos == pos);

 end:
  DBUG_RETURN(bup_table);
}


/**
  Locate a perdb object by catalog coordinates.

  Catalog coordinates for perdb items are:

      db_pos        database position in catalog
      pos           perdb item position in database

  @param[in]    bup_catalog     catalog reference
  @param[in]    db_pos          position in catalog's database array
  @param[in]    pos             position in databases's perdb array

  @return       perdb item reference
*/

struct st_backup_perdb*
backup_locate_perdb(struct st_backup_catalog *bup_catalog,
                    ulong db_pos, ulong pos)
{
  static struct st_backup_database      *bup_database;
  static struct st_backup_perdb         *bup_perdb;
  DBUG_ENTER("backup_locate_perdb");
  DBUG_ASSERT(bup_catalog);
  DBUG_PRINT("bupstrm", ("db_pos: %lu  pos: %lu", db_pos, pos));

  /* Check plausibility of array position. */
  if (db_pos >= bup_catalog->cat_databases.elements)
  {
    /* purecov: begin inspected */
    errm("non-existent database position: %lu in catalog.\n", db_pos);
    bup_perdb= NULL;
    goto end;
    /* purecov: end */
  }

  /*
    Get a reference to the database object.
    It does not fail because we checked that db_pos is in range.
    Note that the array contains pointers only.
  */
  bup_database= *((struct st_backup_database**)
                  dynamic_array_ptr(&bup_catalog->cat_databases, db_pos));
  DBUG_ASSERT(bup_database);

  /* Check plausibility of array position. */
  ERROR_INJECT("backup_locate_perdb_pos",
               pos= bup_database->db_perdbs.elements;);
  if (pos >= bup_database->db_perdbs.elements)
  {
    errm("non-existent perdb position: %lu in database: '%.*s'\n",
         pos, BBLS(&bup_database->db_item.base.name));
    bup_perdb= NULL;
    goto end;
  }

  /*
    Get a reference to the perdb object.
    It does not fail because we checked that db_pos is in range.
    Note that the array contains pointers only.
  */
  bup_perdb= *((struct st_backup_perdb**)
               dynamic_array_ptr(&bup_database->db_perdbs, pos));
  DBUG_ASSERT(bup_perdb);
  DBUG_PRINT("bupstrm",
             ("located perdb: '%.*s'.'%.*s'  "
              "item: 0x%lx  db_pos: %lu  pos: %lu",
              BBLS(&bup_perdb->perdb_item.db->base.name),
              BBLS(&bup_perdb->perdb_item.base.name), (long) bup_perdb,
              bup_perdb->perdb_item.db->base.pos,
              bup_perdb->perdb_item.base.pos));

  /*
    Check consistency of array positions. They have been checked before
    insert into the array. So they are assumed to be correct here.
  */
  DBUG_ASSERT(bup_perdb->perdb_item.db->base.pos == db_pos);
  DBUG_ASSERT(bup_perdb->perdb_item.base.pos == pos);

 end:
  DBUG_RETURN(bup_perdb);
}


