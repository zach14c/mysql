#include "../mysql_priv.h"
#include "my_dir.h"

#include "backup_stream.h"
#include "stream.h"

#ifdef HAVE_COMPRESS
#define ZBUF_SIZE 65536 // compression I/O buffer size
#endif

const unsigned char backup_magic_bytes[8]=
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

namespace backup {

/**
  Low level write for backup stream library.

  Pointer to this function is stored in @c backup_stream::stream structure
  and then used by other stream library function for physical writing of
  data.

  Performs stream compression if requested.
*/
extern "C" int stream_write(void *instance, bstream_blob *buf, bstream_blob)
{
  int fd;
  int res;

  DBUG_ENTER("backup::stream_write");

  DBUG_ASSERT(instance);
  DBUG_ASSERT(buf);

  fd_stream *s= (fd_stream*)instance;

  fd= s->m_fd;

  DBUG_ASSERT(fd >= 0);

  if (!buf->begin || buf->begin == buf->end)
    DBUG_RETURN(BSTREAM_OK);

  DBUG_ASSERT(buf->end);

  size_t howmuch = buf->end - buf->begin;
#ifdef HAVE_COMPRESS
  if (s->m_with_compression)
  {
    z_stream *zstream= &s->zstream;
    zstream->next_in= buf->begin;
    /*
      uInt is a type defined by zlib. zstream can't process blocks of data
      whose size won't fit into uInt but theoretically buf can hold larger 
      blocks. However, this is a remote possibility. So we assume here that
      the size of buf fits into uInt, check our assumption with an ASSERT and
      do an explicit cast.
    */
    DBUG_ASSERT(howmuch <= ~((uInt)0));
    zstream->avail_in= (uInt)howmuch;
    do
    {
      if (!zstream->avail_out)
      {
        if (my_write(fd, s->zbuf, ZBUF_SIZE, MYF(MY_NABP)))
          DBUG_RETURN(BSTREAM_ERROR);
        zstream->next_out= s->zbuf;
        zstream->avail_out= ZBUF_SIZE;
      }
      if (deflate(zstream, Z_NO_FLUSH) != Z_OK)
        DBUG_RETURN(BSTREAM_ERROR);
    } while (zstream->avail_in);
  }
  else
#endif
  {
    res= my_write(fd, buf->begin, howmuch,
                  MY_NABP /* error if not all bytes written */ );

    if (res)
      DBUG_RETURN(BSTREAM_ERROR);
  }

  s->bytes += howmuch;

  buf->begin= buf->end;
    DBUG_RETURN(BSTREAM_OK);
}

/**
  Low level read for backup stream library.

  Pointer to this function is stored in @c backup_stream::stream structure
  and then used by other stream library function for physical reading of
  data.

  Performs stream decompression if requested.
*/
extern "C" int stream_read(void *instance, bstream_blob *buf, bstream_blob)
{
  int fd;
  size_t howmuch;

  DBUG_ENTER("backup::stream_read");

  DBUG_ASSERT(instance);
  DBUG_ASSERT(buf);

  fd_stream *s= (fd_stream*)instance;

  fd= s->m_fd;

  DBUG_ASSERT(fd >= 0);

  if (!buf->begin || buf->begin == buf->end)
    DBUG_RETURN(BSTREAM_OK);

  DBUG_ASSERT(buf->end);

  howmuch= buf->end - buf->begin;
#ifdef HAVE_COMPRESS
  if (s->m_with_compression)
  {
    int zerr;
    z_stream *zstream= &s->zstream;
    zstream->next_out= buf->begin;
    /*
      Zstream can process in one go a block whose size fits into uInt type.
      If we have more space available in the buffer, we ignore the extra bytes.
    */    
    if (howmuch > ~((uInt)0))
      howmuch= ~((uInt)0);
    zstream->avail_out= (uInt)howmuch;
    do
    {
      if (!zstream->avail_in)
      {
        zstream->avail_in= (uInt) my_read(fd, s->zbuf, ZBUF_SIZE, MYF(0));
        if (zstream->avail_in == (uInt) -1)
          DBUG_RETURN(BSTREAM_ERROR);
        else if (!zstream->avail_in)
          break;
        zstream->next_in= s->zbuf;
      }
      zerr= inflate(zstream, Z_NO_FLUSH);
      if (zerr == Z_STREAM_END)
      {
        howmuch= zstream->next_out - buf->begin;
        break;
      }
      else if (zerr != Z_OK)
        DBUG_RETURN(BSTREAM_ERROR);
      howmuch= zstream->next_out - buf->begin;
    } while (zstream->avail_out);
  }
  else
#endif
  {
    howmuch= my_read(fd, buf->begin, howmuch, MYF(0));
  }

  /*
   How to detect EOF when reading bytes with my_read().

   We assume that my_read(fd, buf, count, MYF(0)) behaves as POSIX read:

   - if it returns -1 then error has been detected.
   - if it returns N>0 then N bytes have been read.
   - if it returns 0 then there are no more bytes in the stream (EOS reached).
  */

  if (howmuch == (size_t) -1)
    DBUG_RETURN(BSTREAM_ERROR);

  if (howmuch == 0)
    DBUG_RETURN(BSTREAM_EOS);

  s->bytes += howmuch;
  buf->begin += howmuch;
  DBUG_RETURN(BSTREAM_OK);
}


Stream::Stream(Logger &log, ::String *backupdir, 
               LEX_STRING orig_loc, int flags)
  :m_flags(flags), m_block_size(0), m_log(log)
{
  prepare_path(backupdir, orig_loc);
  bzero(&stream, sizeof(stream));
  bzero(&buf, sizeof(buf));
  bzero(&mem, sizeof(mem));
  bzero(&data_buf, sizeof(data_buf));
  block_size= 0;
  state= CLOSED;
}

/**
  Prepare path for access.

  This method takes the backupdir and the file name specified on the backup
  command (orig_loc) and forms a combined path + file name as follows:

    1. If orig_loc has a relative path, make it relative to backupdir
    2. If orig_loc has a hard path, use it.
    3. If orig_loc has no path, append to backupdir

  @param[IN]  backupdir  The backupdir system variable value.
  @param[IN]  orig_loc   The path + file name specified in the backup command.

  @returns 0
*/
int Stream::prepare_path(::String *backupdir, 
                         LEX_STRING orig_loc)
{
  char fix_path[FN_REFLEN]; 
  char full_path[FN_REFLEN]; 

  /*
    Prepare the path using the backupdir iff no relative path
    or no hard path included.

    Relative paths are formed from the backupdir system variable.

    Case 1: Backup image file name has relative path. 
            Make relative to backupdir.

    Example BACKUP DATATBASE ... TO '../monthly/dec.bak'
            If backupdir = '/dev/daily' then the
            calculated path becomes
            '/dev/monthly/dec.bak'

    Case 2: Backup image file name has no path or has a subpath. 

    Example BACKUP DATABASE ... TO 'week2.bak'
            If backupdir = '/dev/weekly/' then the
            calculated path becomes
            '/dev/weekly/week2.bak'
    Example BACKUP DATABASE ... TO 'jan/day1.bak'
            If backupdir = '/dev/monthly/' then the
            calculated path becomes
            '/dev/monthly/jan/day1.bak'

    Case 3: Backup image file name has hard path. 

    Example BACKUP DATATBASE ... TO '/dev/dec.bak'
            If backupdir = '/dev/daily/backup' then the
            calculated path becomes
            '/dev/dec.bak'
  */

  /*
    First, we construct the complete path from backupdir.
  */
  fn_format(fix_path, backupdir->ptr(), mysql_real_data_home, "", 
            MY_UNPACK_FILENAME | MY_RELATIVE_PATH);

  /*
    Next, we contruct the full path to the backup file.
  */
  fn_format(full_path, orig_loc.str, fix_path, "", 
            MY_UNPACK_FILENAME | MY_RELATIVE_PATH);

  /*
    Copy result to member variable for Stream class.
  */
  m_path.copy(full_path, strlen(full_path), system_charset_info);
 
  return 0;
}

bool Stream::open()
{
  close();
  m_fd= my_open(m_path.c_ptr(), m_flags, MYF(0));
  return m_fd >= 0;
}

void Stream::close()
{
  if (m_fd >= 0)
  {
    my_close(m_fd, MYF(0));
    m_fd= -1;
  }
}

bool Stream::rewind()
{
#ifdef HAVE_COMPRESS
  /* Compressed stream cannot be rewound */
  if (m_with_compression)
    return FALSE;
#endif
  return m_fd >= 0 && my_seek(m_fd, 0, SEEK_SET, MYF(0)) == 0;
}


Output_stream::Output_stream(Logger &log, ::String *backupdir,
                             LEX_STRING orig_loc,
                             bool with_compression)
  :Stream(log, backupdir, orig_loc, 0)
{
  m_with_compression= with_compression;
  stream.write= stream_write;
  m_block_size=0; // use default block size provided by the backup stram library
}

/**
  Write the magic bytes and format version number at the beginning of a stream.

  Stream should be positioned at its beginning.

  @return Number of bytes written or -1 if error.
*/
int Output_stream::write_magic_and_version()
{
  byte buf[10];
  bstream_blob blob;
  DBUG_ASSERT(m_fd >= 0);

  memmove(buf, backup_magic_bytes, 8);
  // format version = 1
  buf[8]= 0x01;
  buf[9]= 0x00;

  blob.begin= buf;
  blob.end= buf + 10;
  int ret= stream_write((fd_stream*)this, &blob, blob);
  if (ret != BSTREAM_OK)
    return -1; // error when writing magic bytes
  else
    return 10;
}

/**
  Initialize backup stream after the underlying stream has been opened.
 */ 
bool Output_stream::init()
{
  // write magic bytes and format version
  int len= write_magic_and_version();

  if (len <= 0)
  {
    m_log.report_error(ER_BACKUP_WRITE_HEADER);
    return FALSE;
  }

  bytes= 0;

  /*
    The backup stream library uses unsigned long type for storing block size.
    We assume here that the size fits into that type, check that assumption with
    an ASSERT and do an explicit cast from size_t.
  */
  DBUG_ASSERT(m_block_size <= ~((unsigned long)0));
  if (BSTREAM_OK != bstream_open_wr(this, (unsigned long)m_block_size, len))
  {
    m_log.report_error(ER_BACKUP_OPEN_WR);
    return FALSE;
  }

  return TRUE;
}

/**
  Open and initialize backup stream for writing.

  @retval TRUE  operation succeeded
  @retval FALSE operation failed

  @todo Report errors.
*/
bool Output_stream::open()
{
  MY_STAT stat_info;
  close();

  /* Allow to write to existing named pipe */
  if (my_stat(m_path.c_ptr(), &stat_info, MYF(0)) &&
      MY_S_ISFIFO(stat_info.st_mode))
    m_flags= O_WRONLY;
  else
    m_flags= O_WRONLY|O_CREAT|O_EXCL|O_TRUNC;

  bool ret= Stream::open();

  if (!ret)
    return FALSE;

  if (m_with_compression)
  {
#ifdef HAVE_COMPRESS
    int zerr;
    if (!(zbuf= (uchar*) my_malloc(ZBUF_SIZE, MYF(0))))
    {
      m_log.report_error(ER_OUTOFMEMORY, ZBUF_SIZE);
      return FALSE;
    }
    zstream.zalloc= 0;
    zstream.zfree= 0;
    zstream.opaque= 0;
    zstream.msg= 0;
    zstream.next_out= zbuf;
    zstream.avail_out= ZBUF_SIZE;
    if ((zerr= deflateInit2(&zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                            MAX_WBITS + 16, MAX_MEM_LEVEL,
                            Z_DEFAULT_STRATEGY) != Z_OK))
    {
      m_log.report_error(ER_BACKUP_FAILED_TO_INIT_COMPRESSION,
                         zerr, zstream.msg);
      return FALSE;
    }
#else
    m_log.report_error(ER_FEATURE_DISABLED, "compression", "--with-zlib-dir");
    return FALSE;
#endif
  }

  return init();
}

/**
  Close backup stream

  If @c destroy is TRUE, the stream object is deleted.
*/
void Output_stream::close()
{
  if (m_fd < 0)
    return;

  bstream_close(this);
#ifdef HAVE_COMPRESS
  if (m_with_compression)
  {
    int zerr;
    zstream.avail_in= 0;
    zstream.next_in= 0;
    do
    {
      zerr= deflate(&zstream, Z_FINISH);
      if (zerr != Z_STREAM_END && zerr != Z_OK)
      {
        m_log.report_error(ER_GET_ERRMSG, zerr, zstream.msg, "deflate");
        break;
      }
      if (my_write(m_fd, zbuf, ZBUF_SIZE - zstream.avail_out,
                   MYF(MY_NABP)))
      {
        m_log.report_error(ER_GET_ERRMSG, my_errno, "", "my_write");
        break;
      }
      zstream.next_out= zbuf;
      zstream.avail_out= ZBUF_SIZE;
    } while (zerr != Z_STREAM_END);
    if ((zerr= deflateEnd(&zstream)) != Z_OK)
      m_log.report_error(ER_GET_ERRMSG, zerr, zstream.msg, "deflateEnd");
    my_free(zbuf, MYF(0));
  }
#endif
  Stream::close();
}

/**
  Rewind output stream so that it is positioned at its beginning and
  ready for writing new image.

  @retval TRUE  operation succeeded
  @retval FALSE operation failed
*/
bool Output_stream::rewind()
{
  bstream_close(this);

  bool ret= Stream::rewind();

  if (!ret)
    return FALSE;

  return init();
}


Input_stream::Input_stream(Logger &log, ::String *backupdir, 
                           LEX_STRING orig_loc)
  :Stream(log, backupdir, orig_loc, O_RDONLY)
{
  m_with_compression= false;
  stream.read= stream_read;
}

/**
  Check that input stream starts with correct magic bytes and
  version number.

  Stream should be positioned at its beginning.

  @return Number of bytes read or -1 if error.
*/
int Input_stream::check_magic_and_version()
{
  DBUG_ASSERT(m_fd >= 0);

  if (memcmp(m_header_buf, backup_magic_bytes, 8))
    return -1; // wrong magic bytes

  unsigned int ver = m_header_buf[8] + (m_header_buf[9]<<8);

  if (ver != 1)
    return -1; // unsupported format version

  return 10;
}

/**
  Initialize backup stream after the underlying stream has been opened.
 */ 
bool Input_stream::init()
{
  int len= check_magic_and_version();

  if (len <= 0)
  {
    m_log.report_error(ER_BACKUP_BAD_MAGIC);
    return FALSE;
  }

  bytes= 0;

  if (BSTREAM_OK != bstream_open_rd(this, len))
  {
    m_log.report_error(ER_BACKUP_OPEN_RD);
    return FALSE;
  }

  return TRUE;
}

/**
  Open backup stream for reading.

  @details This method can detect and open compressed streams. In that case
  stream is initialized for decompression so that stream_read() function will
  return decompressed data.

  The first 10 bytes in the stream (whether compressed or not) are not
  available for reading with stream_read(). Instead, they are stored in
  m_header_buf member and examined by check_magic_and_version().

  @retval TRUE  operation succeeded
  @retval FALSE operation failed

  @todo Report errors.
*/
bool Input_stream::open()
{
  close();

  bool ret= Stream::open();

  if (!ret)
    return FALSE;

  if (my_read(m_fd, m_header_buf, sizeof(m_header_buf),
              MY_NABP /* error if not all bytes read */ ))
    return FALSE;

#ifdef HAVE_COMPRESS
  if (!memcmp(m_header_buf, "\x1f\x8b\x08", 3))
  {
    int zerr;
    bstream_blob blob;
    if (!(zbuf= (uchar*) my_malloc(ZBUF_SIZE, MYF(0))))
    {
      m_log.report_error(ER_OUTOFMEMORY, ZBUF_SIZE);
      return FALSE;
    }
    zstream.zalloc= 0;
    zstream.zfree= 0;
    zstream.opaque= 0;
    zstream.msg= 0;
    zstream.next_in= zbuf;
    zstream.avail_in= 10;
    memcpy(zbuf, m_header_buf, 10);
    if ((zerr= inflateInit2(&zstream, MAX_WBITS + 16)) != Z_OK)
    {
      m_log.report_error(ER_GET_ERRMSG, zerr, zstream.msg, "inflateInit2");
      my_free(zbuf, MYF(0));
      return FALSE;
    }
    m_with_compression= true;
    blob.begin= m_header_buf;
    blob.end= m_header_buf + 10;
    if (stream_read((fd_stream*) this, &blob, blob) != BSTREAM_OK ||
        blob.begin != blob.end)
      return FALSE;
  }
#endif

  return init();
}

/**
  Close backup stream

  If @c destroy is TRUE, the stream object is deleted.
*/
void Input_stream::close()
{
  if (m_fd < 0)
    return;

  bstream_close(this);
#ifdef HAVE_COMPRESS
  if (m_with_compression)
  {
    int zerr;
    if ((zerr= inflateEnd(&zstream)) != Z_OK)
      m_log.report_error(ER_GET_ERRMSG, zerr, zstream.msg, "inflateEnd");
    my_free(zbuf, (MYF(0)));
  }
#endif
  Stream::close();
}

/**
  Rewind input stream so that it can be read again.

  @retval TRUE  operation succeeded
  @retval FALSE operation failed
*/
bool Input_stream::rewind()
{
  bstream_close(this);

  bool ret= Stream::rewind();

  return ret ? init() : FALSE;
}

/// Move to next chunk in the stream.
int Input_stream::next_chunk()
{
  return bstream_next_chunk(this);
}

} // backup namespace
