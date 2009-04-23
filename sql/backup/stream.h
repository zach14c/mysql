#ifndef _BACKUP_STREAM_H_
#define _BACKUP_STREAM_H_

#include <backup_stream.h>

#include <backup/api_types.h>    // for Buffer definition
#include <backup/image_info.h>
#include <backup/logger.h>
#ifdef HAVE_COMPRESS
#include <zlib.h>
#endif

/**
  @file

 Interface layer between backup kernel and the backup stream library defining
 format of the data written/read.

*/

/************************************************************

  Backup Stream Interface

  The stream is organized as a sequence of chunks each of which
  can have different length. When stream is read chunk boundaries
  are detected. If this happens, next_chunk() member must be called
  in order to access data in next chunk. When writing to a stream,
  data is appended to the current chunk. End_chunk() member closes
  the current chunk and starts a new one.

 ************************************************************/

namespace backup {

/// Structure for storing stream results.
struct stream_result
{
  /// Enumeration of stream result values.
  enum value {
    OK= BSTREAM_OK,
    EOC= BSTREAM_EOC,
    EOS= BSTREAM_EOS,
    ERROR= BSTREAM_ERROR
  };
};


extern "C" int stream_write(void *instance, bstream_blob *buf, bstream_blob);
extern "C" int stream_read(void *instance, bstream_blob *buf, bstream_blob);

/****************************************************

   Definitions of input and output backup streams

 ****************************************************/

/// Structure for storing information about the file stream.
struct fd_stream: public backup_stream
{
  int m_fd;                 ///< file descriptor
  size_t bytes;             ///< bytes read
  uchar m_header_buf[10];   ///< header buffer
  bool m_with_compression;  ///< switch to use compression
#ifdef HAVE_COMPRESS
  z_stream zstream;         ///< the compression stream
  uchar *zbuf;              ///< compression buffer
#endif
  
  fd_stream() :m_fd(-1), bytes(0) {}
};

/**
  Base for @c Output_stream and @c Input_stream.

  It stores file descriptor and provides basic methods for operating on
  it. It also inherits from (and correctly fills) the backup_stream structure
  so that an instance of @c Stream class can be passed to backup stream library
  functions.
*/
class Stream: public fd_stream
{
public:

  int open();
  virtual bool close();

  bool rewind();

  /// Check if stream is opened
  bool is_open() const
  { return m_fd>0; }

  virtual ~Stream()
  { close(); }

protected:

  /// Constructor
  Stream(Logger&, ::String *, int);

  ::String  *m_path;    ///< path for file
  int     m_flags;      ///< flags used when opening the file
  size_t  m_block_size; ///< block size for data stream
  Logger&  m_log;       ///< reference to logger class

  /// Create or open file
  virtual File get_file() = 0; 

  friend int stream_write(void*, bstream_blob*, bstream_blob);
  friend int stream_read(void*, bstream_blob*, bstream_blob);

private:

  bool test_secure_file_priv_access(char *path);

};

/// Used to write to backup stream.
class Output_stream: public Stream
{
public:

  /// Constructor
  Output_stream(Logger&, ::String *, bool);

  int open();
  bool close();
  bool rewind();

protected:

  virtual File get_file();

private:

  int write_magic_and_version();
  bool init();
};

/// Used to read from backup stream.
class Input_stream: public Stream
{
public:

  /// Constructor
  Input_stream(Logger&, ::String *);

  int open();
  bool close();
  bool rewind();

  int next_chunk();

protected:

  virtual File get_file();

private:

  int check_magic_and_version();
  bool init();
};


/*
 Wrappers around backup stream functions which perform necessary type conversions.
*/

/**
  Write the preamble.

  @param[in]  info  The image info.
  @param[in]  s     The output stream.

  @retval  ERROR if stream error, OK if no errors.
*/
inline
result_t
write_preamble(const Image_info &info, Output_stream &s)
{
  const st_bstream_image_header *hdr;

  hdr= static_cast<const st_bstream_image_header*>(&info);
  int ret= bstream_wr_preamble(&s, const_cast<st_bstream_image_header*>(hdr));

  return ret == BSTREAM_ERROR ? ERROR : OK;
}

/**
  Write the summary.

  @param[in]  info  The image info.
  @param[in]  s     The output stream.

  @retval  ERROR if stream error, OK if no errors.
*/
inline
result_t
write_summary(const Image_info &info, Output_stream &s)
{
  const st_bstream_image_header *hdr;

  hdr= static_cast<const st_bstream_image_header*>(&info);
  int ret= bstream_wr_summary(&s, const_cast<st_bstream_image_header*>(hdr));

  return ret == BSTREAM_ERROR ? ERROR : OK;
}

/**
  Read the header.

  @param[in]  info  The image info.
  @param[in]  s     The input stream.

  @retval  ERROR if stream error, OK if no errors.
*/
inline
result_t
read_header(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_header(&s, static_cast<st_bstream_image_header*>(&info));
  DBUG_EXECUTE_IF("restore_read_header", ret= BSTREAM_ERROR;);
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

/**
  Read the catalogue.

  @param[in]  info  The image info.
  @param[in]  s     The input stream.

  @retval  ERROR if stream error, OK if no errors.
*/
inline
result_t
read_catalog(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_catalogue(&s, static_cast<st_bstream_image_header*>(&info));
  DBUG_EXECUTE_IF("restore_read_catalog", ret= BSTREAM_ERROR;);
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

/**
  Read the metadata.

  @param[in]  thd   Connection thread handle. 
  @param[in]  info  The image info.
  @param[in]  s     The input stream.

  @retval  ERROR if stream error, OK if no errors.

  FIXME: the thd parameter for read_meta_data() is here only because the
  temporary code within the function needs it. It should be removed once the
  issue is fixed (see BUG#41294).
*/
inline
result_t
read_meta_data(THD *thd, Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_meta_data(&s, static_cast<st_bstream_image_header*>(&info));

  /* 
    FIXME: the following code is here because object services doesn't clean the
    statement execution context properly, which leads to assertion failure.
    It should be fixed inside object services implementation and then the
    following line should be removed (see BUG#41294).
   */
  DBUG_ASSERT(thd);
  close_thread_tables(thd);                   // Never errors
  if (ret != BSTREAM_ERROR)
    thd->clear_error();                       // Never errors  
  /* end of temporary code */

  DBUG_EXECUTE_IF("restore_read_meta_data", ret= BSTREAM_ERROR;);
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

/**
  Read the summary data.

  @param[in]  info  The image info.
  @param[in]  s     The input stream.

  @retval  ERROR if stream error, OK if no errors.
*/
inline
result_t
read_summary(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_summary(&s, static_cast<st_bstream_image_header*>(&info));
  DBUG_EXECUTE_IF("restore_read_summary",ret= BSTREAM_ERROR;);
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

} // backup namespace

#endif /*BACKUP_STREAM_H_*/
