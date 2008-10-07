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

struct stream_result
{
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

struct fd_stream: public backup_stream
{
  int m_fd;
  size_t bytes;
  uchar m_header_buf[10];
  bool m_with_compression;
#ifdef HAVE_COMPRESS
  z_stream zstream;
  uchar *zbuf;
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
  virtual void close();
  bool rewind();

  /// Check if stream is opened
  bool is_open() const
  { return m_fd>0; }

  virtual ~Stream()
  { close(); }

 protected:

  Stream(Logger&, ::String *, LEX_STRING, int);

  String  m_path;
  int     m_flags;  ///< flags used when opening the file
  size_t  m_block_size;
  Logger  m_log;

  friend int stream_write(void*, bstream_blob*, bstream_blob);
  friend int stream_read(void*, bstream_blob*, bstream_blob);

private:

  int make_relative_path(char *new_path, 
                         char *orig_loc, 
                         ::String *backupdir);
  int prepare_path(::String *backupdir, 
                   LEX_STRING orig_loc);
  bool test_secure_file_priv_access(char *path);

};

/// Used to write to backup stream.
class Output_stream:
  public Stream
{
 public:

  Output_stream(Logger&, ::String *, LEX_STRING, bool);

  int  open();
  void close();
  bool rewind();

 private:

  int write_magic_and_version();
  bool init();
};

/// Used to read from backup stream.
class Input_stream:
  public Stream
{
 public:

  Input_stream(Logger&, ::String *, LEX_STRING);

  int  open();
  void close();
  bool rewind();

  int next_chunk();

 private:

  int check_magic_and_version();
  bool init();
};


/*
 Wrappers around backup stream functions which perform necessary type conversions.
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

inline
result_t
write_summary(const Image_info &info, Output_stream &s)
{
  const st_bstream_image_header *hdr;

  hdr= static_cast<const st_bstream_image_header*>(&info);
  int ret= bstream_wr_summary(&s, const_cast<st_bstream_image_header*>(hdr));

  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_header(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_header(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_catalog(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_catalogue(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_meta_data(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_meta_data(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_summary(Image_info &info, Input_stream &s)
{
  int ret= bstream_rd_summary(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

} // backup namespace

#endif /*BACKUP_STREAM_H_*/
