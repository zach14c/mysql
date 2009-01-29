/* Copyright (C) 2000-2007 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

/**
  @file
  Function to display and apply a MyISAM logical or physical log to tables.
*/

#ifndef USE_MY_FUNC
#define USE_MY_FUNC
#endif

#include "myisamdef.h"
#include <my_tree.h>
#include <stdarg.h>
#ifdef HAVE_GETRUSAGE
#include <sys/resource.h>
#endif

/** Human-readable names of commands storable in MyISAM logs */
const char *mi_log_command_name[]=
{"open","write","update","delete","close","extra","lock",
 "delete-all", "write-bytes-to-MYD", "write-bytes-to-MYI", "chsize-MYI",
 /*
   This one is special: it is never in log records, it's just used by
   mi_examine_log() to tell the user that it failed when reopening a table. It
   has to be last before NullS.
 */
 "re-open", NullS};

#define FILENAME(A) (A ? A->show_name : "Unknown")

/** In some cases we do not want to flush the index header in mi_close() */
static my_bool update_index_on_close= TRUE;

struct file_info {
  long process;
  /**
    File descriptor of the table at time of logging: in logical log,
    it's descriptor of data file; in physical log: of index file.
    All log records contain a corresponding descriptor value to indicate the
    table they are about.
  */
  int  filenr;
  int id;
  uint rnd;
  char *name, *show_name;
  uchar *record;
  MI_INFO *isam;
  /**
    If 'isam' is currently closed. A not 'used' file is always 'closed' (why
    open it?). A 'used' file may temporarily be closed because of the max
    open file descriptors limit (but if we later meet a command which wants
    to use this file, we will re-open it).
  */
  my_bool closed;
  /** If this table matches the inclusion rules (or has to be ignored) */
  my_bool used;
  ulong accessed;
};

struct test_if_open_param {
  char * name;
  int max_id;
};

struct st_access_param
{
  ulong min_accessed;
  struct file_info *found;
};

#define NO_FILEPOS HA_OFFSET_ERROR

void mi_examine_log_param_init(MI_EXAMINE_LOG_PARAM *param);
int mi_examine_log(MI_EXAMINE_LOG_PARAM *param);
static int read_string(IO_CACHE *file,uchar* *to,uint length);
static int file_info_compare(void *cmp_arg, void *a,void *b);
static int test_if_open(struct file_info *key,element_count count,
			struct test_if_open_param *param);
static void fix_blob_pointers(MI_INFO *isam,uchar *record);
static int test_when_accessed(struct file_info *key,element_count count,
			      struct st_access_param *access_param);
static void file_info_free(struct file_info *info);
static int close_some_file(TREE *tree);
static int reopen_closed_file(TREE *tree,struct file_info *file_info);
static int find_record_with_key(struct file_info *file_info,uchar *record);
static int mi_close_care_state(MI_INFO *info);
static void printf_log(uint verbose, ulong isamlog_process,
                       my_off_t isamlog_filepos, const char *format,...);
static my_bool cmp_filename(struct file_info *file_info, const char *name);


void mi_examine_log_param_init(MI_EXAMINE_LOG_PARAM *mi_exl)
{
  bzero(mi_exl,sizeof(*mi_exl));
  mi_exl->number_of_commands= (ulong) ~0L;
  mi_exl->record_pos= NO_FILEPOS;
}


/**
  Displays or applies the content of a MyISAM logical or physical log
  to tables.

  Applies either to all tables referenced by the log, or only to a subset
  specified in mi_exl->table_selection_hook.
  If applying the content of the log, this function should be called only
  when all involved tables are closed and cannot be opened by any concurrent
  thread/program. It indeed opens tables and modifies them without locking
  them.
  Is used both by the standalone program myisamlog and by the restore
  code of the MyISAM online backup driver.

  @param  mi_exl           Parameters of the applying

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

int mi_examine_log(MI_EXAMINE_LOG_PARAM *mi_exl)
{
  ulong isamlog_process;
  my_off_t isamlog_filepos;
  uint command, result, files_open, big_numbers;
  ulong access_time,length;
  my_off_t filepos;
  int lock_command,mi_result;
  char isam_file_name[FN_REFLEN], llbuff[21], llbuff2[21];
  uchar head[20], *head_ptr;
  uchar	*buff;
  struct test_if_open_param open_param;
  IO_CACHE cache;
  File log_file;
  FILE *write_file;
  enum ha_extra_function extra_command;
  TREE tree;
  struct file_info file_info,*curr_file_info;
  uint head_len[][2]=
    { { 11, 14 }, { 15, 22 }, { 15, 22 }, { 11, 14 }, { 11, 14 }, { 11, 14 },
      { 11, 14 }, { 11, 14 }, {  9, 16 }, {  9, 16 }, {  7, 12 }  };
  uint has_pid_and_result[]= {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0};
  DBUG_ENTER("mi_examine_log");

  compile_time_assert((sizeof(mi_log_command_name) /
                       sizeof(mi_log_command_name[0]) ==
                       (MI_LOG_END_SENTINEL + 2)) &&
                      (sizeof(has_pid_and_result) /
                       sizeof(has_pid_and_result[0]) ==
                       MI_LOG_END_SENTINEL) &&
                      (sizeof(head_len) / sizeof(head_len[0]) ==
                       MI_LOG_END_SENTINEL) &&
                       (MI_LOG_END_SENTINEL <= MI_LOG_BIG_NUMBERS) &&
                      (sizeof(mi_exl->com_count) /
                       sizeof(mi_exl->com_count[0]) == MI_LOG_END_SENTINEL));
  if ((log_file=my_open(mi_exl->log_filename,O_RDONLY,MYF(MY_WME))) < 0)
    DBUG_RETURN(1);
  write_file=0;
  if (mi_exl->write_filename)
  {
    if (!(write_file=my_fopen(mi_exl->write_filename,O_WRONLY,MYF(MY_WME))))
    {
      my_close(log_file,MYF(0));
      DBUG_RETURN(1);
    }
  }

  init_io_cache(&cache,log_file,0,READ_CACHE,mi_exl->start_offset,0,MYF(0));
  bzero(mi_exl->com_count,sizeof(mi_exl->com_count));
  init_tree(&tree,0,0,sizeof(file_info),(qsort_cmp2) file_info_compare,1,
	    (tree_element_free) file_info_free, NULL);
  (void) init_key_cache(dflt_key_cache,KEY_CACHE_BLOCK_SIZE,KEY_CACHE_SIZE,
                        0, 0);

  files_open=0; access_time=0;
  while (access_time++ != mi_exl->number_of_commands &&
	 !my_b_read(&cache, head, 1))
  {
    isamlog_filepos=my_b_tell(&cache)-1L;
    head_ptr= head;
    command=(uint) head_ptr[0];
    command-= (big_numbers= (command & MI_LOG_BIG_NUMBERS));
    if (big_numbers != 0)
      big_numbers= 1;
    if (my_b_read(&cache, head, head_len[command][big_numbers] - 1))
      goto err;
    if (big_numbers)
    {
      file_info.filenr= mi_uint3korr(head);
      head_ptr+= 3;
    }
    else
    {
      file_info.filenr= mi_uint2korr(head);
      head_ptr+= 2;
    }
    if (has_pid_and_result[command])
    {
      isamlog_process= file_info.process= (long) mi_uint4korr(head_ptr);
      head_ptr+= 4;
      if (!mi_exl->opt_processes)
        file_info.process=0;
      result= mi_uint2korr(head_ptr);
      head_ptr+= 2;
    }
    else
      isamlog_process= file_info.process= result= 0;
    if ((curr_file_info=(struct file_info*) tree_search(&tree, &file_info,
							tree.custom_arg)))
    {
      curr_file_info->accessed=access_time;
      if (mi_exl->update && curr_file_info->used && curr_file_info->closed)
      {
	if (reopen_closed_file(&tree,curr_file_info))
	{
	  command=sizeof(mi_exl->com_count)/sizeof(mi_exl->com_count[0][0])/3;
	  result=0;
	  goto com_err;
	}
        mi_exl->re_open_count++;
      }
    }
    DBUG_PRINT("info",("command: %u curr_file_info: 0x%lx used: %u",
                       command, (ulong)curr_file_info,
                       curr_file_info ? curr_file_info->used : 0));
    /*
      We update our statistic (how many commands issued, per command type),
      if this is a valid command about a file we want to include.
      For MI_LOG_OPEN decision must be postponed, as curr_file_info is
      meaningless for it.
    */
    if ((command <
         sizeof(mi_exl->com_count)/sizeof(mi_exl->com_count[0][0])/3) &&
        (!mi_exl->table_selection_hook ||
         (curr_file_info && curr_file_info->used)) &&
        (((enum myisam_log_commands) command) != MI_LOG_OPEN))
    {
      mi_exl->com_count[command][0]++;
      if (result)
        mi_exl->com_count[command][1]++;
    }
    switch ((enum myisam_log_commands) command) {
    case MI_LOG_OPEN:
      if (curr_file_info)
	printf("\nWarning: %s is opened with same process and filenumber\n"
               "Maybe you should use the -P option ?\n",
	       curr_file_info->show_name);
      file_info.name=0;
      file_info.show_name=0;
      file_info.record=0;
      length= big_numbers ? mi_uint4korr(head_ptr) : mi_uint2korr(head_ptr);
      if (read_string(&cache, (uchar **)&file_info.name, length))
	goto err;
      {
	uint i;
	char *pos,*to;

	/* Fix if old DOS files to new format */
	for (pos=file_info.name; (pos=strchr(pos,'\\')) ; pos++)
	  *pos= '/';

	pos=file_info.name;
	for (i=0 ; i < mi_exl->prefix_remove ; i++)
	{
	  char *next;
	  if (!(next=strchr(pos,'/')))
	    break;
	  pos=next+1;
	}
	to=isam_file_name;
	if (mi_exl->filepath)
	  to=convert_dirname(isam_file_name,mi_exl->filepath,NullS);
	strmov(to,pos);
	fn_ext(isam_file_name)[0]=0;	/* Remove extension */
      }
      open_param.name=file_info.name;
      open_param.max_id=0;
      (void) tree_walk(&tree, (tree_walk_action) test_if_open,
                       (void*) &open_param, left_root_right);
      file_info.id=open_param.max_id+1;
      /*
       * In the line below +10 is added to accomodate '<' and '>' chars
       * plus '\0' at the end, so that there is place for 7 digits.
       * It is improbable that same table can have that many entries in
       * the table cache.
       * The additional space is needed for the sprintf commands two lines
       * below.
       */
      file_info.show_name=my_memdup(isam_file_name,
				    (uint) strlen(isam_file_name)+10,
				    MYF(MY_WME));
      if (file_info.id > 1)
	sprintf(strend(file_info.show_name),"<%d>",file_info.id);
      file_info.closed=1;
      file_info.accessed=access_time;
      file_info.used= !mi_exl->table_selection_hook ||
        ((*(mi_exl->table_selection_hook))(isam_file_name));
      if (mi_exl->update && file_info.used)
      {
	if (files_open >= mi_exl->max_files)
	{
	  if (close_some_file(&tree))
	    goto com_err;
	  files_open--;
	}
        /*
          index may be truncated (if physical logging excluded its pages so
          use HA_OPEN_FOR_REPAIR).
        */
	if (!(file_info.isam= mi_open(isam_file_name, O_RDWR,
                                      HA_OPEN_FOR_REPAIR |
                                      HA_OPEN_WAIT_IF_LOCKED)))
	  goto com_err;
	if (!(file_info.record=my_malloc(file_info.isam->s->base.reclength,
					 MYF(MY_WME))))
	  goto end;
	files_open++;
	file_info.closed=0;
      }
      (void) tree_insert(&tree, (uchar*) &file_info, 0, tree.custom_arg);
      if (file_info.used)
      {
	if (mi_exl->verbose && !mi_exl->record_pos_file)
	  printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                     "%s: open -> %d",file_info.show_name, file_info.filenr);
	mi_exl->com_count[command][0]++;
        /* given how we log MI_LOG_OPEN, "result" is always 0 here */
	if (result)
	  mi_exl->com_count[command][1]++;
      }
      break;
    case MI_LOG_CLOSE:
      if (mi_exl->verbose && !mi_exl->record_pos_file &&
	  (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
	printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                   "%s: %s -> %d",FILENAME(curr_file_info),
                   mi_log_command_name[command],result);
      if (curr_file_info)
      {
	if (!curr_file_info->closed)
	  files_open--;
	(void) tree_delete(&tree, (uchar*) curr_file_info, 0, tree.custom_arg);
      }
      break;
    case MI_LOG_EXTRA:
      length= big_numbers ? mi_uint4korr(head_ptr) : mi_uint2korr(head_ptr);
      DBUG_ASSERT(length == 1);
      if (my_b_read(&cache, head, length))
	goto err;
      extra_command=(enum ha_extra_function) head[0];
      if (mi_exl->verbose && !mi_exl->record_pos_file &&
	  (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
	printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                   "%s: %s(%d) -> %d",FILENAME(curr_file_info),
		   mi_log_command_name[command], (int) extra_command,result);
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
	if (mi_extra(curr_file_info->isam, extra_command, 0) != (int) result)
	{
	  fflush(stdout);
	  fprintf(stderr,
                  "Warning: error %d, expected %d on command %s at %s\n",
                  my_errno,result,mi_log_command_name[command],
                  llstr(isamlog_filepos,llbuff));
	  fflush(stderr);
	}
      }
      break;
    case MI_LOG_DELETE:
      length= big_numbers ? mi_uint4korr(head_ptr) : mi_uint2korr(head_ptr);
      DBUG_ASSERT(length == 8);
      if (my_b_read(&cache, head, length))
	goto err;
      filepos=mi_sizekorr(head);
      if (mi_exl->verbose &&
          (!mi_exl->record_pos_file ||
           ((mi_exl->record_pos == filepos ||
             mi_exl->record_pos == NO_FILEPOS) &&
            !cmp_filename(curr_file_info,mi_exl->record_pos_file))) &&
	  (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
	printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                   "%s: %s at %ld -> %d",FILENAME(curr_file_info),
		   mi_log_command_name[command],(long) filepos,result);
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
	if (mi_rrnd(curr_file_info->isam,curr_file_info->record,filepos))
	{
	  if (!mi_exl->recover)
	    goto com_err;
	  if (mi_exl->verbose)
	    printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                       "error: Didn't find row to delete with mi_rrnd");
	  mi_exl->com_count[command][2]++;		/* Mark error */
	}
	mi_result=mi_delete(curr_file_info->isam,curr_file_info->record);
	if ((mi_result == 0 && result) ||
	    (mi_result && (uint) my_errno != result))
	{
	  if (!mi_exl->recover)
	    goto com_err;
	  if (mi_result)
	    mi_exl->com_count[command][2]++;		/* Mark error */
	  if (mi_exl->verbose)
	    printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                       "error: Got result %d from mi_delete instead of %d",
		       mi_result, result);
	}
      }
      break;
    case MI_LOG_WRITE:
    case MI_LOG_UPDATE:
      if (big_numbers)
      {
        filepos= mi_sizekorr(head_ptr);
        head_ptr+= 8;
        length= mi_uint4korr(head_ptr);
      }
      else
      {
        filepos= mi_uint4korr(head_ptr);
        head_ptr+= 4;
        length= mi_uint2korr(head_ptr);
      }
      buff=0;
      if (read_string(&cache,&buff,length))
	goto err;
      if ((!mi_exl->record_pos_file ||
	  ((mi_exl->record_pos == filepos || mi_exl->record_pos == NO_FILEPOS) &&
	   !cmp_filename(curr_file_info,mi_exl->record_pos_file))) &&
	  (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
      {
	if (write_file &&
	    (my_fwrite(write_file,buff,length,MYF(MY_WAIT_IF_FULL | MY_NABP))))
	  goto end;
	if (mi_exl->verbose)
	  printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                     "%s: %s at %ld, length=%ld -> %d",
		     FILENAME(curr_file_info),
		     mi_log_command_name[command], filepos,length,result);
      }
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
	if (curr_file_info->isam->s->base.blobs)
	  fix_blob_pointers(curr_file_info->isam,buff);
	if ((enum myisam_log_commands) command == MI_LOG_UPDATE)
	{
	  if (mi_rrnd(curr_file_info->isam,curr_file_info->record,filepos))
	  {
	    if (!mi_exl->recover)
	    {
	      result=0;
	      goto com_err;
	    }
	    if (mi_exl->verbose)
	      printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                         "error: Didn't find row to update with mi_rrnd");
	    if (mi_exl->recover == 1 || result ||
		find_record_with_key(curr_file_info,buff))
	    {
	      mi_exl->com_count[command][2]++;		/* Mark error */
	      break;
	    }
	  }
	  mi_result=mi_update(curr_file_info->isam,curr_file_info->record,
			      buff);
	  if ((mi_result == 0 && result) ||
	      (mi_result && (uint) my_errno != result))
	  {
	    if (!mi_exl->recover)
	      goto com_err;
	    if (mi_exl->verbose)
	      printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                         "error: Got result %d from mi_update instead of %d",
			 mi_result, result);
	    if (mi_result)
	      mi_exl->com_count[command][2]++;		/* Mark error */
	  }
	}
	else
	{
	  mi_result=mi_write(curr_file_info->isam,buff);
	  if ((mi_result == 0 && result) ||
	      (mi_result && (uint) my_errno != result))
	  {
	    if (!mi_exl->recover)
	      goto com_err;
	    if (mi_exl->verbose)
	      printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                         "error: Got result %d from mi_write instead of %d",
			 mi_result, result);
	    if (mi_result)
	      mi_exl->com_count[command][2]++;		/* Mark error */
	  }
	  if (!mi_exl->recover && filepos != curr_file_info->isam->lastpos)
	  {
	    printf("error: Wrote at position: %s, should have been %s",
		   llstr(curr_file_info->isam->lastpos,llbuff),
		   llstr(filepos,llbuff2));
	    goto end;
	  }
	}
      }
      my_free(buff,MYF(0));
      break;
    case MI_LOG_WRITE_BYTES_MYI:
    case MI_LOG_WRITE_BYTES_MYD:
      if (big_numbers)
      {
        filepos= mi_sizekorr(head_ptr);
        head_ptr+= 8;
        length= mi_uint4korr(head_ptr);
      }
      else
      {
        filepos= mi_uint4korr(head_ptr);
        head_ptr+= 4;
        length= mi_uint2korr(head_ptr);
      }
      buff=0;
      if (read_string(&cache, &buff, length))
        goto err;
      if ((!mi_exl->record_pos_file ||
           ((mi_exl->record_pos == filepos ||
             mi_exl->record_pos == NO_FILEPOS) &&
            !cmp_filename(curr_file_info,mi_exl->record_pos_file))) &&
          (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
      {
        if (write_file &&
            (my_fwrite(write_file, buff, length,
                       MYF(MY_WAIT_IF_FULL | MY_NABP))))
          goto end;
        if (mi_exl->verbose)
          printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                     "%s: %s at %s, length=%lu -> %d",
                     FILENAME(curr_file_info),
                     mi_log_command_name[command], llstr(filepos,llbuff),
                     length, result);
      }
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
        update_index_on_close= FALSE;
        if (my_pwrite((command == MI_LOG_WRITE_BYTES_MYI) ?
                      curr_file_info->isam->s->kfile :
                      curr_file_info->isam->dfile,
                      buff,length,filepos,MYF(MY_NABP)))
          goto com_err;
      }
      my_free(buff,MYF(0));
      break;
    case MI_LOG_CHSIZE_MYI:
      /* here 'filepos' means new length of file */
      if (big_numbers)
        filepos= mi_sizekorr(head_ptr);
      else
        filepos= mi_uint4korr(head_ptr);
      if ((!mi_exl->record_pos_file ||
           ((mi_exl->record_pos == filepos ||
             mi_exl->record_pos == NO_FILEPOS) &&
            !cmp_filename(curr_file_info, mi_exl->record_pos_file))) &&
          (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
      {
        /* nothing to write to write_file ("length" is 0) */
        if (mi_exl->verbose)
          printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                     "%s: %s at %s -> %d", FILENAME(curr_file_info),
                     mi_log_command_name[command], llstr(filepos,llbuff),
                     result);
      }
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
        update_index_on_close= FALSE;
        if (my_chsize(curr_file_info->isam->s->kfile, filepos,
                      0, MYF(MY_WME)))
          goto com_err;
      }
      break;
    case MI_LOG_LOCK:
      length= big_numbers ? mi_uint4korr(head_ptr) : mi_uint2korr(head_ptr);
      DBUG_ASSERT(length == 4);
      if (my_b_read(&cache, head, length))
	goto err;
      /* remove 'flag' (see mi_lock_database() */
      lock_command= (uchar)(uint4korr(head));
      if (mi_exl->verbose && !mi_exl->record_pos_file &&
	  (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
	printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                   "%s: %s(%d) -> %d",FILENAME(curr_file_info),
		   mi_log_command_name[command],lock_command,result);
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
	if (mi_lock_database(curr_file_info->isam,lock_command) !=
	    (int) result)
	  goto com_err;
      }
      break;
    case MI_LOG_DELETE_ALL:
      if (mi_exl->verbose && !mi_exl->record_pos_file &&
	  (!mi_exl->table_selection_hook ||
           (curr_file_info && curr_file_info->used)))
	printf_log(mi_exl->verbose, isamlog_process, isamlog_filepos,
                   "%s: %s -> %d",FILENAME(curr_file_info),
		   mi_log_command_name[command],result);
      if (mi_exl->update && curr_file_info && !curr_file_info->closed)
      {
	if (mi_delete_all_rows(curr_file_info->isam) != (int) result)
	  goto com_err;
      }
      break;
    default:
      fflush(stdout);
      fprintf(stderr, "Error: found unknown command %d in logfile, aborted\n",
              command);
      fflush(stderr);
      goto end;
    }
  }
  end_key_cache(dflt_key_cache,1);
  delete_tree(&tree);
  (void) end_io_cache(&cache);
  (void) my_close(log_file,MYF(0));
  if (write_file && my_fclose(write_file,MYF(MY_WME)))
    DBUG_RETURN(1);
  DBUG_RETURN(0);

 err:
  fflush(stdout);
  fprintf(stderr,"Got error %d when reading from logfile\n",my_errno);
  fflush(stderr);
  goto end;
 com_err:
  fflush(stdout);
  fprintf(stderr,"Got error %d, expected %d on command %s at %s\n",
          my_errno,result,mi_log_command_name[command],
          llstr(isamlog_filepos,llbuff));
  fflush(stderr);
 end:
  end_key_cache(dflt_key_cache, 1);
  delete_tree(&tree);
  (void) end_io_cache(&cache);
  (void) my_close(log_file,MYF(0));
  if (write_file)
    (void) my_fclose(write_file,MYF(MY_WME));
  DBUG_RETURN(1);
}


static int read_string(IO_CACHE *file, register uchar* *to,
                       register uint length)
{
  DBUG_ENTER("read_string");

  if (*to)
    my_free((uchar*) *to,MYF(0));
  if (!(*to= (uchar*) my_malloc(length+1,MYF(MY_WME))) ||
      my_b_read(file, *to,length))
  {
    if (*to)
      my_free(*to,MYF(0));
    *to= 0;
    DBUG_RETURN(1);
  }
  *((char*) *to+length)= '\0';
  DBUG_RETURN (0);
}				/* read_string */


static int file_info_compare(void* cmp_arg __attribute__((unused)),
			     void *a, void *b)
{
  long lint;

  if ((lint=((struct file_info*) a)->process -
       ((struct file_info*) b)->process))
    return lint < 0L ? -1 : 1;
  return ((struct file_info*) a)->filenr - ((struct file_info*) b)->filenr;
}

	/* ARGSUSED */

static int test_if_open (struct file_info *key,
			 element_count count __attribute__((unused)),
			 struct test_if_open_param *param)
{
  if (!strcmp(key->name,param->name) && key->id > param->max_id)
    param->max_id=key->id;
  return 0;
}


static void fix_blob_pointers(MI_INFO *info, uchar *record)
{
  uchar *pos;
  MI_BLOB *blob,*end;

  pos=record+info->s->base.reclength;
  for (end=info->blobs+info->s->base.blobs, blob= info->blobs;
       blob != end ;
       blob++)
  {
    memcpy_fixed(record+blob->offset+blob->pack_length,&pos,sizeof(char*));
    pos+=_mi_calc_blob_length(blob->pack_length,record+blob->offset);
  }
}

	/* close the file with hasn't been accessed for the longest time */
	/* ARGSUSED */

static int test_when_accessed (struct file_info *key,
			       element_count count __attribute__((unused)),
			       struct st_access_param *access_param)
{
  if (key->accessed < access_param->min_accessed && ! key->closed)
  {
    access_param->min_accessed=key->accessed;
    access_param->found=key;
  }
  return 0;
}


static void file_info_free(struct file_info *fileinfo)
{
  DBUG_ENTER("file_info_free");
  /* The 2 conditions below can be true only if 'update' */
  if (!fileinfo->closed)
    (void) mi_close_care_state(fileinfo->isam);
  if (fileinfo->record)
    my_free(fileinfo->record,MYF(0));
  my_free(fileinfo->name,MYF(0));
  my_free(fileinfo->show_name,MYF(0));
  DBUG_VOID_RETURN;
}



static int close_some_file(TREE *tree)
{
  struct st_access_param access_param;

  access_param.min_accessed=LONG_MAX;
  access_param.found=0;

  (void) tree_walk(tree,(tree_walk_action) test_when_accessed,
                   (void*) &access_param,left_root_right);
  if (!access_param.found)
    return 1;			/* No open file that is possibly to close */
  if (mi_close_care_state(access_param.found->isam))
    return 1;
  access_param.found->closed=1;
  return 0;
}


static int reopen_closed_file(TREE *tree, struct file_info *fileinfo)
{
  char name[FN_REFLEN];
  if (close_some_file(tree))
    return 1;				/* No file to close */
  strmov(name,fileinfo->show_name);
  if (fileinfo->id > 1)
    *strrchr(name,'<')='\0';		/* Remove "<id>" */

  if (!(fileinfo->isam= mi_open(name, O_RDWR,
                                HA_OPEN_FOR_REPAIR | HA_OPEN_WAIT_IF_LOCKED)))
    return 1;
  fileinfo->closed=0;
  return 0;
}

	/* Try to find record with uniq key */

static int find_record_with_key(struct file_info *file_info, uchar *record)
{
  uint key;
  MI_INFO *info=file_info->isam;
  uchar tmp_key[HA_MAX_KEY_BUFF];

  for (key=0 ; key < info->s->base.keys ; key++)
  {
    if (mi_is_key_active(info->s->state.key_map, key) &&
	info->s->keyinfo[key].flag & HA_NOSAME)
    {
      (void) _mi_make_key(info, key, tmp_key, record, 0L);
      return mi_rkey(info,file_info->record,(int) key,tmp_key,0,
		     HA_READ_KEY_EXACT);
    }
  }
  return 1;
}


/**
  In practice this is only called if verbose>=1. When mi_examine_log() is
  used in the server it is with verbose==0 so this is not called.
*/

static void printf_log(uint verbose, ulong isamlog_process,
                       my_off_t isamlog_filepos, const char *format,...)
{
  char llbuff[21];
  va_list args;
  va_start(args,format);
  DBUG_ASSERT(verbose > 0);
  if (verbose > 2)
    printf("%9s:",llstr(isamlog_filepos,llbuff));
  if (verbose > 1)
    printf("%5ld ",isamlog_process);	/* Write process number */
  (void) vprintf((char*) format,args);
  putchar('\n');
  va_end(args);
}


static my_bool cmp_filename(struct file_info *file_info, const char *name)
{
  if (!file_info)
    return 1;
  return strcmp(file_info->name,name) ? 1 : 0;
}


/**
  Closes a table but, if physical log, updates the share from disk first.

  mi_close() calls mi_state_info_write() if the table is corrupted.
  This can happen for example is the table is from an online backup which
  made a copy of its data file and only its index' header.
  But in that case, if we have executed some MI_LOG_WRITE_BYTES_MYI commands,
  the state in memory is older than the state on disk, so we update the
  share from disk.

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int mi_close_care_state(MI_INFO *info)
{
  if (!update_index_on_close)
  {
    MYISAM_SHARE *share;

    share= info->s;
    (void) mi_state_info_read_dsk(share->kfile, &share->state, 1, 1);
  }
  return mi_close(info);
}
