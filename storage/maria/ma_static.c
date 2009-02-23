/* Copyright (C) 2006 MySQL AB & MySQL Finland AB & TCX DataKonsult AB,
   2008 - 2009 Sun Microsystems, Inc.

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


/**
  @file
  Static variables for MARIA library. All defined here for easy making of
  a shared library
*/

#ifndef _global_h
#include "maria_def.h"
#include "trnman.h"
#endif

LIST	*maria_open_list=0;
uchar	maria_file_magic[]=
{ (uchar) 254, (uchar) 254, (uchar) 9, '\003', };
uchar	maria_pack_file_magic[]=
{ (uchar) 254, (uchar) 254, (uchar) 10, '\001', };
/* Unique number for this maria instance */
uchar   maria_uuid[MY_UUID_SIZE];
IO_CACHE maria_physical_log; /**< Physical log (used by online backup) */
uint	maria_quick_table_bits=9;
ulong	maria_block_size= MARIA_KEY_BLOCK_LENGTH;
my_bool maria_flush= 0, maria_single_user= 0;
my_bool maria_delay_key_write= 0, maria_page_checksums= 1;
my_bool maria_inited= FALSE;
my_bool maria_in_ha_maria= FALSE; /* If used from ha_maria or not */
/** For insert/delete in the list of Maria open tables */
pthread_mutex_t THR_LOCK_maria;
/** For writing to the Maria logs */
pthread_mutex_t THR_LOCK_maria_log;
#if defined(THREAD) && !defined(DONT_USE_RW_LOCKS)
ulong maria_concurrent_insert= 2;
#else
ulong maria_concurrent_insert= 0;
#endif
my_off_t maria_max_temp_length= MAX_FILE_SIZE;
ulong    maria_bulk_insert_tree_size=8192*1024;
ulong    maria_data_pointer_size= 4;

PAGECACHE maria_pagecache_var;
PAGECACHE *maria_pagecache= &maria_pagecache_var;

PAGECACHE maria_log_pagecache_var;
PAGECACHE *maria_log_pagecache= &maria_log_pagecache_var;
MY_TMPDIR *maria_tmpdir;                        /* Tempdir for redo */
char *maria_data_root;
HASH maria_stored_state;

/**
   @brief when transactionality does not matter we can use this transaction

   Used in external programs like ma_test*, and also internally inside
   libmaria when there is no transaction around and the operation isn't
   transactional (CREATE/DROP/RENAME/OPTIMIZE/REPAIR).
*/
TRN dummy_transaction_object;

/* a WT_RESOURCE_TYPE for transactions waiting on a unique key conflict */
WT_RESOURCE_TYPE ma_rc_dup_unique={ wt_resource_id_memcmp, 0};

/* Enough for comparing if number is zero */
uchar maria_zero_string[]= {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

/*
  read_vec[] is used for converting between P_READ_KEY.. and SEARCH_
  Position is , == , >= , <= , > , <
*/

uint32 maria_read_vec[]=
{
  SEARCH_FIND,                               /* HA_READ_KEY_EXACT */
  SEARCH_FIND | SEARCH_BIGGER,               /* HA_READ_KEY_OR_NEXT */
  SEARCH_FIND | SEARCH_SMALLER,              /* HA_READ_KEY_OR_PREV */
  SEARCH_NO_FIND | SEARCH_BIGGER,            /* HA_READ_AFTER_KEY */
  SEARCH_NO_FIND | SEARCH_SMALLER,	     /* HA_READ_BEFORE_KEY */
  SEARCH_FIND | SEARCH_PART_KEY,	     /* HA_READ_PREFIX */
  SEARCH_LAST,                               /* HA_READ_PREFIX_LAST */
  SEARCH_LAST | SEARCH_SMALLER,              /* HA_READ_PREFIX_LAST_OR_PREV */
  MBR_CONTAIN,                               /* HA_READ_MBR_CONTAIN */
  MBR_INTERSECT,                             /* HA_READ_MBR_INTERSECT */
  MBR_WITHIN,                                /* HA_READ_MBR_WITHIN */
  MBR_DISJOINT,                              /* HA_READ_MBR_DISJOINT */
  MBR_EQUAL                                  /* HA_READ_MBR_EQUAL */
};

uint32 maria_readnext_vec[]=
{
  SEARCH_BIGGER, SEARCH_BIGGER, SEARCH_SMALLER, SEARCH_BIGGER, SEARCH_SMALLER,
  SEARCH_BIGGER, SEARCH_SMALLER, SEARCH_SMALLER
};

static int always_valid(const char *filename __attribute__((unused)))
{
  return 0;
}

int (*maria_test_invalid_symlink)(const char *filename)= always_valid;

/** Hash of all tables for which we want physical logging */
const HASH *ma_log_tables_physical;
/**
  If page changes to the index file should be logged to the physical log.

  @note Changes to the header of the index file of a table in physical
  logging are always logged because the header is not redundant with the data
  file.
*/
my_bool ma_log_index_pages_physical;

/**
  All Maria-specific error messages which may be sent to the user.
  They will be localized (translated) as part of
  http://forge.mysql.com/worklog/task.php?id=2940
  "MySQL plugin interface: error reporting".
  Same order as enum myisam_errors.
*/
const char *maria_error_messages[] =
{
  "online backup impossible with --external-locking",
  "backup archive format has too recent version (%u) (current: %u)"
};

static inline void maria_error_messages_dummy_validator()
{
  compile_time_assert((sizeof(maria_error_messages) /
                       sizeof(maria_error_messages[0])) ==
                      (-MARIA_ERR_LAST-1));
}
