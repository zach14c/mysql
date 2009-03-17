#ifndef _BACKUP_AUX_H
#define _BACKUP_AUX_H

/** 
  @file
 
  @brief Auxiliary declarations used in online backup code.

*/ 

/// Definition for storage engine reference type.
typedef st_plugin_int* storage_engine_ref;

// Macro which transforms plugin_ref to storage_engine_ref
#ifdef DBUG_OFF
/// Macro to map plugin_ref to se_ref
#define plugin_ref_to_se_ref(A) (A)
/// Macro to map se_ref ro plugin_ref
#define se_ref_to_plugin_ref(A) (A)
#else
/// Macro to map plugin_ref to se_ref
#define plugin_ref_to_se_ref(A) ((A) ? *(A) : NULL)
/// Macro to map se_ref ro plugin_ref
#define se_ref_to_plugin_ref(A) &(A)
#endif

/// Return the storage engine name.
inline
const char* se_name(storage_engine_ref se)
{ return se->name.str; }

/// Return the version of the plugin.
inline
uint se_version(storage_engine_ref se)
{ return se->plugin->version; }  // Q: Or, should it be A->plugin_dl->version?

/// Return the pointer to the handlerton.
inline
handlerton* se_hton(storage_engine_ref se)
{ return (handlerton*)(se->data); }

/// Return a storage engine reference by name.
inline
storage_engine_ref get_se_by_name(const LEX_STRING name)
{ 
  plugin_ref plugin= ::ha_resolve_by_name(::current_thd, &name);
  return plugin_ref_to_se_ref(plugin); 
}


namespace backup {

/**
  Local version of LEX_STRING structure.

  Defines various constructors for convenience.
 */
struct LEX_STRING: public ::LEX_STRING
{
  /// Base constructor for null string.
  LEX_STRING()
  {
    str= NULL;
    length= 0;
  }

  /** 
    Constructor for LEX_STRING class.

    @param[in]  s  LEX_STRING string.
  */
  LEX_STRING(const ::LEX_STRING &s)
  {
    str= s.str;
    length= s.length;
  }

  /** 
    Constructor for LEX_STRING class.

    @param[in]  s  Character class.
  */
  LEX_STRING(const char *s)
  {
    str= const_cast<char*>(s);
    length= strlen(s);
  }

  /** 
    Constructor for LEX_STRING class.

    @param[in]  s  String class.
  */
  LEX_STRING(const String &s)
  {
    str= const_cast<char*>(s.ptr());
    length= s.length();
  }

  /**
     Constructor for the string class.

     This constructor takes a begging and ending pointer to construct the
     string.

     @param[in]  begin  Pointer to start of string.
     @param[in]  end    Pointer to end of string.
  */
  LEX_STRING(byte *begin, byte *end)
  {
    str= (char*)begin;
    if( begin && end > begin)
      length= end - begin;
    else
      length= 0;
  }
};

/**
  Local version of String class.

  Defines various constructors for convenience.
 */
class String: public ::String
{
public:

  /** 
    Constructor for string class.

    @param[in]  s  String class.
  */
  String(const ::String &s) : ::String(s)
  {}

  /** 
    Constructor for string class.

    @param[in]  s  LEX_STRING string.
  */
  String(const ::LEX_STRING &s)
    : ::String(s.str, (uint32)s.length, &::my_charset_bin)
  {
    // Check that string fits.
    DBUG_ASSERT(s.length <= ~((uint32)0));
  }

  /**
     Constructor 

     This constructor takes a begging and ending pointer to construct the
     string.

     @param[in]  begin  Pointer to start of string.
     @param[in]  end    Pointer to end of string.
  */
  String(byte *begin, byte *end)
    : ::String((char*)begin, (uint32)(end - begin), &::my_charset_bin)
  {
    // Check that string length is correct.
    DBUG_ASSERT(begin <= end);
    /* 
      This complex expression checks that the pointer difference fits into
      uint32 type reagardless of the size of pointer type and without generating
      compiler warnings (hopefully).
      
      The idea is to check that in the difference (Which is positive) no bits
      beyond the ones used by unit32 type are set.
    */
    DBUG_ASSERT(!((size_t)(end - begin) & ~((size_t)~((uint32)0))));

    if (!begin)
     set((char*)NULL, 0, NULL); // Note: explicit cast is needed to disambiguate.
  }

  /** 
    Constructor for string class.

    @param[in]  s  Character string.
  */
  String(const char *s)
    : ::String(s, &::my_charset_bin)
  {}

  /// Base constructor for null string class.
  String() : ::String()
  {}
};

/**
  Set a table into a TABLE_LIST.

  @param[in]  tl         The table list.
  @param[in]  tbl        A table for the list.
  @param[in]  lock_type  The lock type.
  @param[in]  mem        Pointer to base memory.

  @retval  TABLE_LIST
*/
inline
int set_table_list(TABLE_LIST &tl, const Table_ref &tbl,
                   thr_lock_type lock_type, MEM_ROOT *mem)
{
  DBUG_ASSERT(mem);

  tl.alias= tl.table_name= const_cast<char*>(tbl.name().ptr());
  tl.db= const_cast<char*>(tbl.db().name().ptr());
  tl.lock_type= lock_type;

  tl.mdl_request= MDL_request::create(0, tl.db, tl.table_name, mem);
  if (!tl.mdl_request)
  {
    return 1;
  }
  return 0;
}

/**
  Build a new TABLE_LIST.

  @param[in]  tbl        A table for the list.
  @param[in]  lock_type  The lock type.
  @param[in]  mem        Pointer to base memory.

  @retval  TABLE_LIST
*/
inline
TABLE_LIST* mk_table_list(const Table_ref &tbl, thr_lock_type lock_type, 
                          MEM_ROOT *mem)
{
  DBUG_ASSERT(mem);

  TABLE_LIST *ptr= (TABLE_LIST*)alloc_root(mem, sizeof(TABLE_LIST));

  if (!ptr)
     return NULL;

  bzero(ptr, sizeof(TABLE_LIST));
  if (set_table_list(*ptr, tbl, lock_type, mem)) // Failed to allocate lock
  {
    return NULL;
  }

  return ptr;
}

/**
  Link two table lists together.

  @param[in] tl   The list to append onto.
  @param[in] next The list to append.
  
  @returns New list pointer.
*/
inline
TABLE_LIST* link_table_list(TABLE_LIST &tl, TABLE_LIST *next)
{
  tl.next_global= tl.next_local= tl.next_name_resolution_table= next;
  return &tl;
}

TABLE_LIST *build_table_list(const Table_list &tables, thr_lock_type lock);
void free_table_list(TABLE_LIST* tables);

} // backup namespace

/**
  Implements a dynamic map from A to B* (also known as hash array).
  
  An instance @c map of class @c Map<A,B> can store mappings from values of 
  type @c A to pointers of type @c B*. Such mappings are added with
  @code
   A a;
   B *ptr;
   
   map.insert(a,ptr);
  @endcode
  
  Later, one can examine the pointer assigned to a given value using operator[]
  @code
   A a
   B *ptr= map[a]
  @endcode
  
  If no mapping for the value a was defined, then returned pointer is NULL. 
  
  In case type @c A is @c int we obtain a dynamic array where pointers are 
  stored at indicated positions.
  @code
  
  Map<uint,B> map;
  
  B x,y;
  
  map.insert(1,&x);
  map.insert(7,&y);
  
  B *p1= map[1];  // p1 points at x
  B *p2= map[2];  // p2 is NULL
  @endcode
  
  However, it is also possible to have the pointers indexed by more complex
  values.

  @note We assume that type A has (fast) copy constructor.
 */ 
template<class A, class B>
class Map
{
  HASH m_hash;
  
public:

  /// Constructor
  Map(size_t);
  ~Map();

  int insert(const A&, B*);
  B* operator[](const A&) const;
  
private:
 
  struct Node;
};

/*****************************************************************
 
  Implementation of Map template using HASH type.
 
 *****************************************************************/

/// Nodes inserted into HASH table
template<class A, class B>
struct Map<A,B>::Node
{
  /* 
    Note: key member must be first for correct key offset value in HASH 
    initialization.
   */
  A key;   ///< key or index in hash/map.
  B *ptr;  ///< pointer to item in hash/map.

  /// Constructor
  Node(const A &a, B *b) :key(a), ptr(b) {}
  
  /**
    Delete the node by key.

    @param[in]  node  The node to delete.
  */
  static void del_key(void *node)
  { delete (Node*) node; }
};

template<class A, class B>
inline
Map<A,B>::Map(size_t init_size)
{
  my_hash_init(&m_hash, &::my_charset_bin, init_size,
               0, sizeof(A), NULL, Node::del_key, MYF(0));
}

template<class A, class B>
inline
Map<A,B>::~Map()
{
  my_hash_free(&m_hash);
}

/** 
  Insert new mapping.

  @todo Consider using mem_root for allocating hash nodes.
 */
template<class A, class B>
inline
int Map<A,B>::insert(const A &a, B *b)
{
  Node *n= new Node(a, b); // TODO: use mem root (?)

  return my_hash_insert(&m_hash, (uchar*) n);
}

/// Get pointer corresponding to the given value.
template<class A, class B>
inline
B* Map<A,B>::operator[](const A &a) const
{
  Node *n= (Node*) my_hash_search(&m_hash, (uchar*) &a, sizeof(A));
  
  return n ? n->ptr : NULL;
}


/**
  Specialization of Map template with integer indexes implemented as a 
  Dynamic_array.
 */ 
template<class T>
class Map<uint,T>: public ::Dynamic_array< T* >
{
  typedef Dynamic_array< T* > Base;
  
public:

  /// Constructor
  Map(uint init_size, uint increment);

  /**
    The index operator.

    @param[in]  pos  Position to retrieve.

    @returns The item and position @c pos.
  */
  T* operator[](ulong pos) const;
  /**
    Insert an item in the map.

    @param[in]  pos  Desired position.
    @param[in]  ptr  Item to insert. 

    @retval  Status of insert.
  */
  int insert(ulong pos, T *ptr); 
  ulong count() const;

private:

   void clear_free_space();
};

template<class T>
inline
Map<uint,T>::Map(uint init_size, uint increment) :Base(init_size, increment)
{
  clear_free_space();
}

template<class T>
inline
void Map<uint,T>::clear_free_space()
{
   DYNAMIC_ARRAY *array= &this->array;
   uchar *start= dynamic_array_ptr(array, array->elements);
   uchar *end= dynamic_array_ptr(array, array->max_element);
   if (end > start)
     bzero(start, end - start);
}

template<class T>
inline
int Map<uint,T>::insert(ulong pos, T *ptr)
{
  uchar *entry;
  DYNAMIC_ARRAY *array= &this->array;

  while (pos >= Base::array.max_element)
  {
    entry= alloc_dynamic(array);
    if (!entry)
     break;
  }

  clear_free_space();

  if (pos >= Base::array.max_element)
    return 1;

  if (pos >= Base::array.elements)
    Base::array.elements= pos + 1;

  entry= dynamic_array_ptr(array, pos);
  *(T**)entry= ptr;
   
  return 0;
}
 
template<class T>
inline
T* Map<uint,T>::operator[](ulong pos) const
{
  if (pos >= Base::elements())
    return NULL;

  return Base::at(pos);
}

/** 
  Return number of entries in the dynamic array.
 
  @note Some of the entries can store NULL pointers as no mapping was
  defined for them.
 */
template<class T>
inline
ulong Map<uint,T>::count() const
{ return Base::array.elements; }


#endif
