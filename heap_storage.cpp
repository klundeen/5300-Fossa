/*
 *@file heap_storage.cpp - Implementation of storage_engine with a heap file structure
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Lili Hao, Tuan Phan, Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */

#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>

using namespace std;
typedef uint16_t u16;

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */

/**
 * Constructor
 * @param block
 * @param block_id
 * @param is_new
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
  if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
  } else {
        get_header(this->num_records, this->end_free);
  }
}

/**
 * Add a new record to the block. Return its id.
 * @param data
 */
RecordID SlottedPage::add(const Dbt *data) {
 if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**                                                                                                                                          
 * Get a record from the block                                                                                             
 * @param record_id                                                                                                                               
 */
Dbt* SlottedPage::get(RecordID record_id) {
  u16 size;
  u16 loc;
  get_header(size, loc, record_id);
  if(loc == 0)
    return nullptr;
  return new Dbt(this->address(loc), size);
}

/**
 * Update the record with the given data.
 * @param record_id
 * @param data       
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {
  u16 size;
  u16 loc;
  get_header(size, loc, record_id);
  u16 new_size = (u16) data.get_size();
  if (new_size > size) {
      u16 extra = new_size - size;
      if (!this->has_room(extra))
          throw DbBlockNoRoomError("not enough room to put new record");
      this->slide(loc, loc - extra);
      memcpy(this->address(loc - extra), data.get_data(), new_size);
  } else {
      memcpy(this->address(loc), data.get_data(), new_size);
      this->slide(loc + new_size, loc + size);
  }
  get_header(size, loc, record_id);
  put_header(record_id, new_size, loc);
}

/**
 * Delete a record from the page
 * @param record_id
 */
void SlottedPage::del(RecordID record_id) {
  u16 size;
  u16 loc;
  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  this->slide(loc, loc + size);
}

/**
 * Get sequence of all non-deleted record ids.
 */
RecordIDs* SlottedPage::ids(void) {
  RecordIDs *result = new RecordIDs();
  u16 size;
  u16 loc;
  for (u16 i  = 1; i <= this->num_records; i++) {
    get_header(size, loc, i);
    if (loc != 0)
          result->push_back(i);
  }
  return result;
}

/**
 * Get the size and offset for given id. 
 * @param size  
 * @param loc  
 * @param id    
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
  size = get_n((u16)4 * id);
  loc = get_n((u16)(4 * id + 2));
}

/**
 * Store the size and offset for given id. For id of zero, store the block header.
 * @param size                                                                                                                               
 * @param loc                                                                                                                                
 * @param id 
 */
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

/**
 * Check if we have room to store a record with given size. 
 * @param size  
 */
bool SlottedPage::has_room(u_int16_t size) {
  u16 capacity;
  capacity = this->end_free -(4 * (this->num_records + 1));
  return size <= capacity;
}

/**
 * Slide the contents to fit for the size of the record.
 * @param start
 * @param end   
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
  u16 shift = end - start;
  if (shift == 0){
          return;
  }

  //slide data
  memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), shift); 

  //move headers to the right
  RecordIDs* record_ids = this->ids();
  for (unsigned int i = 0; i < record_ids->size(); i++) {
    u16 loc;
    u16 size;
    get_header(size, loc, record_ids->at(i));
    if (loc <= start) {
      loc += shift;
      put_header(record_ids->at(i), size, loc);
    }
  }

  this->end_free += shift;
  this->put_header();
  delete record_ids;
}

/**
 * Get 2-byte integer at given offset in block.
 * @param offset
 */
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

/**
 *Put a 2-byte integer at given offset in block.
 *@param offset
 *@param n
 */
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

/**
 *Make a void* pointer for a given offset into the data block.
 *@param offset
 */
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */

/**
 * Create new file
 */
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block_page = get_new();
    delete block_page;
}

/**
 * Delete file
 */
void HeapFile::drop(void) {
    close();
    // Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

/**                                                                                                                                          
 * Open file                                                                                                                               
 */
void HeapFile::open(void) {
    db_open();
}

/**                                                                                                                                          
 * Close file                                                                                                                               
 */
void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

/**
 * Allocate a new block for the database file.
 * Returns the new empty DbBlock that is managing the records in this block and its block id
 */
SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

/**
 * Get a block from the database
 * @param block_id
 */
SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

/**                                                                                                                                          
 * Write a block to the database                                                                                                             
 * @param block                                                                                                                           
 */
void HeapFile::put(DbBlock *block) {
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Get sequence of all block ids.
 */
BlockIDs *HeapFile::block_ids() {
    BlockIDs *result = new BlockIDs();
    for (BlockID id = 1; id <= this->last; id++)
        result->push_back(id);
    return result;
}

/**
 * Wrapper for Berkeley DB open, which does both open and creation.
 */
void HeapFile::db_open(uint flags) {
	if (!this->closed) 
		return;

	this->db.set_re_len(DbBlock::BLOCK_SZ);
    // get the correct name for db
    // need to test here to make sure it work
	this->dbfilename = this->name + ".db";
	this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
	DB_BTREE_STAT *stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	this->last = flags ? 0 : stat->bt_ndata;
	this->closed = false;
}
         

         
         
bool test_heap_storage() {return true;}
