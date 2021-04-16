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

/********** SlottedPage Class **********/

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new = false) {
  if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
  } else {
        get_header(this->num_records, this->end_free);
  }
}

// Add a new record to the block. Return its id.
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

Dbt* SlottedPage::get(RecordID record_id) {
  u16 size;
  u16 loc;
  get_header(size, loc, record_id);
  if(loc == 0)
    return nullptr;
  return new Dbt(this->address(loc), size);
}

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

void SlottedPage::del(RecordID record_id) {
  u16 size;
  u16 loc;
  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  this->slide(loc, loc + size);
}

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

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0) {
  size = get_n((u16)4 * id);
  loc = get_n((u16)(4 * id + 2));
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
  u16 capacity;
  capacity = this->end_free -(4 * (this->num_records + 1));
  return size <= capacity;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
  u16 shift = end - start;
  if (shift == 0){
          return;
  }

  //slide data
  memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_fre$

  //move headers to the right
  RecordIDs* record_ids = this->ids();
  for (unsigned int i = 0; i < record_ids->size(); i++) {
    u16 loc;
    u16 size;
    get_header(size, loc, temp->at(i));
    if (loc <= start) {
      loc += shift;
      put_header(temp->at(i), size, loc);
    }
  }

  this->end_free += shift;
  this->put_header();
  delete record_ids;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}







bool test_heap_storage() {return true;}
