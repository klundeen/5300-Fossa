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

}

void SlottedPage::put(RecordID record_id, const Dbt &data) {

}

void SlottedPage::del(RecordID record_id) {

}

RecordIDs* SlottedPage::ids(void) {

}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0) {

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

}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {

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
