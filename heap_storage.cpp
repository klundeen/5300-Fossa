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
#include "storage_engine.h"
#include "db_cxx.h"
#include <cstring>
#include <iostream>
#include <string>

// asertion test
#include <cassert>
#include <sys/stat.h>
#include <stdio.h>

using namespace std;
typedef uint16_t u16;
typedef u_int32_t u32;

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

//HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

/**
 * Create new file
 */
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block_page = get_new();
    //delete block_page;
    this->put(block_page);
}

/**
 * Delete file
 */
void HeapFile::drop(void) {
    close();
    Db db(_DB_ENV, 0);
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
    this->dbfilename = this->name + ".db";
    this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
	if (flags == 0) {
        DB_BTREE_STAT stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);
        this->last = stat.bt_ndata;
    } else {
        this->last = 0;
    }
	this->closed = false;
}

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name){
  //this->file = HeapFile(table_name);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
      ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    cout << "get here" << endl;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}                                                                                                     


ValueDict *HeapTable::unmarshal(Dbt *data) {
    
    ValueDict* retRow = new ValueDict();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        string* dataString = (string*)(data->get_data());
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            string subDataString = dataString->substr(offset, offset + sizeof(int32_t)); // from offset => offset + sizeof(int32_t);
            const void* subDataPointer = &subDataString;
            cout << subDataString;
            int v;
            memcpy(&v, subDataPointer, sizeof(int32_t));
            Value* val = new Value(v);
            retRow->insert(pair<Identifier , Value>(column_name, *val));
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            string sizeDataString = dataString->substr(offset, offset + sizeof(u_int16_t)); // Make Constant for 2
            const void* sizeDataPointer = &sizeDataString;
            int size;
            memcpy(&size, sizeDataPointer, sizeof(u_int16_t));
            offset += 2;
            string subDataString = dataString->substr(offset, offset + size);
            const void* subDataPointer = &subDataString;
            cout << subDataString;
            char* v;
            memcpy(&v, subDataPointer, size);
            offset += size;
            Value* val = new Value(v);
            retRow->insert(pair<Identifier , Value>(column_name, *val));
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    return retRow;

}

Handle HeapTable::append(const ValueDict *row){
    /*****
     Assumes row is fully fleshed-out. Appends a record to the file.
     *****/
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbRelationError) {
        block = this->file.get_new();
        record_id = block->add(data);
    }

    return Handle(this->file.get_last_block_id(), record_id);
};

ValueDict *HeapTable::validate(const ValueDict *row){
    /*****
     Check if the given row is acceptable to insert. Raise ValueError if not.
     Otherwise return the full row dictionary.
     *****/
  ValueDict *full_row = new ValueDict();

    for (auto const& column_name: this->column_names) {
        Value value;
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("Not Handle");
        else {
            value = column->second;
            (*full_row)[column_name] = value;
        }
    }
    return full_row;
};

void HeapTable::create(){
    /*****
     Execute: CREATE TABLE <table_name> ( <columns> )
    Is not responsible for metadata storage or validation.
     *****/
    this->file.create();
};

void HeapTable::create_if_not_exists(){
    /*****
     Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
    Is not responsible for metadata storage or validation.
     *****/
    try {
        this->file.open();
    }catch (...) {
        this->file.create();
    }
    
};

void HeapTable::open(){
    /*****
    Open existing table. Enables: insert, update, delete, select, project
     *****/
    this->file.open();
};

void HeapTable::close(){
    /*****
     Closes the table. Disables: insert, update, delete, select, project
     *****/
    this->file.close();
};

void HeapTable::drop(){
    /*****
     Execute: DROP TABLE <table_name>
     *****/
    this->file.drop();
};

Handle HeapTable::insert(const ValueDict *row){
    /*****
     Expect row to be a dictionary with column name keys.
     Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
     Return the handle of the inserted row.
     *****/
    this->file.open();
    return this->append(this->validate(row));
};

void HeapTable::update(const Handle handle, const ValueDict* new_values) {
	throw DbRelationError("Not Implemented");
}

Handles* HeapTable::select() {
	Handles* handles = new Handles();
	BlockIDs* block_ids = file.block_ids();
	for (auto const& block_id : *block_ids) {
		SlottedPage* block = file.get(block_id);
		RecordIDs* record_ids = block->ids();
		for (auto const& record_id : *record_ids)
			handles->push_back(Handle(block_id, record_id));
		delete record_ids;
		delete block;
	}
	delete block_ids;
	return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

void HeapTable::del(const Handle handle){
    /*****
    execute: DELETE FROM <table_name> WHERE <handle>
    where handle is sufficient to identify one specific record
     (e.g., returned from an insert or select).
     *****/
    this->open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    
    SlottedPage* block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
};

ValueDict* HeapTable::project(Handle handle) {
	this->open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file.get(block_id);
	Dbt* data = block->get(record_id);
	ValueDict* row = this->unmarshal(data);
	delete data;
	delete block;
	return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){
    
    /*****
     Return a sequence of values for handle given by column_names.
     *****/

    this->open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    
    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = this->unmarshal(data);
    
    if (column_names == NULL) {
        return row;
    } else {
        //FIXME
        //return {k: row[k] for k in column_names}
        return row;
    }
}

bool fileExists(const std::string& filename){
    struct stat buf;
    if (stat(filename.c_str(), &buf) != -1)
    {
        return true;
    }
    return false;
}

bool test_heap_storage(){
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exists ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    
    std::cout << "debug: *********" << std::endl;

    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();
    return true;
};

