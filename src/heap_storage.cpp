#include "heap_storage.h"
#include "not_impl.h"
#include "storage_engine.h"
#include <cstring>

typedef u_int16_t u16;
typedef u_int32_t u32;

// BEGIN: SlottedPage //

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new)
    : DbBlock(block, block_id, is_new) {
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
  u16 size = (u16)data->get_size();
  this->end_free -= size;
  u16 loc = this->end_free + 1;
  put_header();
  put_header(id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return id;
}

Dbt *SlottedPage::get(RecordID record_id) {
  u16 size, loc;
  this->get_header(size, loc, record_id);
  return loc == 0 ? nullptr : new Dbt(address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
  throw NotImplementedError();
}

// FIXME: This is probably wrong
void SlottedPage::del(RecordID record_id) {
  u16 size, loc;
  get_header(size, loc, record_id);
  if (loc != 0) {
    put_header(record_id, 0, 0);
    memmove(this->address(this->end_free + size), this->address(this->end_free),
            loc - this->end_free);
    this->end_free += size;
    for (RecordID i = record_id + 1; i < this->num_records; i++) {
      u16 s, l;
      get_header(s, l, i);
      put_header(i, s, l + size);
    }
  }
}

RecordIDs *SlottedPage::ids() { throw NotImplementedError(); }

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
  size = get_n(2 * sizeof(u16) * id);
  loc = get_n(2 * sizeof(u16) * id + sizeof(u16));
}

// Store the size and offset for given id. For id of zero, store the block
// header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
  if (id == 0) { // called the put_header() version and using the default params
    size = this->num_records;
    loc = this->end_free;
  }
  put_n(2 * sizeof(u16) * id, size);
  put_n(2 * sizeof(u16) * id + 2, loc);
}

bool SlottedPage::has_room(u16 size) { throw NotImplementedError(); }

void SlottedPage::slide(u16 start, u16 end) { throw NotImplementedError(); }

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) { return *(u16 *)this->address(offset); }

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
  *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset) {
  return (void *)((char *)this->block.get_data() + offset);
}

// END  : SlottedPage //

// BEGIN: HeapFile //

void HeapFile::create(void) { throw NotImplementedError(); }

void HeapFile::drop(void) { throw NotImplementedError(); }

void HeapFile::open(void) { throw NotImplementedError(); }

void HeapFile::close(void) { throw NotImplementedError(); }

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and
// its block id.
SlottedPage *HeapFile::get_new(void) {
  char block[DbBlock::BLOCK_SZ];
  std::memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  int block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));

  // write out an empty block and read it back in so Berkeley DB is managing the
  // memory
  SlottedPage *page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data,
               0); // write it out with initialization applied
  this->db.get(nullptr, &key, &data, 0);
  return page;
}

SlottedPage *HeapFile::get(BlockID block_id) { throw NotImplementedError(); }

void HeapFile::put(DbBlock *block) { throw NotImplementedError(); }

BlockIDs *HeapFile::block_ids() { throw NotImplementedError(); }

void HeapFile::db_open(uint flags) { throw NotImplementedError(); }

// END  : HeapFile //

// BEGIN: HeapTable //

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes),
      file(table_name) {
  throw NotImplementedError();
}

void HeapTable::create() { throw NotImplementedError(); }

void HeapTable::create_if_not_exists() { throw NotImplementedError(); }

void HeapTable::drop() { throw NotImplementedError(); }

void HeapTable::open() { throw NotImplementedError(); }

void HeapTable::close() { throw NotImplementedError(); }

Handle HeapTable::insert(const ValueDict *row) { throw NotImplementedError(); }

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
  throw NotImplementedError();
}

void HeapTable::del(const Handle handle) { throw NotImplementedError(); }

Handles *HeapTable::select() { throw NotImplementedError(); }

Handles *HeapTable::select(const ValueDict *where) {
  Handles *handles = new Handles();
  BlockIDs *block_ids = file.block_ids();
  for (auto const &block_id : *block_ids) {
    SlottedPage *block = file.get(block_id);
    RecordIDs *record_ids = block->ids();
    for (auto const &record_id : *record_ids)
      handles->push_back(Handle(block_id, record_id));
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}

ValueDict *HeapTable::project(Handle handle) { throw NotImplementedError(); }

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
  throw NotImplementedError();
}

ValueDict *HeapTable::validate(const ValueDict *row) {
  throw NotImplementedError();
}

Handle HeapTable::append(const ValueDict *row) { throw NotImplementedError(); }

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed
// ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row) {
  char *bytes =
      new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row
                                   // fits into DbBlock::BLOCK_SZ)
  uint offset = 0;
  uint col_num = 0;
  for (auto const &column_name : this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      *(int32_t *)(bytes + offset) = value.n;
      offset += sizeof(int32_t);
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      uint size = value.s.length();
      *(u16 *)(bytes + offset) = size;
      offset += sizeof(u16);
      memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
      offset += size;
    } else {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data) { throw NotImplementedError(); }

// END  : HeapTable //
