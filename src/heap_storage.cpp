#include "heap_storage.h"
#include "not_impl.h"
#include "storage_engine.h"
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <db_cxx.h>
#include <string>
#include <utility>

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
  u16 size, loc;
  get_header(size, loc, record_id);
  u16 new_size = data.get_size();
  if (new_size > size) {
    u16 extra = new_size - size;
    if (!has_room(extra))
      throw DbBlockNoRoomError("not enough room for new record");
    slide(loc, loc - extra);
    memcpy(this->address(loc - extra), data.get_data(), new_size);
  } else {
    memcpy(this->address(loc), data.get_data(), new_size);
    slide(loc + new_size, loc + size);
  }
  get_header(size, loc, record_id);
  put_header(record_id, size, loc);
}

void SlottedPage::del(RecordID record_id) {
  u16 size, loc;
  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);
}

RecordIDs *SlottedPage::ids() {
  RecordIDs *records = new RecordIDs();
  records->reserve(this->num_records);
  for (u16 i = 1; i <= this->num_records; i++) {
    u16 size, loc;
    get_header(size, loc, i);
    if (loc != 0) {
      records->emplace_back(i);
    }
  }
  return records;
}

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

bool SlottedPage::has_room(u16 size) {
  u16 space = this->end_free - (this->num_records + 1) * sizeof(u16) * 2;
  return space >= size;
}

void SlottedPage::slide(u16 start, u16 end) {
  int shift = end - start;
  if (shift == 0)
    return;

  u16 block_start = this->end_free + 1;
  // Memmove should be safer for overlap
  memmove(address(block_start + shift), address(block_start), abs(shift));

  RecordIDs *records = this->ids();
  for (RecordID &it : *records) {
    u16 size, loc;
    get_header(size, loc, it);
    if (loc <= start) {
      loc += shift;
      put_header(it, size, loc);
    }
  }
  this->end_free += shift;
  put_header();

  delete records;
}

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

void HeapFile::create(void) {
  db_open(DB_CREATE | DB_EXCL);
  SlottedPage *first_block = get_new();
  put(first_block);
  delete first_block;
}

void HeapFile::drop(void) {
  this->close();
  std::remove(this->dbfilename.c_str());
}

void HeapFile::open(void) { db_open(); }

void HeapFile::close(void) {
  this->db.close(0U);
  this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and
// its block id.
SlottedPage *HeapFile::get_new(void) {
  char block[DbBlock::BLOCK_SZ];
  std::memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  u32 block_id = ++(this->last);
  Dbt key(&block_id, sizeof(block_id));

  // write out an empty block and read it back in so Berkeley DB is managing the
  // memory
  SlottedPage *page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data,
               0U); // write it out with initialization applied
  this->db.get(nullptr, &key, &data, 0U);
  return page;
}

SlottedPage *HeapFile::get(BlockID block_id) {
  Dbt key(&block_id, sizeof(block_id)), block;
  this->db.get(nullptr, &key, &block, 0U);
  SlottedPage *page = new SlottedPage(block, block_id);
  return page;
}

void HeapFile::put(DbBlock *block) {
  BlockID block_id = block->get_block_id();
  Dbt key(&block_id, sizeof(block_id));
  this->db.put(nullptr, &key, block->get_block(), 0U);
}

BlockIDs *HeapFile::block_ids() {
  BlockIDs *ids = new BlockIDs();
  ids->reserve(this->last);
  for (u32 i = 1; i <= this->last; i++) {
    ids->emplace_back(i);
  }
  return ids;
}

void HeapFile::db_open(uint flags) {
  this->db.set_message_stream(_DB_ENV->get_message_stream());
  this->db.set_error_stream(_DB_ENV->get_error_stream());
  this->db.set_re_len(DbBlock::BLOCK_SZ); // Set record length to 4K
  db.open(nullptr, (this->name + ".db").c_str(), nullptr, DB_RECNO, flags,
          0644);

  const char *filename, *dbname;
  this->db.get_dbname(&filename, &dbname);
  _DB_ENV->get_home(&dbname); // We dont need dbname so reuse it.
  this->dbfilename = std::string(dbname) + '/' + std::string(filename);

  // FIXME: This still results in a massive last block for opening files
  // If the create flag is set then assume blank file.
  if ((flags & DB_CREATE) == 0U) {
    DB_BTREE_STAT stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = stat.bt_ndata;
  } else {
    this->last = 0;
  }

  this->closed = false;
}

// END  : HeapFile //

// BEGIN: HeapTable //

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes),
      file(table_name) {}

void HeapTable::create() { this->file.create(); }

void HeapTable::create_if_not_exists() {
  try {
    this->open();
  } catch (const DbException &) {
    // TODO: Narrow exception catching
    this->create();
  }
}

void HeapTable::drop() { this->file.drop(); }

void HeapTable::open() { this->file.open(); }

void HeapTable::close() { this->file.close(); }

Handle HeapTable::insert(const ValueDict *row) {
  ValueDict *validated = validate(row);
  Handle added = append(validated);
  delete validated;
  return added;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
  throw NotImplementedError();
}

void HeapTable::del(const Handle handle) { throw NotImplementedError(); }

Handles *HeapTable::select() {
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

Handles *HeapTable::select(const ValueDict *where) {
  throw NotImplementedError();
}

ValueDict *HeapTable::project(Handle handle) {
  SlottedPage *block = this->file.get(handle.first);
  Dbt *data = block->get(handle.second);
  ValueDict *row = unmarshal(data);
  delete data;
  delete block;
  return row;
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
  ValueDict *result = new ValueDict();
  ValueDict *row = project(handle);
  for (auto const &it : *column_names) {
    result->insert(row->extract(it));
  }
  delete row;
  return result;
}

ValueDict *HeapTable::validate(const ValueDict *row) {
  ValueDict *new_row = new ValueDict();
  for (Identifier const &it : this->column_names) {
    auto const &elm = row->find(it);
    if (elm == row->end()) {
      throw DbRelationError("Row missing fields");
    }
    new_row->emplace(std::make_pair(it, elm->second));
  }
  return new_row;
}

Handle HeapTable::append(const ValueDict *row) {
  Dbt *data = marshal(row);
  SlottedPage *block = this->file.get(this->file.get_last_block_id());
  RecordID id;
  try {
    id = block->add(data);
  } catch (const DbBlockNoRoomError &) {
    delete block;
    block = this->file.get_new();
    id = block->add(data);
  }
  this->file.put(block);

  Handle h(block->get_block_id(), id);

  char *bytes = (char *)data->get_data();
  delete[] bytes;
  delete block;
  delete data;
  return h;
}

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

ValueDict *HeapTable::unmarshal(Dbt *data) {
  ValueDict *values = new ValueDict();
  char *bytes = (char *)data->get_data();
  uint offset = 0;
  u16 size;
  std::string text;
  for (size_t i = 0; i < this->column_names.size(); i++) {
    switch (this->column_attributes[i].get_data_type()) {
    case ColumnAttribute::DataType::INT:
      values->emplace(std::make_pair(this->column_names[i],
                                     Value(*(int32_t *)(bytes + offset))));
      offset += sizeof(int32_t);
      break;
    case ColumnAttribute::DataType::TEXT:
      size = *(u16 *)(bytes + offset);
      offset += sizeof(u16);
      text = std::string(bytes + offset, size);
      offset += size;
      values->emplace(std::make_pair(this->column_names[i], Value(text)));
      break;
    default:
      throw NotImplementedError();
      break;
    }
  }
  return values;
}

// END  : HeapTable //
