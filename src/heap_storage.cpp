#include "heap_storage.h"
#include "storage_engine.h"

typedef u_int16_t u16;

// BEGIN: SlottedPage //

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new)
    : DbBlock(block, block_id, is_new) {}

RecordID SlottedPage::add(const Dbt *data) {}

Dbt *SlottedPage::get(RecordID record_id) {}

void SlottedPage::put(RecordID record_id, const Dbt &data) {}

void SlottedPage::del(RecordID record_id) {}

RecordIDs *SlottedPage::ids() {}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {}

bool SlottedPage::has_room(u_int16_t size) {}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {}

u_int16_t SlottedPage::get_n(u_int16_t offset) {}

void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {}

void *SlottedPage::address(u_int16_t offset) {}

// END  : SlottedPage //

// BEGIN: HeapFile //

void HeapFile::create(void) {}

void HeapFile::drop(void) {}

void HeapFile::open(void) {}

void HeapFile::close(void) {}

SlottedPage *HeapFile::get_new(void) {}

SlottedPage *HeapFile::get(BlockID block_id) {}

void HeapFile::put(DbBlock *block) {}

BlockIDs *HeapFile::block_ids() {}

void HeapFile::db_open(uint flags) {}

// END  : HeapFile //

// BEGIN: HeapTable //

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file("TODO") {}

void HeapTable::create() {}

void HeapTable::create_if_not_exists() {}

void HeapTable::drop() {}

void HeapTable::open() {}

void HeapTable::close() {}

Handle HeapTable::insert(const ValueDict *row) {}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

Handles *HeapTable::select() {}

Handles *HeapTable::select(const ValueDict *where) {}

ValueDict *HeapTable::project(Handle handle) {}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {}

ValueDict *HeapTable::validate(const ValueDict *row) {}

Handle HeapTable::append(const ValueDict *row) {}

Dbt *HeapTable::marshal(const ValueDict *row) {}

ValueDict *HeapTable::unmarshal(Dbt *data) {}

// END  : HeapTable //
