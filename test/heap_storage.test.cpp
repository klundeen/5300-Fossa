#include "heap_storage.h"
#include "storage_engine.h"
#include <gtest/gtest.h>

DbEnv *_DB_ENV; // TODO: Mock when needed

class SlottedPageTest : public testing::Test {
protected:
  void SetUp() override {
    wrapper = Dbt(buf, DbBlock::BLOCK_SZ);
    page = nullptr;
  }

  void TearDown() override { delete page; }

  char buf[DbBlock::BLOCK_SZ]; // Fake DB block
  Dbt wrapper;                 // Wraps buf
  SlottedPage *page;           // Page handle for tests

  // Treat buf as a u16 array with in index by u8
  u_int16_t &addrBufFrom(size_t index) { return *(u_int16_t *)&buf[index]; }

  // Wrappers to allow tests access of protected/private methods
  void wrap_get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0) {
    page->get_header(size, loc, id);
  }
  void wrap_put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0) {
    page->put_header(id, size, loc);
  }
  bool wrap_has_room(u_int16_t size) { return page->has_room(size); }
  void wrap_slide(u_int16_t start, u_int16_t end) { page->slide(start, end); }
  u_int16_t wrap_get_n(u_int16_t offset) { return page->get_n(offset); }
  void wrap_put_n(u_int16_t offset, u_int16_t n) { page->put_n(offset, n); }
  void *wrap_address(u_int16_t offset) { return page->address(offset); }

  // Getter and setter for num_records
  u_int16_t get_num_records() { return page->num_records; }
  void set_num_records(u_int16_t num_records) {
    page->num_records = num_records;
  }

  // Getter and setter for end_free
  u_int16_t get_end_free() { return page->end_free; }
  void set_end_free(u_int16_t end_free) { page->end_free = end_free; }
};

/**
 * @tests SlottedPage Constructor
 */
TEST_F(SlottedPageTest, ConstructFirstBlock) {
  memset(buf, 0xF, DbBlock::BLOCK_SZ); // Fill the buffer
  page = new SlottedPage(wrapper, 0, true);
  ASSERT_EQ(addrBufFrom(0), get_num_records());
  ASSERT_EQ(addrBufFrom(sizeof(u_int16_t)), get_end_free());
}

/**
 * @tests SlottedPage::put_n
 */
TEST_F(SlottedPageTest, PutNVisibleInBuffer) {
  page = new SlottedPage(wrapper, 0, true);
  u_int16_t testParams[3][2] = {
      {0, 0x3}, {DbBlock::BLOCK_SZ / 2, 0xA}, {DbBlock::BLOCK_SZ - 1, 0xF}};
  for (size_t i = 0; i < 3; i++) {
    wrap_put_n(testParams[i][0], testParams[i][1]);

    ASSERT_EQ(addrBufFrom(testParams[i][0]), testParams[i][1]);
  }
}

/**
 * @tests SlottedPage::get_n
 */
TEST_F(SlottedPageTest, GetNFromBuffer) {
  page = new SlottedPage(wrapper, 0, true);
  u_int16_t testParams[3][2] = {
      {0, 0xF}, {DbBlock::BLOCK_SZ / 2, 0x3}, {DbBlock::BLOCK_SZ - 1, 0xA}};
  for (size_t i = 0; i < 3; i++) {
    addrBufFrom(testParams[i][0]) = testParams[i][1];

    ASSERT_EQ(wrap_get_n(testParams[i][0]), testParams[i][1]);
  }
}
