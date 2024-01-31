#include "heap_storage.h"
#include "storage_engine.h"
#include "gmock/gmock.h"
#include <cstring>
#include <gtest/gtest.h>
#include <string>

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
 * @tests SlottedPage::add
 */
TEST_F(SlottedPageTest, AddTestMem) {
  page = new SlottedPage(wrapper, 0, true);
  std::string field_a("ABCDEFGHIJKLM");
  std::string field_b("NOPQRSTUVWXYZ");

  std::string expected = field_b + field_a;

  Dbt f_1(field_a.data(), field_a.length());
  Dbt f_2(field_b.data(), field_b.length());

  page->add(&f_1);
  page->add(&f_2);

  // Check that memory is as expected
  ASSERT_THAT(std::string(&buf[DbBlock::BLOCK_SZ - expected.length()],
                          expected.length()),
              expected);
}

/**
 * @tests SlottedPage::add
 */
TEST_F(SlottedPageTest, AddToFullPage) {
  page = new SlottedPage(wrapper, 0, true);
  const size_t field_a_len = DbBlock::BLOCK_SZ - sizeof(u_int16_t) * 2 * 2 - 1;
  char field_a[field_a_len];
  std::string field_b("A");

  Dbt f_1(&field_a, field_a_len);
  Dbt f_2(field_b.data(), field_b.length());

  ASSERT_NO_THROW(page->add(&f_1));
  ASSERT_THROW(page->add(&f_2), DbBlockNoRoomError);
}

/**
 * @tests SlottedPage::get
 */
TEST_F(SlottedPageTest, GetField) {
  page = new SlottedPage(wrapper, 0, true);
  std::string field_a("ABCDEFGHIJKLM");
  std::string field_b("NOPQRSTUVWXYZ");

  Dbt f_1(field_a.data(), field_a.length());
  Dbt f_2(field_b.data(), field_b.length());

  RecordID p_1 = page->add(&f_1);
  RecordID p_2 = page->add(&f_2);

  Dbt *get_1 = page->get(p_1);

  ASSERT_THAT(std::string((char *)get_1->get_data(), get_1->get_size()),
              field_a);

  Dbt *get_2 = page->get(p_2);

  ASSERT_THAT(std::string((char *)get_2->get_data(), get_2->get_size()),
              field_b);

  delete get_1;
  delete get_2;
}

/**
 * @tests SlottedPage::put
 */
TEST_F(SlottedPageTest, PutAddHole) {
  page = new SlottedPage(wrapper, 0, true);
  std::string mem_a("ABCDEFGHIJKLM");
  std::string mem_b("0000000000000");
  std::string mem_c("NOPQRSTUVWXYZ");

  std::string value = mem_c + mem_a;
  std::string expected = mem_c + mem_b + mem_a;

  Dbt f_1(mem_a.data(), mem_a.length());
  Dbt f_2(mem_b.data(), mem_b.length());
  Dbt f_3(mem_c.data(), mem_c.length());
  Dbt f_b(nullptr, 0);

  page->add(&f_1);
  RecordID p_2 = page->add(&f_b);
  page->add(&f_3);

  // Check that memory is as expected
  ASSERT_THAT(
      std::string(&buf[DbBlock::BLOCK_SZ - value.length()], value.length()),
      value);

  page->put(p_2, f_2);

  ASSERT_THAT(std::string(&buf[DbBlock::BLOCK_SZ - expected.length()],
                          expected.length()),
              expected);
}

/**
 * @tests SlottedPage::del
 */
TEST_F(SlottedPageTest, DelCloseHole) {
  page = new SlottedPage(wrapper, 0, true);
  std::string mem_a("ABCDEFGHIJKLM");
  std::string mem_b("0000000000000");
  std::string mem_c("NOPQRSTUVWXYZ");

  std::string value = mem_c + mem_b + mem_a;
  std::string expected = mem_c + mem_a;

  Dbt f_1(mem_a.data(), mem_a.length());
  Dbt f_2(mem_b.data(), mem_b.length());
  Dbt f_3(mem_c.data(), mem_c.length());

  page->add(&f_1);
  RecordID p_2 = page->add(&f_2);
  page->add(&f_3);

  // Check that memory is as expected
  ASSERT_THAT(
      std::string(&buf[DbBlock::BLOCK_SZ - value.length()], value.length()),
      value);

  page->del(p_2);

  ASSERT_THAT(std::string(&buf[DbBlock::BLOCK_SZ - expected.length()],
                          expected.length()),
              expected);
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

/**
 * @tests SlottedPage::has_room
 */
TEST_F(SlottedPageTest, HasRoom) {
  page = new SlottedPage(wrapper, 0, true);

  // Check that the page has room for a record of size 0
  ASSERT_TRUE(wrap_has_room(0));

  // Check that the page has room for a record of that fills the entire free
  // space
  ASSERT_TRUE(wrap_has_room(DbBlock::BLOCK_SZ - sizeof(u_int16_t) * 2 - 1));

  // Boundary check
  ASSERT_FALSE(wrap_has_room(DbBlock::BLOCK_SZ - sizeof(u_int16_t) * 2));

  // Simulate a 5 records with total size BLOCK_SZ/2
  set_num_records(5);
  set_end_free(DbBlock::BLOCK_SZ - DbBlock::BLOCK_SZ / 2 - 1);

  ASSERT_TRUE(
      wrap_has_room(DbBlock::BLOCK_SZ / 2 - sizeof(u_int16_t) * 2 * 6 - 1));

  ASSERT_FALSE(
      wrap_has_room(DbBlock::BLOCK_SZ / 2 - sizeof(u_int16_t) * 2 * 6));
}
