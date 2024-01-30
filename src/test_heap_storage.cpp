#include "heap_storage.h"

// test function -- returns true if all tests pass
bool test_heap_storage() {
  ColumnNames column_names;
  column_names.push_back("a");
  column_names.push_back("b");
  ColumnAttributes column_attributes;
  ColumnAttribute ca(ColumnAttribute::INT);
  column_attributes.push_back(ca);
  ca.set_data_type(ColumnAttribute::TEXT);
  column_attributes.push_back(ca);

  std::cout << "create " << std::flush;
  HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
  table1.create();
  std::cout << "ok" << std::endl;

  std::cout << "drop " << std::flush;
  table1.drop(); // drop makes the object unusable because of BerkeleyDB
                 // restriction -- maybe want to fix this some day
  std::cout << "ok" << std::endl;

  std::cout << "create_if_not_exists " << std::flush;
  HeapTable table("_test_data_cpp", column_names, column_attributes);
  table.create_if_not_exists();
  std::cout << "ok" << std::endl;

  ValueDict row;
  row["a"] = Value(12);
  row["b"] = Value("Hello!");
  std::cout << "insert " << std::flush;
  table.insert(&row);
  std::cout << "ok" << std::endl;

  std::cout << "select " << std::flush;
  Handles *handles = table.select();
  std::cout << "ok " << handles->size() << std::endl;

  std::cout << "project " << std::flush;
  ValueDict *result = table.project((*handles)[0]);
  Value value = (*result)["a"];
  if (value.n != 12)
    return false;
  value = (*result)["b"];
  if (value.s != "Hello!")
    return false;
  std::cout << "ok" << std::endl;

  std::cout << "close " << std::flush;
  table.close();
  std::cout << "ok" << std::endl;
  delete handles;
  delete result;

  HeapTable table3("_test_data_cpp", column_names, column_attributes);
  std::cout << "open " << std::flush;
  table3.open();
  handles = table3.select();
  std::cout << "ok " << handles->size() << std::endl;
  delete handles;

  table3.drop();

  return true;
}
