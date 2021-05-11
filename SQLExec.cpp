/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"
#include <iostream>

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    if (SQLExec::indices == nullptr)
        SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception &e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    string table_name = string(statement->tableName);
    string index_name = string(statement->indexName);
    string index_type = string(statement->indexType);
    int32_t is_unique = (index_type == "BTREE")? 1 : 0;
    vector<char*>* index_columns = statement->indexColumns;
    DbRelation& table_to_index = SQLExec::tables->get_table(table_name);
    ColumnNames table_to_index_columns = table_to_index.get_column_names();
    cout << "DB Open Error post-call to get_column_names" << endl;

    int32_t seq_in_index = 0;

    Handles inserted_rows;
    try {
        for (uint i = 0; i < index_columns->size(); i++) {
            if (find(table_to_index_columns.begin(), table_to_index_columns.end(), Identifier(index_columns->at(i))) == table_to_index_columns.end()) {
                cout << "Index Column NOT FOUND: " << string(index_columns->at(i)) << endl;
                throw DbRelationError("Indexing column not found in table to index");
            } else {
                cout << "Index Column added: " << string(index_columns->at(i)) << endl;
                cout << "Index Column of Type: " << index_type << endl;
                seq_in_index++;
                ValueDict index_row;
                index_row["table_name"] = Value(table_name);
                index_row["index_name"] = Value(index_name);
                index_row["seq_in_index"] = Value(seq_in_index);
                index_row["column_name"] = Value(string(index_columns->at(i)));
                index_row["index_type"] = Value(index_type);
                index_row["is_unique"] = Value(is_unique);
                // construct ValueDict for index row
                inserted_rows.push_back(SQLExec::indices->insert(&index_row));
            }
        }
    } catch (exception &e) {
        for (Handle handle : inserted_rows) {
            cout << "Opps, deleted row" << endl;
            cout << "What the hell: " << e.what() << endl;
            SQLExec::indices->del(handle);
        }
    }
    cout << "Number of rows in index table entry: " << to_string(inserted_rows.size()) << endl;
    cout << "DB Open Error pre-getIndex" << endl;
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    cout << "DB Open Error point #1" << endl;
    index.create();
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove Indices for table
    IndexNames index_names = indices->get_index_names(table_name);
    for (Identifier index : index_names) {
        DbIndex& index_handle = indices->get_index(table_name, index);
        index_handle.drop();
    }
    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    string table_name = string(statement->name);
    string index_name = string(statement->indexName);
    DbIndex& index = indices->get_index(table_name, index_name);
    index.drop();

    DbRelation& indices_table = tables->get_table(Indices::TABLE_NAME);
    ValueDict where;
    where["index_name"] = Value(index_name);
    where["table_name"] = Value(table_name);
    Handles *handles = indices_table.select(&where);

    for (auto const &handle: *handles)
        indices_table.del(handle);
    delete handles;
    ColumnNames index_columns;

    return new QueryResult("dropped index " + index_name);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    DbRelation &indices_handle = SQLExec::tables->get_table(Indices::TABLE_NAME);
    ColumnNames* col_names = new ColumnNames;
    ColumnAttributes* col_attributes = new ColumnAttributes;

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    cout << "Table Name sent to select: " << statement->tableName << endl;

    col_names->push_back("table_name");
    col_names->push_back("index_name");
    col_names->push_back("seq_in_index");
    col_names->push_back("column_name");
    col_names->push_back("index_type");
    col_names->push_back("is_unique");

    ColumnAttribute ca(ColumnAttribute::TEXT);
    col_attributes->push_back(ca);  // table_name
    col_attributes->push_back(ca);  // index_name
    ca.set_data_type(ColumnAttribute::INT);
    col_attributes->push_back(ca);  // seq_in_index
    ca.set_data_type(ColumnAttribute::TEXT);
    col_attributes->push_back(ca);  // column_name
    col_attributes->push_back(ca);  // index_type
    ca.set_data_type(ColumnAttribute::BOOLEAN);
    col_attributes->push_back(ca);  // is_unique


    Handles *handles = indices_handle.select(&where);
    u_long n = handles->size();
    ValueDicts *rows = new ValueDicts;

    for(auto const &handle: *handles) {
        ValueDict *row = indices_handle.project(handle);
        cout << "seq_in_index: " << row->at("seq_in_index").n << endl;
        cout << "column_name: " << row->at("column_name").s << endl;
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(col_names, col_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

