/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

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
    // FIXME
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
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
    column_name = string(col->name);
    ColumnDefinition::DataType dataType = col->type;
    switch (dataType) {
        case ColumnDefinition::DataType::INT:
            column_attribute = ColumnAttribute(ColumnAttribute::INT);
            break;
        case ColumnDefinition::DataType::TEXT:
            column_attribute = ColumnAttribute(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("Column Datatype not supported");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    if (statement->type != CreateStatement::kTable)
        throw SQLExecError("CREATE Statement Not recognized");

    // Update _tables Schema
    string table_name = string(statement->tableName);
    ValueDict *table_entry = new ValueDict();
    Value val_table_name = Value(table_name);
    (*table_entry)["table_name"] = val_table_name;
    tables->insert(table_entry); // May Throw DbRelationError

    try {
        // Update _columns Schema
        ColumnNames* column_names = new ColumnNames;
        ColumnAttributes* column_attributes = new ColumnAttributes();
        for (ColumnDefinition *col : *statement->columns) {
            Identifier column_name;
            ColumnAttribute column_attribute;
            column_definition(col, column_name, column_attribute);
            column_names->push_back(column_name);
            column_attributes->push_back(column_attribute);
        }
        ValueDict row;
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names->size(); i++) {
                row["column_name"] = (*column_names)[i];
                row["data_type"] = Value((*column_attributes)[i].get_data_type() ==
                                         ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(
                        columns.insert(&row));  // Insert into _columns
            }
        } catch (...) {
            for(Handle handle : c_handles) {
                columns.del(handle);
            }
        }
        HeapTable *table = new HeapTable(table_name, *column_names,
                                     *column_attributes);
        if (statement->ifNotExists) {
            table->create_if_not_exists();
        } else {
            table->create();
        }
    } catch (DbRelationError) {
        tables->del((*tables->select(table_entry))[0]);
    }
    return new QueryResult("Created" + table_name); // FIXME
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type){
	case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
       // case ShowStatement::kIndex:
           // return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
    return new QueryResult("not implemented");
}

QueryResult *SQLExec::show_tables() {
    DbRelation &tables_relation = tables->get_table(Tables::TABLE_NAME);
    // Construct Query with "Print out at message"
    // Add in table_name to message and "\n"
    // Call Dbrelation's select() method to get handles
    // loop through handles (calling project each time project)
    // Look through valueDict and add to QueryResult message (with a \n between rows)
    ColumnNames *col_names = new ColumnNames;
    col_names->push_back("table_name");

    ColumnAttributes *col_attributes = new ColumnAttributes;
    col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() -3;
    ValueDicts *rows = new ValueDicts;
    for(auto const &handle: *handles){
	ValueDict *row = SQLExec::tables->project(handle, col_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME) //removed table_name != Indices::TABLE_NAME for test
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(col_names, col_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // Logic for pulling columns out to print
    // Call get_columns specific to the table in "statement"
    DbRelation &cols = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = cols.select(&where);

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles){
	ValueDict *row = cols.project(handle, column_names);
	rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "success"); 
}

