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
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute = ColumnAttribute(ColumnAttribute::INT);
        case ColumnDefinition::TEXT:
            column_attribute = ColumnAttribute(ColumnAttribute::TEXT);
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
        for (ColumnDefinition *col : *statement->columns) {
            ValueDict *column_entry = new ValueDict();
            Value val_table_name = Value(table_name);
            Value val_column_name = string(col->name);
            Value val_data_type = sting(if(col->type == ColumnDefinition::TEXT)? "TEXT" : "INT");
            (*column_entry)["table_name"] = val_table_name;
            (*column_entry)["column_name"] = val_column_name;
            (*column_entry)["data_type"] = val_data_type;
            tables->columns_table->insert(column_entry); // May Throw DbRelationError
        }
        // Update _columns Schema
        ColumnAttributes* column_attributes = new ColumnAttributes();
        ColumnNames *column_names = new ColumnNames();
        for (ColumnDefinition *col : *statement->columns) {
            string column_name;
            ColumnAttribute column_attribute;
            column_definition(col, column_name, column_attribute);
            column_names->push_back(column_name);
            column_attributes->push_back(column_attribute);
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
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

