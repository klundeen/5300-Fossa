/**
 * @file sql5300.cpp - main entry for the relation manaager's SQL shell
 * @author Tuan Phan, Lili Hao
 * @see "Seattle University, cpsc4300/5300, Spring 2021"
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch (col->type) {
        case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " UNKNOWN TYPE";
            break;
    }
    return ret;
}

/**
 * Execute an SQL create statement
 * @param stmt  Hyrise AST for the create statement
 * @returns     a string of the SQL statment
 */
string executeCreate(const CreateStatement *stmt) {
    string result("CREATE TABLE ");
    if (stmt->type != CreateStatement::kTable)
        return result + "...";
    if (stmt->ifNotExists)
        result += "IF NOT EXISTS ";
    result += string(stmt->tableName) + " (";
    for (ColumnDefinition *col : *stmt->columns) {
        result += columnDefinitionToString(col);
        result += ", ";
    }
    result = result.substr(0, result.length() - 2);
    result += ")";
    return result;
}

/**
 * Execute an SQL statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the statement
 * @returns     a string (for now) of the SQL statment
 */
string execute(const SQLStatement *stmt) {
  switch (stmt->type()) {
  //      case kStmtSelect:
  //          return executeSelect((const SelectStatement *) stmt);
        case kStmtCreate:
            return executeCreate((const CreateStatement *) stmt);
        default:
            return "Not implemented";
   }
}

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char **argv) {

    // Open/create the db enviroment
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }
    char *envHome = argv[1];
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }

    // Enter the SQL shell loop
    while (true) {
        cout << "SQL> ";
        string query;
        getline(cin, query);
        if (query.length() == 0)
            continue;  // blank line -- just skip
        if (query == "quit")
            break;  // only way to get out

        // use the Hyrise sql parser to get us our AST
        SQLParserResult *result = SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            cout << "invalid SQL: " << query << endl;
            continue;
        }

        // execute the statement
        for (uint i = 0; i < result->size(); ++i) {
            cout << execute(result->getStatement(i)) << endl;
        }
    }
    return EXIT_SUCCESS;
}
