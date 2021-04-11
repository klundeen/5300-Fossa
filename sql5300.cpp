//
//  sql5300.cpp
//  milestone1
//
//  Created by lili hao on 4/8/21.
//  Copyright © 2021 lili hao. All rights reserved.
//

// include the Berkeley DB
#include "sql5300.h"
#include <db_cxx.h>



void Sql5300::initialize_db_env(std::string envHome, std::string dbName) {
    
    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + envHome;
    
    std::cout << "Database Environment directory: " << envdir << std::endl;
    std::cout << "database name: " << dbName << std::endl;
    std::cout << "enter 'quit' to end " << std::endl;

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
    
    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ); // Set record length to 4K
    db.open(NULL, dbName.c_str(), NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there
    
    /*
    char block[BLOCK_SZ];
    Dbt data(block, sizeof(block));
    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = 1;
    strcpy(block, "hello!");
    db.put(NULL, &key, &data, 0);  // write block #1 to the database
    
    Dbt rdata;
    db.get(NULL, &key, &rdata, 0); // read block #1 from the database
    std::cout << "Read (block #" << block_number << "): '" << (char *)rdata.get_data() << "'";
    std::cout << " (expect 'hello!')" << std::endl;
    */
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col column definition to unparse
 * @return SQL equivalent to *col
 */
string columnDefinitionToString(const hsql::ColumnDefinition *col) {
    string ret(col->name);
    switch(col->type) {
        case hsql::ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case hsql::ColumnDefinition::INT:
            ret += " INT";
            break;
        case hsql::ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
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
 * Convert the hyrise SQL Expression AST back into the equivalent string
 * reference from "printExpression" in sqlhelper.cpp
 */
string ExpressionToString(Expr* expr) {
    string ret;

    switch (expr->type) {
        case kExprStar:
            ret += "*";
            break;
        case kExprColumnRef:
            if (expr->table != NULL) {
                ret += string(expr->table);
                ret += ".";
            };
            ret += expr->name;
            break;
        case kExprLiteralFloat:
            ret += to_string(expr->fval);
            break;
        case kExprLiteralInt:
            ret += to_string(expr->ival);
            break;
        case kExprLiteralString:
            ret += expr->name;
            break;
        case kExprFunctionRef:
            ret += expr->name;
            break;
        case kExprOperator:
            ret += OperatorExpressionToString(expr);
            break;
        default:
            fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
            return ret;
    }
    if (expr->alias != NULL) {
        ret += expr->alias;
    }
    return ret;
}

/**
 * Convert the hyrise SQL OperatorExpression AST back into the equivalent SQL string
 * reference from "printOperatorExpression" in sqlhelper.cpp
 */
string OperatorExpressionToString(Expr* expr) {
    string ret;
    if (expr == NULL) {
        return ret;
    }
    
    switch (expr->opType) {
        case Expr::SIMPLE_OP:
            ret += ExpressionToString(expr->expr) + " " + expr->opChar + " " + ExpressionToString(expr->expr2);
            break;
        case Expr::AND:
            ret += " AND ";
            break;
        case Expr::OR:
            ret += " OR ";
            break;
        case Expr::NOT:
            ret += " NOT ";
            break;
        default:
            ret += expr->opType;
            break;
    }

    return ret;
}

/**
 * Convert the hyrise SQL table reference AST back into the equivalent SQL string
 * reference from "printTableRefInfo" in sqlhelper.cpp
 */
string TableRefInfoToString(TableRef* table) {
    string ret;
    
    switch (table->type) {
        case kTableName:
            ret += table->name;
            if (table->alias != NULL) {
                ret += " AS " + string(table->alias);
            }
            break;
        case kTableSelect:
            cout << "####### not implemented! - kTableSelect" << endl;
            //SelectStatementToString(table->select);
            break;
        case kTableJoin:

            ret += TableRefInfoToString(table->join->left);
            switch (table->join->type) {
                case kJoinInner:
                    ret += " JOIN ";
                    break;
                case kJoinLeft:
                    ret += " LEFT JOIN ";
                    break;
                default:
                    ret += " NOT IMPLMENTED JOIN";
                    break;
            }
            
            ret += TableRefInfoToString(table->join->right);
            
            if (table->join->condition != NULL ) {
                ret += " ON " + ExpressionToString(table->join->condition);
            }
            
            break;
        case kTableCrossProduct:
            // for multiple tables in "FROM", delimit in ','
            int ptr = 1;
            string delim = ", ";
            for (TableRef* tbl : *table->list) {
                ret += TableRefInfoToString(tbl);
                if (ptr < table->list->size()) {
                    ret += delim;
                    ptr++;
                }
            }
            break;
    }
    
    return ret;
}

/**
 * Convert the hyrise SQL SelectStatement AST back into the equivalent SQL
 * reference from "printSelectStatementInfo" in sqlhelper.cpp
 */
string SelectStatementToString(const SelectStatement* stmt) {
    string sqlstmt = "SELECT ";
    
    // for the purpose of join
    int ptr = 1;
    string delim = ", ";
    
    for (Expr* expr : *stmt->selectList){
        sqlstmt.append(ExpressionToString(expr));
        if (ptr < stmt->selectList->size()) {
            sqlstmt.append(delim);
            ptr++;
        }
    }
    
    sqlstmt.append(" FROM ");
    sqlstmt.append(TableRefInfoToString(stmt->fromTable));

    
    if (stmt->whereClause != NULL) {
        sqlstmt.append(" WHERE ");
        sqlstmt.append(ExpressionToString(stmt->whereClause));
    }
    
    if (stmt->unionSelect != NULL) {
        sqlstmt.append(SelectStatementToString(stmt->unionSelect));
    }

    return sqlstmt;

}


/**
 * Convert the hyrise SQL statement AST back into the equivalent SQL
 * reference from "printStatementInfo" in sqlhelper.cpp
 */
string sqlStatementToString(const SQLStatement* stmt) {
    string sqlstmt;
    switch (stmt->type()) {
        case kStmtSelect:
            //printSelectStatementInfo((const SelectStatement*) stmt, 0);
            sqlstmt += SelectStatementToString((const SelectStatement*) stmt);
            break;
        case kStmtInsert:
            //not implemented
            //printInsertStatementInfo((const InsertStatement*) stmt, 0);
            break;
        case kStmtCreate:
            //printCreateStatementInfo((const CreateStatement*) stmt, 0);
            sqlstmt += executeCreate((const CreateStatement *) stmt);

            break;
        default:
            break;
    }
    return sqlstmt;
}

string Sql5300::execute(std::string a_query) {
    // parse a given query
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(a_query);

    // check whether the parsing is successful
    if (result->isValid()) {
        //printf("Parsed successfully!\n");
        //printf("Number of statements: %lu\n", result->size());
        
        for (uint i = 0; i < result->size(); ++i) {
            // Print a statement summary.
            const SQLStatement* stmt = result->getStatement(i);
            cout << sqlStatementToString(stmt) << endl;
            
        }
        
        delete result;
        return "";
        
    } else {
        fprintf(stderr, "Invalid SQL: %s\n", a_query.c_str());
        /*fprintf(stderr, "%s (L%d:%d)\n",
                result->errorMsg(),
                result->errorLine(),
                result->errorColumn());*/
        delete result;
        return "";
    }

}



int main(int argc, const char * argv[]) {

    // check parameters for database environment path
    if (argc <= 1) {
        std::cout << "usage: " << argv[0] << " ~/" << ENVHOME << std::endl << std::endl;
        std::cout << "please create the directory by running:" << std::endl;
        std::cout << "       mkdir -p ~/" << ENVHOME << std::endl;
        exit(1);
    }
    
    // prepare database environment
    Sql5300 sql5300;
    sql5300.initialize_db_env(ENVHOME, DBNAME);
    
    // SQL shell loop
    std::string query;
    while (true) {
        std::cout << "SQL> ";
        std::getline(std::cin, query);
        if (query == QUIT) {
            break;
        }
        
        sql5300.execute(query);
    }
    
    return 0;
}
