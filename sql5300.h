//
//  sql5300.h
//  milestone1
//
//  Created by lili hao on 4/8/21.
//  Copyright © 2021 lili hao. All rights reserved.
//

#ifndef sql5300_h
#define sql5300_h

#endif /* sql5300_h */

//#include <stdlib.h>
#include <string>

// include the sql parser
#include "SQLParser.h"

// contains printing utilities
#include "sqlhelper.h"

// for join function
#include <vector>
#include <iostream>

using namespace std;
using namespace hsql;

std::string QUIT = "quit";
std::string ENVHOME = "cpsc5300/data";
std::string DBNAME = "mybdb.db";
const unsigned int BLOCK_SZ = 4096;

string TableRefInfoToString(TableRef);
string sqlStatementToString(const SQLStatement);
string SelectStatementToString(const SelectStatement);
string ExpressionToString(hsql::Expr*);
string OperatorExpressionToString(hsql::Expr*);

class Sql5300 {
public:
    void initialize_db_env(std::string, std::string);
    string execute(std::string);
    //string sqlStatementToString(const SQLStatement);

};
