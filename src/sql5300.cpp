/**
 * @file sql5300.cpp - Entry file for relational sql shell
 * @author Samuel Monson
 */
#include "Execute.h"
#include "heap_storage.h"
#include "not_impl.h"
#include <SQLParser.h>
#include <cstddef>
#include <cstring>
#include <db_cxx.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

using hsql::SQLParser;
using hsql::SQLParserResult;

// Global handler for DbEnv
DbEnv *_DB_ENV;

const char *DB_NAME = "cs5300.db";
const std::string QUIT = "quit";

/**
 * Prints the program usage and exits
 * @param exec      Calling name of the executable
 */
void bad_usage(char *exec) {
  std::cerr << "Usage: " << exec << " DB\n\nOptions\n"
            << "DB\tpath to database\n";
  std::exit(1);
}

/**
 * Ensures given args match program requirements
 * @param argc      C-style argc
 * @param argv      C-style argv
 */
void parse_args(int argc, char *argv[]) {
  if (argc != 2) {
    bad_usage(argv[0]);
  }
}

/**
 * Open a DB environment at the given dir
 * @param env     Location to return a DbEnv on
 * @param envdir  Path to open dbenv in
 */
void openDBEnv(DbEnv &env, std::string &envdir) {
  env.set_message_stream(&std::cout);
  env.set_error_stream(&std::cerr);
  env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
}

/**
 * Spawns a SQL shell in the given DB
 * @param db     DB to open shell for
 */
void sqlShell(void) {
  char *buf; // Buffer for readline
  std::string input;
  // By default readline binds tab to autocomplete, which we dont implement.
  // This rebinds it back to tab.
  rl_bind_key('\t', rl_insert);

  while ((buf = readline("SQL> ")) != nullptr) {
    // Skip if line is blank
    if (strlen(buf) == 0) {
      continue;
    }

    add_history(buf);

    // Switch from readline buffer to a c++ string
    input = std::string(buf);
    delete[] buf;

    // If we ^d break the loop
    if (std::cin.eof()) {
      break;
    }

    // BEGIN: SHELL COMMANDS //
    if (input == QUIT) {
      break;
    } else if (input == "test") {
      bool tester = test_heap_storage();
      std::cout << "test_heap_storage: " << (tester ? "ok" : "failed")
                << std::endl;
      continue;
    }

    // END:   SHELL COMMANDS //

    SQLParserResult *result = SQLParser::parseSQLString(input);

    if (result->isValid()) {
      try {
        std::cout << Execute::execute(result) << '\n';
      } catch (NotImplementedError &e) {
        std::cerr << e.what() << '\n';
      }
    } else {
      std::cerr << result->errorMsg() << '\n';
      std::cerr << input << '\n';
      for (int i = 0; i < result->errorColumn() - 1; i++) {
        std::cerr << '.';
      }
      std::cerr << "^ Here\n";
    }

    delete result;
  }
}

/**
 * Main entry of sql5300
 * @args DB      path to database
 */
int main(int argc, char *argv[]) {
  parse_args(argc, argv);
  std::string envdir = argv[1];

  DbEnv env(0U);
  openDBEnv(env, envdir);
  _DB_ENV = &env;

  sqlShell();

  return EXIT_SUCCESS;
}
