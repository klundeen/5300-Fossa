#include <SQLParser.h>
#include <string>

#ifndef EXECUTE_H_
#define EXECUTE_H_

/**
 * Static class that contains execute function and helpers
 */
class Execute {
public:
  /**
   * Executes a given parse tree
   * @param tree      A SQL parse tree root
   */
  static std::string execute(const hsql::SQLParserResult *tree);

protected:
  // Protect these classes because they are only called in execute
  static std::string create(const hsql::CreateStatement *create);
  static std::string select(const hsql::SelectStatement *select);
  static std::string expr(const hsql::Expr *expr);
  static std::string table(const hsql::TableRef *table);
  static std::string join(const hsql::JoinDefinition *join);

private:
  Execute();
};

#endif // EXECUTE_H_
