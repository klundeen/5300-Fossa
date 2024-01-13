#include "Execute.h"
#include <ios>
#include <sstream>
#include <stdexcept>

std::string Execute::execute(const hsql::SQLParserResult *tree) {
  std::stringstream builder;
  for (size_t i = 0; i < tree->size(); i++) {
    const hsql::SQLStatement *statement = tree->getStatement(i);
    switch (statement->type()) {
    case hsql::kStmtCreate:
      builder << Execute::create((const hsql::CreateStatement *)statement);
      break;
    case hsql::kStmtSelect:
      builder << Execute::select((const hsql::SelectStatement *)statement);
      break;
    default:
      throw std::logic_error("Not yet implemented");
      break;
    }
  }

  return builder.str();
}

std::string Execute::create(const hsql::CreateStatement *create) {
  std::stringstream builder;
  builder << "CREATE ";
  switch (create->type) {
  case hsql::CreateStatement::kTable:
    builder << create->tableName << " (";
    for (hsql::ColumnDefinition *col : *create->columns) {
      builder << col->name << ' ';
      switch (col->type) {
      case hsql::ColumnDefinition::TEXT:
        builder << "TEXT";
        break;
      case hsql::ColumnDefinition::DOUBLE:
        builder << "DOUBLE";
        break;
      case hsql::ColumnDefinition::INT:
        builder << "INT";
        break;
      default:
        builder << "...";
        break;
      }
      builder << ", ";
    }
    builder.seekp(-2, std::ios_base::cur); // Remove last seperator
    builder << ')';
    break;
  default:
    throw std::logic_error("Not yet implemented");
    break;
  }
  return builder.str();
}

std::string Execute::select(const hsql::SelectStatement *select) {
  std::stringstream builder;
  builder << "SELECT ";
  for (hsql::Expr *expr : *select->selectList) {
    builder << Execute::expr(expr) << ", ";
  }
  builder.seekp(-2, std::ios_base::cur); // Remove last seperator

  builder << " FROM " << Execute::table(select->fromTable);

  if (select->whereClause) {
    builder << " WHERE " << Execute::expr(select->whereClause);
  }
  return builder.str();
}

std::string Execute::join(const hsql::JoinDefinition *join) {
  std::stringstream builder;
  builder << Execute::table(join->left);
  switch (join->type) {
  case hsql::kJoinLeft:
    builder << " LEFT JOIN ";
    break;
  case hsql::kJoinInner:
    builder << " JOIN ";
    break;
  default:
    throw std::logic_error("Not yet implemented");
    break;
  }
  builder << Execute::table(join->right) << " ON "
          << Execute::expr(join->condition);
  return builder.str();
}

std::string Execute::table(const hsql::TableRef *table) {
  std::stringstream builder;

  switch (table->type) {
  case hsql::kTableName:
    builder << table->name;
    if (table->alias) {
      builder << " AS " << table->alias;
    }
    break;
  case hsql::kTableJoin:
    builder << Execute::join(table->join);
    break;
  case hsql::kTableCrossProduct:
    for (hsql::TableRef *t : *table->list) {
      builder << Execute::table(t) << ", ";
    }
    builder.seekp(-2, std::ios_base::cur); // Remove last seperator
    builder << ' ';                        // Write over the comma
    break;
  default:
    throw std::logic_error("Not yet implemented");
    break;
  }
  return builder.str();
}

std::string Execute::expr(const hsql::Expr *expr) {
  std::stringstream builder;
  switch (expr->type) {
  case hsql::kExprStar:
    builder << '*';
    break;
  case hsql::kExprOperator:
    builder << Execute::expr(expr->expr) << ' ' << expr->opChar << ' '
            << Execute::expr(expr->expr2);
    break;
  case hsql::kExprColumnRef:
    if (expr->table) {
      builder << expr->table << '.';
    }
    if (expr->alias) {
      builder << expr->alias;
    } else {
      builder << expr->name;
    }
    break;
  case hsql::kExprLiteralInt:
    builder << expr->ival;
    break;
  default:
    throw std::logic_error("Not yet implemented");
    break;
  }
  return builder.str();
}
