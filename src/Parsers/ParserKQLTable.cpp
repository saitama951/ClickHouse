#include <memory>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserKQLQuery.h>
#include <Parsers/ParserKQLTable.h>

namespace DB
{

bool ParserKQLTable :: parsePrepare(Pos & pos)
{
   if (!op_pos.empty())
    return false;

   op_pos.push_back(pos);
   return true;
}

bool ParserKQLTable :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::unordered_set<String> SQLKeywords 
    ( {
        "SELECT",
        "INSERT",
        "CREATE",
        "ALTER",
        "SYSTEM",
        "SHOW",
        "GRANT",
        "REVOKE",
        "ATTACH",
        "CHECK",
        "DESCRIBE",
        "DESC",
        "DETACH",
        "DROP",
        "EXISTS",
        "KILL",
        "OPTIMIZE",
        "RENAME",
        "SET",
        "TRUNCATE",
        "USE",
        "EXPLAIN"
    } ); 

    if (op_pos.empty())
        return false;

    auto begin = pos;
    pos = op_pos.back();

    String tableName(pos->begin,pos->end);
    String tableNameUpcase(tableName);
    
    std::transform(tableNameUpcase.begin(), tableNameUpcase.end(),tableNameUpcase.begin(), toupper);

    if (SQLKeywords.find(tableNameUpcase) != SQLKeywords.end())
        return false;
    
    if (!ParserTablesInSelectQuery().parse(pos, node, expected))
        return false;
    pos = begin;

    return true;
}

}
