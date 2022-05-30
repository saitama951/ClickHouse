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
#include <Parsers/ParserKQLFilter.h>
#include <Parsers/ParserKQLOperators.h>

namespace DB
{

bool ParserKQLFilter :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;
    Pos begin = pos;
    String expr;

    KQLOperators convetor;

    for (auto it = op_pos.begin(); it != op_pos.end(); ++it)
    {
        pos = *it;
        if (expr.empty())
            expr = "(" + convetor.getExprFromToken(pos) +")";
        else
            expr = expr + " and (" + convetor.getExprFromToken(pos) +")";
    }

    Tokens tokenFilter(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos posFilter(tokenFilter, pos.max_depth);
    if (!ParserExpressionWithOptionalAlias(false).parse(posFilter, node, expected))
        return false;
    pos = begin;

    return true;
}



}
