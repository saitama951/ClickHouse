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
#include <Parsers/ParserKQLProject.h>
namespace DB
{

bool ParserKQLProject :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;
    String expr;
    if (op_pos.empty())
        expr = "*";
    else 
    {
        for (auto it = op_pos.begin(); it != op_pos.end(); ++it)
        {
            pos = *it ;
            while (!pos->isEnd() && pos->type != TokenType::PipeMark)
            {
                if (pos->type == TokenType::BareWord)
                {
                    String tmp(pos->begin,pos->end);

                    if (it != op_pos.begin() && columns.find(tmp) == columns.end())
                        return false;
                    columns.insert(tmp);
                }
                ++pos;
            }
        }
        expr = getExprFromToken(op_pos.back());
    }

    Tokens tokens(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos newPos(tokens, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(newPos, node, expected))
        return false; 

    pos = begin;

    return true;
}



}
