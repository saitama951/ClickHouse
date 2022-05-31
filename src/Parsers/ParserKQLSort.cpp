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
#include <Parsers/ParserKQLSort.h>

namespace DB
{

bool ParserKQLSort :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;

    auto begin = pos;
    bool hasDir = false;
    std::vector <bool> hasDirection;
    ParserOrderByExpressionList order_list;
    ASTPtr order_expression_list;
    ParserKeyword by("by");

    pos = op_pos.back();  // sort only affected by last one

    if (!by.ignore(pos, expected))
        return false;

    if (!order_list.parse(pos,order_expression_list,expected))
        return false;
    if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        return false;

    pos = op_pos.back();  
    while (!pos->isEnd() && pos->type != TokenType::PipeMark)
    {
        String tmp(pos->begin,pos->end);
        if (tmp == "desc" or tmp == "asc")
            hasDir = true;

        if (pos->type == TokenType::Comma)
        {
            hasDirection.push_back(hasDir);
            hasDir = false;
        }

        ++pos;
    }
    hasDirection.push_back(hasDir);

    for (unsigned long i = 0; i < order_expression_list->children.size(); ++i)
    {
        if (!hasDirection[i])
        {
            auto order_expr =  order_expression_list->children[i]->as<ASTOrderByElement>();
            order_expr->direction = -1; // default desc
            if (!order_expr->nulls_direction_was_explicitly_specified)
                order_expr->nulls_direction = -1;
            else
                order_expr->nulls_direction = order_expr->nulls_direction == 1 ? -1 : 1;

        }
    }

    node = order_expression_list;
    
    pos =begin;
    return true;
} 

}
