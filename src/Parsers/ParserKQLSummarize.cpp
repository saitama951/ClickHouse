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
#include <Parsers/ParserKQLSummarize.h>

namespace DB
{

bool ParserKQLSummarize :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;
    if (op_pos.size() != 1 )  // now only support one summarize
        return false;

    //summarize avg(age) by FirstName  ==> select FirstName,avg(Age) from Customers3 group by FirstName
    /*
    summarize has syntax :

    T | summarize [SummarizeParameters] [[Column =] Aggregation [, ...]] [by [Column =] GroupExpression [, ...]]

    right now , we only support:

    T | summarize Aggregation [, ...] [by GroupExpression  [, ...]]
    Aggregation -> the Aggregation function on column
    GroupExpression - > columns
    */
    auto begin = pos;

    pos = op_pos.back();  
    String exprAggregation;
    String exprGroupby;
    String exprColumns;

    bool groupby = false;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark &&  pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin,pos->end) == "by")
            groupby = true;
        else 
        {
            if (groupby) 
                exprGroupby = exprGroupby + String(pos->begin,pos->end) +" ";
            else
                exprAggregation = exprAggregation + String(pos->begin,pos->end) +" ";
        }
        ++pos;
    }

    if (exprGroupby.empty())
        exprColumns =exprAggregation;
    else
    {
        if (exprAggregation.empty())
            exprColumns = exprGroupby;
        else
            exprColumns = exprGroupby + "," + exprAggregation;
    }

    Tokens tokenColumns(exprColumns.c_str(), exprColumns.c_str()+exprColumns.size());
    IParser::Pos posColumns(tokenColumns, pos.max_depth);
    if (!ParserNotEmptyExpressionList(true).parse(posColumns, node, expected))
        return false; 

    if (groupby)
    {
        Tokens tokenGroupby(exprGroupby.c_str(), exprGroupby.c_str()+exprGroupby.size());
        IParser::Pos postokenGroupby(tokenGroupby, pos.max_depth);
        if (!ParserNotEmptyExpressionList(false).parse(postokenGroupby, group_expression_list, expected))
            return false; 
    }

    pos =begin;
    return true;
 
}

}
