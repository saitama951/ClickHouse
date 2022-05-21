#include <memory>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
//#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserKQLQuery.h>
#include <Parsers/ParserKQLTable.h>
#include <Parsers/ParserKQLProject.h>
#include <Parsers/ParserKQLFilter.h>
#include <Parsers/ParserKQLSort.h>
#include <Parsers/ParserKQLSummarize.h>
#include <Parsers/ParserKQLFilter.h>
#include <Parsers/ParserKQLLimit.h>

namespace DB
{

bool ParserKQLBase :: parsePrepare(Pos & pos)
{
   op_pos.push_back(pos);
   return true;
}

String ParserKQLBase :: getExprFromToken(Pos pos)
{
    String res;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark)
    {
        res = res + String(pos->begin,pos->end) +" ";
        ++pos;
    }
    return res;
}

bool ParserKQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;

    
    ParserKQLFilter KQLfilter_p;
    ParserKQLLimit KQLlimit_p;
    ParserKQLProject KQLproject_p;
    //ParserKQLSort KQLsort_p;
    ParserKQLSummarize KQLsummarize_p;
    ParserKQLTable KQLtable_p;
    
    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr where_expression;
    ASTPtr group_expression_list;
    ASTPtr order_expression_list;
    ASTPtr limit_length;

    std::unordered_map<std::string, ParserKQLBase * > KQLParser = {
       // { "filter",&KQLfilter_p},
       // { "where",&KQLfilter_p},
        { "limit",&KQLlimit_p},
        { "take",&KQLlimit_p},
        { "project",&KQLproject_p},
       // { "sort",&KQLsort_p},
       // { "order",&KQLsort_p},
        { "summarize",&KQLsummarize_p},
        { "table",&KQLtable_p}
    };

    std::vector<std::pair<String, Pos>> operation_pos;
    
    operation_pos.push_back(std::make_pair("table",pos));

    while (!pos->isEnd()) 
    {
        ++pos;
        if (pos->type == TokenType::PipeMark)
        {   
            ++pos;
            String KQLOperator(pos->begin,pos->end); 
            if (pos->type != TokenType::BareWord || KQLParser.find(KQLOperator) == KQLParser.end())
                return false;            
            ++pos;
            operation_pos.push_back(std::make_pair(KQLOperator,pos));
        }
    }

    for (auto &op_pos : operation_pos)
    {
        auto KQLOperator = op_pos.first;
        auto npos = op_pos.second; 
        if (!npos.isValid())
            return false;

        if (!KQLParser[KQLOperator]->parsePrepare(npos))
            return false;
    }

    if (!KQLtable_p.parse(pos, tables, expected))
        return false;

    if (!KQLproject_p.parse(pos, select_expression_list, expected))
        return false;

    if (!KQLlimit_p.parse(pos, limit_length, expected))
        return false;
    //if (!KQLfilter_p.parse(pos, where_expression, expected))
        //return false;
    //if (!KQLsort_p.parse(pos, order_expression_list, expected))
        // return false;
        
    if (!KQLsummarize_p.parse(pos, select_expression_list, expected))
         return false;
    else
        group_expression_list = KQLsummarize_p.group_expression_list;

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));

    return true;
}

}
