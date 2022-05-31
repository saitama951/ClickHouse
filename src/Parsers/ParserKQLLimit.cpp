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
#include <Parsers/ParserKQLLimit.h>
#include <cstdlib>
namespace DB
{

bool ParserKQLLimit :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;

    auto begin = pos;
    Int64 minLimit = -1;
    auto destpos = pos;
    for (auto it = op_pos.begin(); it != op_pos.end(); ++it)
    {

        pos = *it;

        auto isNumber = [&]
        {
            for (auto ch = pos->begin ; ch < pos->end; ++ch)
            {
                if (!isdigit(*ch))
                    return false;
            }
            return true;
        };

        if (!isNumber())
            return false;

        auto limitLength = std::strtol(pos->begin,nullptr, 10);
        if (-1 == minLimit) 
        {
            minLimit = limitLength;
            destpos = pos;
        }
        else 
        {
            if (minLimit > limitLength) 
            {
                minLimit = limitLength;
                destpos = pos;
            }
        }
    }

    if (!ParserExpressionWithOptionalAlias(false).parse(destpos, node, expected))
        return false;

    pos = begin;

    return true;
}

}
