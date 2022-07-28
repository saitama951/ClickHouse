#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTopHitters.h>
namespace DB
{
bool ParserKQLTopHitters ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;
    String expr;
    pos = op_pos.back();
    bool first_argument = true;
    String limit;
    String column_name;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (first_argument)
        {
            first_argument = false;
            limit += String(pos->begin, pos->end);
            auto is_number = [&] {
                for (const auto ch : limit)
                {
                    if (!isdigit(ch))
                        return false;
                }
                return true;
            };

            if (!is_number())
                return false;
        }
        else
        {
            if (String(pos->begin, pos->end) != "of")
                column_name += String(pos->begin, pos->end);
        }
        ++pos;
    }
    expr = "topK(" + limit + ")(" + column_name + ")";
    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(new_pos, node, expected))
        return false;

    pos = begin;

    return true;
}

}
