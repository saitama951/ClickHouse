#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserKQLBase : public IParserBase
{
public:
    virtual bool parsePrepare(Pos & pos) ;

protected:
    std::vector<Pos> op_pos;
    std::vector<String> expresions;
    String getExprFromToken(Pos pos);
};

class ParserKQLQuery : public IParserBase
{
public:
    enum class KQLOperator : uint8_t
    {
        filter,
        limit,      
        table,
        order_by,
        project,
        sort_by,
        summarize,
        take,
        where,
    };

protected:
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
