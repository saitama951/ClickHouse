#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLTopHitters : public ParserKQLBase
{
public:

protected:
    const char * getName() const override { return "KQL Top Hitters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    std::unordered_set <String> columns;
};

}
