#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ParserKQLQuery.h>

namespace DB
{

class ParserKQLFilter : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL where"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
