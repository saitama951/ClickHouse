#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ParserKQLQuery.h>

namespace DB
{

class ParserKQLLimit : public ParserKQLBase
{

protected:
    const char * getName() const override { return "KQL limit"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
