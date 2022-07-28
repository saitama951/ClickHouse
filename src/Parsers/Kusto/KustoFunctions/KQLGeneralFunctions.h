#pragma once

#include "IParserKQLFunction.h"

namespace DB
{
class Bin : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bin()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

}

