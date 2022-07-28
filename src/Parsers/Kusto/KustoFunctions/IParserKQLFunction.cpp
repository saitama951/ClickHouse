#include "IParserKQLFunction.h"

namespace DB
{
bool IParserKQLFunction::convert(String & out,IParser::Pos & pos)
{
    return wrapConvertImpl(pos, IncreaseDepthTag{}, [&]
    {
        bool res = convertImpl(out,pos);
        if (!res)
            out = "";
        return res;
    });
}
}
