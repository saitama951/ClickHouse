#include "KQLGeneralFunctions.h"

#include "KQLCommon.h"

namespace DB
{

bool Bin::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
