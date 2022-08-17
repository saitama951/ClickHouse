#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>

#include <format>

namespace DB
{

bool ArrayConcat::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArrayIif::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
    return false;

    const auto conditions = getArgument(function_name, pos);
    const auto if_true = getArgument(function_name, pos);
    const auto if_false = getArgument(function_name, pos);

    out = std::format(
        "arrayMap(x -> if(x.1 != 0, x.2, x.3), arrayZip({0}, arrayResize({1}, length({0}), null), arrayResize({2}, length({0}), null)))",
        conditions,
        if_true,
        if_false);

    return true;
}

bool ArrayIndexOf::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String array = getConvertedArgument(fn_name, pos);
    ++pos;
    const auto needle = getConvertedArgument(fn_name, pos);
    out = "minus(indexOf(" + array + ", " + needle + ") , 1)";
  
    return true;
}

bool ArrayLength::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "length");
}

bool ArrayReverse::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArrayRotateLeft::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArrayRotateRight::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArrayShiftLeft::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArrayShiftRight::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArraySlice::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArraySortAsc::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    String first_arg = getConvertedArgument(fn_name, pos);
    if(pos->type == TokenType::Comma)
    {
        ++pos;
        String second_arg = getConvertedArgument(fn_name, pos);
        size_t position = std::string::npos;
        while (second_arg.find(" ") != std::string::npos)
        {
            position  = second_arg.find(" ");
            second_arg.erase(position, 1);
        }
        if(second_arg == "true" || second_arg == "false")
        {
            if(second_arg == "true")
                out = "arraySort(" + first_arg + ")";
            else
            {
                int nulls_total = 0;
                while (first_arg.find(" ") != std::string::npos)
                {
                    position  = first_arg.find(" ");
                    first_arg.erase(position, 1);
                }

                while (first_arg.find("null") != std::string::npos)
                {
                    position  = first_arg.find("null");
                    first_arg.erase(position, 4);
                    if(first_arg[position] == ',')
                        first_arg[position] = ' ';
                    nulls_total += 1;
                }

                while (first_arg.find("NULL") != std::string::npos)
                {
                    position  = first_arg.find("NULL");
                    first_arg.erase(position, 4);
                    if(first_arg[position] == ',')
                        first_arg[position] = ' ';
                    nulls_total += 1;
                }

                int index = first_arg.size() - 1;
                while(index > 0)
                {
                    if(first_arg[index] == '\'' || first_arg[index] == '\"')
                        break;
                    if(first_arg[index] == ',')
                    {
                        first_arg[index] = ' ';
                        break;
                    }
                    index -= 1;
                }
                String null_array = "[";
                if(nulls_total > 0)
                {
                    while(nulls_total > 0)
                    {
                        null_array += "null";
                        if(nulls_total > 1)
                            null_array += ", ";
                        nulls_total -= 1;
                    }
                    null_array += "]";
                    out = "arrayConcat( " + null_array + " , arraySort( " + first_arg + " ) )";
                }
                else
                    out = "arraySort(" + first_arg + ")";

            }
        }
        else
        {
            out = "arraySort((x, y) -> y, " + second_arg + "," + first_arg + ")"; 
        }
    }
    else
        out = "arraySort(" + first_arg + ")";
    return true;
}

bool ArraySortDesc::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    String first_arg = getConvertedArgument(fn_name, pos);
    if(pos->type == TokenType::Comma)
    {
        ++pos;
        String second_arg = getConvertedArgument(fn_name, pos);
        size_t position = std::string::npos;
        while (second_arg.find(" ") != std::string::npos)
        {
            position  = second_arg.find(" ");
            second_arg.erase(position, 1);
        }
        if(second_arg == "true" || second_arg == "false")
        {
            if(second_arg == "true")
                out = "arrayReverseSort(" + first_arg + ")";
            else
            {
                int nulls_total = 0;
                while (first_arg.find(" ") != std::string::npos)
                {
                    position  = first_arg.find(" ");
                    first_arg.erase(position, 1);
                }

                while (first_arg.find("null") != std::string::npos)
                {
                    position  = first_arg.find("null");
                    first_arg.erase(position, 4);
                    if(first_arg[position] == ',')
                        first_arg[position] = ' ';
                    nulls_total += 1;
                }

                while (first_arg.find("NULL") != std::string::npos)
                {
                    position  = first_arg.find("NULL");
                    first_arg.erase(position, 4);
                    if(first_arg[position] == ',')
                        first_arg[position] = ' ';
                    nulls_total += 1;
                }

                int index = first_arg.size() - 1;
                while(index > 0)
                {
                    if(first_arg[index] == '\'' || first_arg[index] == '\"')
                        break;
                    if(first_arg[index] == ',')
                    {
                        first_arg[index] = ' ';
                        break;
                    }
                    index -= 1;
                }
                String null_array = "[";
                if(nulls_total > 0)
                {
                    while(nulls_total > 0)
                    {
                        null_array += "null";
                        if(nulls_total > 1)
                            null_array += ", ";
                        nulls_total -= 1;
                    }
                    null_array += "]";
                    out = "arrayConcat( " + null_array + " , arrayReverseSort( " + first_arg + " ) )";
                }
                else
                    out = "arrayReverseSort(" + first_arg + ")";

            }
        }
        else
        {
            out = "arrayReverseSort((x, y) -> y, " + second_arg + "," + first_arg + ")"; 
        }
    }
    else
        out = "arrayReverseSort(" + first_arg + ")";
    return true;
}

bool ArraySplit::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ArraySum::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "arraySum");
}

bool BagKeys::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BagMerge::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BagRemoveKeys::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool JaccardIndex::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Pack::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool PackAll::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool PackArray::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Repeat::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool SetDifference::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool SetHasElement::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool SetIntersect::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool SetUnion::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool TreePath::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Zip::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
