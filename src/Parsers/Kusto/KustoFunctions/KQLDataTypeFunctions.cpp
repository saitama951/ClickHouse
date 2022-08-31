#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>

#include <algorithm>
#include <format>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYNTAX_ERROR;
}

namespace DB
{
bool DatatypeBool::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toBool");
}

bool DatatypeDatetime::convertImpl(String &out,IParser::Pos &pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String datetime_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier)
        datetime_str = std::format("'{}'", String(pos->begin+1, pos->end -1));
    else if (pos->type == TokenType::StringLiteral)
        datetime_str = String(pos->begin, pos->end);
    else 
    {   auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if  (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        datetime_str = std::format("'{}'",String(start->begin,pos->end));
    }
    out = std::format("parseDateTime64BestEffortOrNull({},9,'UTC')", datetime_str);
    ++pos;
    return true;
}

bool DatatypeDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto appendToken = [&out, &pos]()
    {
        out.push_back('\'');
        out.append(pos->begin, pos->end);
        out.push_back('\'');
        ++pos;
    };

    const auto determineKey = [](const DB::TokenType token_type)
    {
        if (token_type == DB::TokenType::OpeningCurlyBrace)
            return "dictionary";
        else if (token_type == DB::TokenType::OpeningSquareBracket)
            return "array";
        else
            return "scalar";
    };

    const auto extractAsJSONString = [&function_name, &pos]()
    {
        const auto expression = std::invoke(
            [&function_name, &pos]
            {
                if (pos->type == TokenType::BareWord)
                    return getConvertedArgument(function_name, pos);

                std::string raw(pos->begin, pos->end);
                std::replace(raw.begin(), raw.end(), '"', '\'');
                ++pos;

                return raw;
            });

        return std::format("concat('\"', toString({0}), '\"')", expression);
    };

    ++pos;
    out = std::format("concat('{{ \"{0}\": ', ", determineKey(pos->type));
    while (!pos->isEnd() && pos->type != TokenType::ClosingRoundBracket)
    {
        if (const auto token_type = pos->type; token_type == TokenType::ClosingCurlyBrace || token_type == TokenType::ClosingSquareBracket
            || token_type == TokenType::Colon || token_type == TokenType::Comma || token_type == TokenType::OpeningCurlyBrace
            || token_type == TokenType::OpeningSquareBracket)
            appendToken();
        else if (token_type == TokenType::QuotedIdentifier || token_type == TokenType::StringLiteral)
            out.append(extractAsJSONString());
        else if (const std::string_view token(pos->begin, pos->end); token_type == TokenType::BareWord && token != "timespan")
        {
            if (token == "datetime")
                out.append(extractAsJSONString());
            else if (token == "dynamic")
                out.append(std::format("JSONExtractKeysAndValuesRaw({0})[1].2", getConvertedArgument(function_name, pos)));
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Encountered unsupported call {} when parsing {}", token, function_name);
        }
        else
            out.append(std::format("toString({})", getConvertedArgument(function_name, pos)));

        out.append(", ");
    }

    out.append("' }')");
    return true;
}

bool DatatypeGuid::convertImpl(String &out,IParser::Pos &pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String guid_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        guid_str = String(pos->begin+1, pos->end -1);
    else 
    {   auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if  (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        guid_str = String(start->begin,pos->end);
    }
    out = std::format("toUUID('{}')", guid_str);
    ++pos;
    return true;
}

bool DatatypeInt::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toInt32");
}

bool DatatypeLong::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toInt64");
}

bool DatatypeReal::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toFloat64");
}

bool DatatypeString::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeTimespan::convertImpl(String &out,IParser::Pos &pos)
{
    ParserKQLDateTypeTimespan time_span;
    ASTPtr node;
    Expected expected;

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;

    if (time_span.parse(pos, node, expected))
    {
        out = std::to_string(time_span.toSeconds());
        ++pos;
    }
    else
        throw Exception("Not a correct timespan expression: " + fn_name, ErrorCodes::BAD_ARGUMENTS);
    return true;
}

bool DatatypeDecimal::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}
}
