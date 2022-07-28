#include "IParserKQLFunction.h"

#include "KQLFunctionFactory.h"

#include <Parsers/Kusto/ParserKQLOperators.h>

#include <format>
#include <numeric>

namespace DB::ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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

bool IParserKQLFunction::directMapping(String & out, IParser::Pos & pos, const String & ch_fn)
{
    std::vector<String> arguments;

    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    String res;
    auto begin = pos;
    ++pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String argument = getConvertedArgument(fn_name, pos);
        arguments.push_back(argument);

        if (pos->type == TokenType::ClosingRoundBracket)
        {
            for (auto arg : arguments)
            {
                if (res.empty())
                    res = ch_fn + "(" + arg;
                else
                    res = res + ", " + arg;
            }
            res += ")";

            out = res;
            return true;
        }
        ++pos;
    }

    pos = begin;
    return false;
}

String IParserKQLFunction::getArgument(const String & function_name, IParser::Pos & pos)
{
    return getOptionalArgument(function_name, pos).value();
}

String IParserKQLFunction::getConvertedArgument(const String & fn_name, IParser::Pos & pos)
{
    String converted_arg;
    std::vector<String> tokens;
    std::unique_ptr<IParserKQLFunction> fun;

    if (pos->type == TokenType::ClosingRoundBracket)
        return converted_arg;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception("Need more argument(s) in function: " + fn_name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String token = String(pos->begin, pos->end);
        String new_token;
        if (!KQLOperators().convert(tokens, pos))
        {
            if (pos->type == TokenType::BareWord)
            {
                tokens.push_back(getExpression(pos));
            }
            else if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
            {
                break;
            }
            else
                tokens.push_back(token);
        }
        ++pos;
        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
            break;
    }
    for (auto token : tokens)
        converted_arg = converted_arg + token + " ";

    return converted_arg;
}

String IParserKQLFunction::getKQLFunctionName(IParser::Pos & pos)
{
    String fn_name = String(pos->begin, pos->end);
    ++pos;
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        --pos;
        return "";
    }
    return fn_name;
}

std::optional<String> IParserKQLFunction::getOptionalArgument(const String & function_name, IParser::Pos & pos)
{
    std::optional<String> argument;
    if (const auto & type = pos->type; type != TokenType::Comma && type != TokenType::OpeningRoundBracket)
        return {};

    ++pos;
    return getConvertedArgument(function_name, pos);
}

String IParserKQLFunction::kqlCallToExpression(
    const String & function_name, std::initializer_list<std::reference_wrapper<const String>> params, const uint32_t max_depth)
{
    const auto params_str = std::accumulate(
        std::cbegin(params),
        std::cend(params),
        String(),
        [](auto acc, const auto & param) { return (acc.empty() ? "" : ", ") + std::move(acc) + param.get(); });

    const auto kql_call = std::format("{}({})", function_name, params_str);
    Tokens call_tokens(kql_call.c_str(), kql_call.c_str() + kql_call.length());
    IParser::Pos tokens_pos(call_tokens, max_depth);
    return getExpression(tokens_pos);
}

void IParserKQLFunction::validateEndOfFunction(const String & fn_name, IParser::Pos & pos)
{
    if (pos->type != TokenType::ClosingRoundBracket)
        throw Exception("Too many arguments in function: " + fn_name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

String IParserKQLFunction::getExpression(IParser::Pos & pos)
{
    String arg = String(pos->begin, pos->end);
    if (pos->type == TokenType::BareWord)
    {
        String new_arg;
        auto fun = KQLFunctionFactory::get(arg);
        if (fun && fun->convert(new_arg, pos))
        {
            validateEndOfFunction(arg, pos);
            arg = new_arg;
        }
    }
    return arg;
}
}
