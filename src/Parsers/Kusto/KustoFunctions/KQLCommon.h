#include <Parsers/IParser.h>

namespace DB
{
bool directMapping(String & out, IParser::Pos & pos, const String & ch_fn);
String getArgument(const String & function_name, IParser::Pos & pos);
String getConvertedArgument(const String & fn_name, IParser::Pos & pos);
String getExpression(IParser::Pos & pos);
String getKQLFunctionName(IParser::Pos & pos);
std::optional<String> getOptionalArgument(const String & function_name, IParser::Pos & pos);
String
kqlCallToExpression(const String & function_name, std::initializer_list<std::reference_wrapper<const String>> params, uint32_t max_depth);
void validateEndOfFunction(const String & fn_name, IParser::Pos & pos);
}
