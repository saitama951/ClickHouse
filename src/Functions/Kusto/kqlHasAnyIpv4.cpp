#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KqlFunctionBase.h>
#include <iostream>
#include <regex>

namespace DB
{
template <typename Name, bool is_any>
class FunctionKqlHasIpv4Generic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlHasIpv4Generic>(std::move(context)); }

    explicit FunctionKqlHasIpv4Generic(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlHasIpv4Generic() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return is_any ? 0 : 2; }
    bool isVariadic() const override { return is_any ? true : false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto argsLength = arguments.size();

        if (argsLength < 2)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                + ", should be 2 or more.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (!isStringOrFixedString(arguments.at(0).type))
        {
            throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (!isStringOrFixedString(arguments.at(1).type) && !isArray(arguments.at(1).type))
        {
            throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (isStringOrFixedString(arguments.at(1).type))
        {
            if(is_any)
            {
                for (size_t i = 2; i < argsLength; i++)
                {
                    if (!isStringOrFixedString(arguments.at(i).type))
                    {
                        throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    }
                }
            }
        }

        else if (is_any == false || !isArray(arguments.at(1).type))
        {
            throw Exception("Illegal type of argument of function " + getName(), ErrorCodes::BAD_ARGUMENTS);
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const override
    {
        auto argsLength = arguments.size();
        std::vector<std::string> ipList;
        auto bool_type = std::make_shared<DataTypeUInt8>();

        if (isStringOrFixedString(arguments.at(1).type))
        {
            for (size_t i = 1; i < argsLength; i++)
            {
                const ColumnsWithTypeAndName isipv4string_args = {arguments[i]};
                auto isipv4 = FunctionFactory::instance()
                    .get("isIPv4String", context)
                    ->build(isipv4string_args)
                    ->execute(isipv4string_args, result_type, input_rows_count);
                if (isipv4->getUInt(0) == 1)
                {
                    std::string ip = arguments[i].column->getDataAt(0).toString();
                    ipList.push_back(ip);
                }
            }
        }
            
        else if (isArray(arguments.at(1).type))
        {
            Field array0;
            arguments[1].column->get(0, array0);
            size_t len0 = array0.get<Array>().size();

            for(size_t k = 0; k < len0; k++)
            {
                if (array0.get<Array>().at(k).getType() == Field::Types::String)
                {
                    ColumnPtr column_ip = DataTypeString().createColumnConst(1, array0.get<Array>().at(k));
                    const ColumnsWithTypeAndName isipv4string_args = {ColumnWithTypeAndName(column_ip, std::make_shared<DataTypeString>(), "ip")};

                    auto isipv4 = FunctionFactory::instance()
                        .get("isIPv4String", context)
                        ->build(isipv4string_args)
                        ->execute(isipv4string_args, result_type, input_rows_count);
                    if (isipv4->getUInt(0) == 1)
                    {
                        ipList.push_back(toString(array0.get<Array>().at(k)));
                    }
                }
            }
        }

        if (!ipList.empty())
        {
            std::string source = arguments[0].column->getDataAt(0).toString();
            std::regex ip_finder("([^[:alnum:]]|^)([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})([^[:alnum:]]|$)");
            std::smatch matches;
            bool foundMatch = false;

            if(std::regex_search(source, matches, ip_finder))
            {
                for (size_t i = 0; i < matches.size(); i++)
                {
                    if (std::any_of(ipList.begin(), ipList.end(), [i, matches](const std::string & str) -> bool { return str == matches[i]; }))
                    {
                        foundMatch = true;
                    }
                }
                if (foundMatch == true)
                    return bool_type->createColumnConst(input_rows_count,1); 
            }
        }
        return bool_type->createColumnConst(input_rows_count,0); 
    }

private:
    ContextPtr context;
};

struct NameKqlHasAnyIpv4
{
    static constexpr auto name = "kql_has_any_ipv4";
};

struct NameKqlHasIpv4
{
    static constexpr auto name = "kql_has_ipv4";
};

using FunctionKqlHasAnyIpv4 = FunctionKqlHasIpv4Generic<NameKqlHasAnyIpv4, true>;
using FunctionKqlHasIpv4    = FunctionKqlHasIpv4Generic<NameKqlHasIpv4, false>;

REGISTER_FUNCTION(KqlHasIpv4Generic)
{
    factory.registerFunction<FunctionKqlHasAnyIpv4>();
    factory.registerFunction<FunctionKqlHasIpv4>();
}
}
