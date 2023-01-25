#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>

#include <format>

namespace DB
{
class FunctionKqlToString : public IFunction
{
public:
    static constexpr auto name = "kql_tostring";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlToString>(std::move(context)); }

    explicit FunctionKqlToString(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionKqlToString() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr
FunctionKqlToString::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, const size_t input_rows_count) const
{
    const auto & argument = arguments.front();
    if (WhichDataType which_data_type(*argument.type); which_data_type.isInterval())
    {
        const auto & in_column = *argument.column;
        auto out_column = ColumnString::create();
        auto & chars = out_column->getChars();
        auto & offsets = out_column->getOffsets();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto value = in_column.getInt(i);
            const auto ticks = value / 100;
            const auto timespan_as_string = ParserKQLTimespan::compose(ticks);

            const auto chars_old_length = chars.size();
            const auto str_length_with_terminator = timespan_as_string.length() + 1;
            chars.resize(chars.size() + str_length_with_terminator);
            std::copy(timespan_as_string.c_str(), timespan_as_string.c_str() + str_length_with_terminator, chars.data() + chars_old_length);
            offsets.push_back(chars.size());
        }

        return out_column;
    }
    else if (which_data_type.isDateOrDate32() || which_data_type.isDateTime() || which_data_type.isDateTime64())
    {
        const ColumnsWithTypeAndName to_datetime64_args{argument, createConstColumnWithTypeAndName<DataTypeUInt8>(7, "scale")};
        const auto as_datetime64 = executeFunctionCall(context, "toDateTime64", to_datetime64_args, input_rows_count);

        const ColumnsWithTypeAndName format_datetime_args{
            asArgument(as_datetime64, "as_datetime64"), createConstColumnWithTypeAndName<DataTypeString>("%FT%T.%fZ", "format_string")};
        return executeFunctionCall(context, "formatDateTime", format_datetime_args, input_rows_count).first;
    }

    return executeFunctionCall(context, "toString", arguments, input_rows_count).first;
}

REGISTER_FUNCTION(KqlToString)
{
    factory.registerFunction<FunctionKqlToString>();
}
}
