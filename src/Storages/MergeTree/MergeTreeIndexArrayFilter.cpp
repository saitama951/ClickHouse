
#include <algorithm>

#include <Storages/MergeTree/MergeTreeIndexArrayFilter.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ArrayFilter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Core/Defines.h>

#include <Poco/Logger.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}

MergeTreeIndexGranuleArrayFilter::MergeTreeIndexGranuleArrayFilter(
    const String & index_name_,
    size_t columns_number,
    const ArrayFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , array_filters(
        columns_number, ArrayFilter(params))
    , has_elems(false)
{
}

void MergeTreeIndexGranuleArrayFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty fulltext index {}.", backQuote(index_name));
    
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();

    for (const auto & array_filter : array_filters)
    {
        size_t filterSize = array_filter.getFilter().size();
        size_serialization->serializeBinary(filterSize, ostr);

        std::vector<uint64_t> tmpArray(filterSize * 3, 0);

        size_t index = 0;
        for (const auto& element : array_filter.getFilter()) {

            tmpArray[index++] = element.first;
            tmpArray[index++] = element.second.items[0];
            tmpArray[index++] = element.second.items[1];
        }
        ostr.write(reinterpret_cast<const char*>(&tmpArray[0]), filterSize * 3 * sizeof(uint64_t));
    }
}

void MergeTreeIndexGranuleArrayFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_rows;
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());

    for (auto & array_filter : array_filters)
    {
        size_type->getDefaultSerialization()->deserializeBinary(field_rows, istr);
        size_t filterSize = field_rows.get<size_t>();

        if (filterSize == 0)
            continue;

        std::vector<uint64_t> tmpArray(filterSize * 3, 0);
        istr.read(reinterpret_cast<char*>(&tmpArray[0]), filterSize * 3 * sizeof(uint64_t));

        array_filter.getFilter().clear();
        for (size_t i = 0; i < filterSize; ++i)
        {
            auto bigram = tmpArray[3 * i];
            BitSet bits;
            bits.items[0] = tmpArray[3 * i + 1];
            bits.items[1] = tmpArray[3 * i + 2];
            array_filter.getFilter().insert({bigram, bits});
        }
    }
    has_elems = true;
}


MergeTreeIndexAggregatorArrayFilter::MergeTreeIndexAggregatorArrayFilter(
    const Names & index_columns_,
    const String & index_name_,
    const ArrayFilterParameters & params_,
    TokenExtractorPtr token_extractor_)
    : index_columns(index_columns_)
    , index_name (index_name_)
    , params(params_)
    , token_extractor(token_extractor_)
    , granule(
        std::make_shared<MergeTreeIndexGranuleArrayFilter>(
            index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorArrayFilter::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleArrayFilter>(
        index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

static void addToArrayFilter(DB::NgramTokenExtractor &extractor, const char* data, size_t length, ArrayFilter& array_filter)
{
	size_t cur = 0;
	size_t token_start = 0;
	size_t token_len = 0;

	int index = 0;
	while (cur < length && extractor.nextInStringPadded(data, length, &cur, &token_start, &token_len))
	{
		array_filter.add(data + token_start, token_len, index);
		index++;
	}
}

void MergeTreeIndexAggregatorArrayFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column_with_type = block.getByName(index_columns[col]);
        const auto & column = column_with_type.column;
        size_t current_position = *pos;

        if (isArray(column_with_type.type))
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            const auto & column_offsets = column_array.getOffsets();
            const auto & column_key = column_array.getData();

            NgramTokenExtractor ngram_extractor1(1);
            NgramTokenExtractor ngram_extractor2(2);
            
            for (size_t i = 0; i < rows_read; ++i)
            {
                size_t element_start_row = column_offsets[current_position - 1];
                size_t elements_size = column_offsets[current_position] - element_start_row;

                for (size_t row_num = 0; row_num < elements_size; ++row_num)
                {
                    auto ref = column_key.getDataAt(element_start_row + row_num);
                    
                    size_t cur = 0;
                    size_t token_start = 0;
                    size_t token_len = 0;
                    while (cur < ref.size && ArrayFilter::getNextInString(ref.data, ref.size, &cur, &token_start, &token_len))
                    {

                        addToArrayFilter(ngram_extractor1, ref.data + token_start, token_len, granule->array_filters[col]);
                        addToArrayFilter(ngram_extractor2, ref.data + token_start, token_len, granule->array_filters[col]);
                    }
                    
                }

                current_position += 1;
            }
        }
        else
        {
            NgramTokenExtractor ngram_extractor1(1);
            NgramTokenExtractor ngram_extractor2(2);

            for (size_t i = 0; i < rows_read; ++i)
            {
                auto ref = column->getDataAt(current_position + i);
                token_extractor->stringPaddedToArrayFilter(ref.data, ref.size, granule->array_filters[col]);

                size_t cur = 0;
                size_t token_start = 0;
                size_t token_len = 0;
                while (cur < ref.size && ArrayFilter::getNextInString(ref.data, ref.size, &cur, &token_start, &token_len))
                {

                    addToArrayFilter(ngram_extractor1, ref.data + token_start, token_len, granule->array_filters[col]);
                    addToArrayFilter(ngram_extractor2, ref.data + token_start, token_len, granule->array_filters[col]);
                }                
            }
        }
    }

    granule->has_elems = true;
    *pos += rows_read;
}

MergeTreeConditionArrayFilter::MergeTreeConditionArrayFilter(
    const SelectQueryInfo & query_info,
    ContextPtr context, 
    const Block & index_sample_block,
    const ArrayFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    : index_columns(index_sample_block.getNames())
    , index_data_types(index_sample_block.getNamesAndTypesList().getTypes())
    , params(params_)
    , token_extractor(token_extactor_)
    , prepared_sets(query_info.sets)
{
    rpn = std::move(
            RPNBuilder<RPNElement>(
                    query_info, context,
                    [this] (const ASTPtr & node, ContextPtr /* context */, Block & block_with_constants, RPNElement & out) -> bool
                    {
                        return this->traverseAtomAST(node, block_with_constants, out);
                    }).extractRPN());
}

bool MergeTreeConditionArrayFilter::alwaysUnknownOrTrue() const
{
    /// Check like in KeyCondition.
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS
             || element.function == RPNElement::FUNCTION_HAS
             || element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN
             || element.function == RPNElement::FUNCTION_MULTI_SEARCH
             || element.function == RPNElement::ALWAYS_FALSE
             || element.function == RPNElement::FUNCTION_LIKE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            // do nothing
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 && arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 || arg2;
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    return rpn_stack[0];
}

bool MergeTreeConditionArrayFilter::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleArrayFilter> granule
            = std::dynamic_pointer_cast<MergeTreeIndexGranuleArrayFilter>(idx_granule);
    if (!granule)
        throw Exception(
                "ArrayFilter index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    /// Check like in KeyCondition.
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS
             || element.function == RPNElement::FUNCTION_HAS
             || element.function == RPNElement::FUNCTION_LIKE)
        {
            rpn_stack.emplace_back(granule->array_filters[element.key_column].contains(*element.array_filter, element.function != RPNElement::FUNCTION_LIKE), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.set_array_filters.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const size_t key_idx = element.set_key_position[column];

                const auto & array_filters = element.set_array_filters[column];
                for (size_t row = 0; row < array_filters.size(); ++row)
                    result[row] = result[row] && granule->array_filters[key_idx].contains(array_filters[row], true);
            }

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
        {
            std::vector<bool> result(element.set_array_filters.back().size(), true);

            const auto & array_filters = element.set_array_filters[0];

            for (size_t row = 0; row < array_filters.size(); ++row)
                result[row] = result[row] && granule->array_filters[element.key_column].contains(array_filters[row], true);

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception("Unexpected function type in ArrayFilterCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in ArrayFilterCondition::mayBeTrueOnGranule", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}

bool MergeTreeConditionArrayFilter::getKey(const std::string & key_column_name, size_t & key_column_num)
{
    auto it = std::find(index_columns.begin(), index_columns.end(), key_column_name);
    if (it == index_columns.end())
        return false;

    key_column_num = static_cast<size_t>(it - index_columns.begin());
    return true;
}

bool MergeTreeConditionArrayFilter::traverseAtomAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
        {
            /// Check constant like in KeyCondition
            if (const_value.getType() == Field::Types::UInt64
                || const_value.getType() == Field::Types::Int64
                || const_value.getType() == Field::Types::Float64)
            {
                /// Zero in all types is represented in memory the same way as in UInt64.
                out.function = const_value.get<UInt64>()
                            ? RPNElement::ALWAYS_TRUE
                            : RPNElement::ALWAYS_FALSE;

                return true;
            }
        }
    }

    if (const auto * function = node->as<ASTFunction>())
    {
        if (!function->arguments)
            return false;

        const ASTs & arguments = function->arguments->children;

        if (arguments.size() != 2)
            return false;

        if (functionIsInOrGlobalInOperator(function->name))
        {
            if (tryPrepareSetArrayFilter(arguments, out))
            {
                if (function->name == "notIn")
                {
                    out.function = RPNElement::FUNCTION_NOT_IN;
                    return true;
                }
                else if (function->name == "in")
                {
                    out.function = RPNElement::FUNCTION_IN;
                    return true;
                }
            }
        }
        else if (function->name == "equals" ||
                 function->name == "notEquals" ||
                 function->name == "has" ||
                 function->name == "mapContains" ||
                 function->name == "like" ||
                 function->name == "notLike" ||
                 function->name == "hasToken" ||
                 function->name == "startsWith" ||
                 function->name == "endsWith" ||
                 function->name == "multiSearchAny")
        {
            Field const_value;
            DataTypePtr const_type;
            if (KeyCondition::getConstant(arguments[1], block_with_constants, const_value, const_type))
            {
                if (traverseASTEquals(function->name, arguments[0], const_type, const_value, out))
                    return true;
            }
            else if (KeyCondition::getConstant(arguments[0], block_with_constants, const_value, const_type) && (function->name == "equals" || function->name == "notEquals"))
            {
                if (traverseASTEquals(function->name, arguments[1], const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeConditionArrayFilter::traverseASTEquals(
    const String & function_name,
    const ASTPtr & key_ast,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    Field const_value = value_field;

    size_t key_column_num = 0;
    bool key_exists = getKey(key_ast->getColumnName(), key_column_num);
    bool map_key_exists = getKey(fmt::format("mapKeys({})", key_ast->getColumnName()), key_column_num);

    if (const auto * function = key_ast->as<ASTFunction>())
    {
        if (function->name == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exists in map
              * we return default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exists.
              */
            if (value_field == value_type->getDefault())
                return false;

            const auto * column_ast_identifier = function->arguments.get()->children[0].get()->as<ASTIdentifier>();
            if (!column_ast_identifier)
                return false;

            const auto & map_column_name = column_ast_identifier->name();

            size_t map_keys_key_column_num = 0;
            auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
            bool map_keys_exists = getKey(map_keys_index_column_name, map_keys_key_column_num);

            size_t map_values_key_column_num = 0;
            auto map_values_index_column_name = fmt::format("mapValues({})", map_column_name);
            bool map_values_exists = getKey(map_values_index_column_name, map_values_key_column_num);

            if (map_keys_exists)
            {
                auto & argument = function->arguments.get()->children[1];

                if (const auto * literal = argument->as<ASTLiteral>())
                {
                    auto element_key = literal->value;
                    const_value = element_key;
                    key_column_num = map_keys_key_column_num;
                    key_exists = true;
                }
                else
                {
                    return false;
                }
            }
            else if (map_values_exists)
            {
                key_column_num = map_values_key_column_num;
                key_exists = true;
            }
            else
            {
                return false;
            }
        }
    }

    if (!key_exists && !map_key_exists)
        return false;

    if (map_key_exists && (function_name == "has" || function_name == "mapContains"))
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "has")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }

    if (function_name == "notEquals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "equals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "like")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_LIKE;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringLikeToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "notLike")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringLikeToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "hasToken")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "startsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "endsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.array_filter = std::make_unique<ArrayFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToArrayFilter(value.data(), value.size(), *out.array_filter);
        return true;
    }
    else if (function_name == "multiSearchAny")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_MULTI_SEARCH;

        /// 2d vector is not needed here but is used because already exists for FUNCTION_IN
        std::vector<std::vector<ArrayFilter>> array_filters;
        array_filters.emplace_back();
        for (const auto & element : const_value.get<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            array_filters.back().emplace_back(params);
            const auto & value = element.get<String>();
            token_extractor->stringToArrayFilter(value.data(), value.size(), array_filters.back().back());
        }
        out.set_array_filters = std::move(array_filters);
        return true;
    }

    return false;
}

bool MergeTreeConditionArrayFilter::tryPrepareSetArrayFilter(
    const ASTs & args,
    RPNElement & out)
{
    const ASTPtr & left_arg = args[0];
    const ASTPtr & right_arg = args[1];

    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    const auto * left_arg_tuple = typeid_cast<const ASTFunction *>(left_arg.get());
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        const auto & tuple_elements = left_arg_tuple->arguments->children;
        for (size_t i = 0; i < tuple_elements.size(); ++i)
        {
            size_t key = 0;
            if (getKey(tuple_elements[i]->getColumnName(), key))
            {
                key_tuple_mapping.emplace_back(i, key);
                data_types.push_back(index_data_types[key]);
            }
        }
    }
    else
    {
        size_t key = 0;
        if (getKey(left_arg->getColumnName(), key))
        {
            key_tuple_mapping.emplace_back(0, key);
            data_types.push_back(index_data_types[key]);
        }
    }

    if (key_tuple_mapping.empty())
        return false;

    PreparedSetKey set_key;
    if (typeid_cast<const ASTSubquery *>(right_arg.get()) || typeid_cast<const ASTIdentifier *>(right_arg.get()))
        set_key = PreparedSetKey::forSubquery(*right_arg);
    else
        set_key = PreparedSetKey::forLiteral(*right_arg, data_types);

    auto set_it = prepared_sets.find(set_key);
    if (set_it == prepared_sets.end())
        return false;

    const SetPtr & prepared_set = set_it->second;
    if (!prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & data_type : prepared_set->getDataTypes())
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
            return false;

    std::vector<std::vector<ArrayFilter>> array_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    for (const auto & elem : key_tuple_mapping)
    {
        array_filters.emplace_back();
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            array_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            array_filters.back().back().setMatchString(ref.data, ref.size);
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_array_filters = std::move(array_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexArrayFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleArrayFilter>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexArrayFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorArrayFilter>(index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexArrayFilter::createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionArrayFilter>(query, context, index.sample_block, params, token_extractor.get());
};

bool MergeTreeIndexArrayFilter::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    return std::find(std::cbegin(index.column_names), std::cend(index.column_names), node->getColumnName()) != std::cend(index.column_names);
}

MergeTreeIndexPtr arrayFilterIndexCreator(
    const IndexDescription & index)
{
    if (index.type == ArrayFilter::getName())
    {
        size_t n = index.arguments[0].get<size_t>();
        ArrayFilterParameters params(n);

        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);

        return std::make_shared<MergeTreeIndexArrayFilter>(index, params, std::move(tokenizer));
    }
    else
    {
        throw Exception("Unknown index type: " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);
    }
}

void arrayFilterIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isArray())
        {
            const auto & array_type = assert_cast<const DataTypeArray &>(*index_data_type);
            data_type = WhichDataType(array_type.getNestedType());
        }
        else if (data_type.isLowCarnality())
        {
            const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index_data_type);
            data_type = WhichDataType(low_cardinality.getDictionaryType());
        }

        if (!data_type.isString() && !data_type.isFixedString())
            throw Exception("Array filter index can be used only with `String`, `FixedString`, `LowCardinality(String)`, `LowCardinality(FixedString)` column or Array with `String` or `FixedString` values column.", ErrorCodes::INCORRECT_QUERY);
    }

    if(index.type != ArrayFilter::getName())
        throw Exception("Unknown index type: " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);
    else if (index.arguments.size() != 1)
        throw Exception("Set index must have exactly one argument.", ErrorCodes::INCORRECT_QUERY);
    else if (index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception("Set index argument must be positive integer.", ErrorCodes::INCORRECT_QUERY);

    for (const auto & arg : index.arguments)
        if (arg.getType() != Field::Types::UInt64)
            throw Exception("All parameters to *af_v1 index must be unsigned integers", ErrorCodes::BAD_ARGUMENTS);

    /// Just validate
    ArrayFilterParameters params(
        index.arguments[0].get<size_t>());
}

}
