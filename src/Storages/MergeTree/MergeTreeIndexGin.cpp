#include <Storages/MergeTree/MergeTreeIndexGin.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

/// 0b11 -- can be true and false at the same time
static const Field UNKNOWN_FIELD(3u);


MergeTreeIndexGranuleGin::MergeTreeIndexGranuleGin(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t ngrams_)
    : index_name(index_name_)
    , ngrams(ngrams_)
    , index_sample_block(index_sample_block_)
    , block(index_sample_block)
{
}

MergeTreeIndexGranuleGin::MergeTreeIndexGranuleGin(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t ngrams_,
    MutableColumns && mutable_columns_)
    : index_name(index_name_)
    , ngrams(ngrams_)
    , index_sample_block(index_sample_block_)
    , block(index_sample_block.cloneWithColumns(std::move(mutable_columns_)))
{
}

void MergeTreeIndexGranuleGin::serializeBinary(WriteBuffer & ostr __attribute__((unused))) const
{
}

void MergeTreeIndexGranuleGin::deserializeBinary(ReadBuffer & istr __attribute__((unused)), MergeTreeIndexVersion version __attribute__((unused)))
{
}


MergeTreeIndexAggregatorGin::MergeTreeIndexAggregatorGin(const String & index_name_, const Block & index_sample_block_, size_t ngrams_)
    : index_name(index_name_)
    , ngrams(ngrams_)
    , index_sample_block(index_sample_block_)
    , columns(index_sample_block_.cloneEmptyColumns())
{
}

void MergeTreeIndexAggregatorGin::update(const Block & block __attribute__((unused)), size_t * pos __attribute__((unused)), size_t limit __attribute__((unused)))
{
}

template <typename Method>
bool MergeTreeIndexAggregatorGin::buildFilter(
    Method & method __attribute__((unused)),
    const ColumnRawPtrs & column_ptrs __attribute__((unused)),
    IColumn::Filter & filter __attribute__((unused)),
    size_t pos __attribute__((unused)),
    size_t limit __attribute__((unused))) const
{
   return false;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorGin::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleGin>(index_name, index_sample_block, ngrams, std::move(columns));
    return granule;
}


MergeTreeIndexConditionGin::MergeTreeIndexConditionGin(
    const String & index_name_ ,
    const Block & index_sample_block_,
    const SelectQueryInfo & query __attribute__((unused)),
    ContextPtr context __attribute__((unused)))
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
}

bool MergeTreeIndexConditionGin::alwaysUnknownOrTrue() const
{
    return useless;
}

bool MergeTreeIndexConditionGin::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule __attribute__((unused))) const
{
    return false;
}

void MergeTreeIndexConditionGin::traverseAST(ASTPtr & node __attribute__((unused))) const
{
}

bool MergeTreeIndexConditionGin::atomFromAST(ASTPtr & node __attribute__((unused))) const
{
    return false;
}

bool MergeTreeIndexConditionGin::operatorFromAST(ASTPtr & node __attribute__((unused)))
{
    return true;
}

bool MergeTreeIndexConditionGin::checkASTUseless(const ASTPtr & node __attribute__((unused)), bool atomic __attribute__((unused))) const
{
    return true;
}


MergeTreeIndexGranulePtr MergeTreeIndexGin::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleGin>(index.name, index.sample_block, ngrams);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexGin::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorGin>(index.name, index.sample_block, ngrams);
}

MergeTreeIndexConditionPtr MergeTreeIndexGin::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionGin>(index.name, index.sample_block, query, context);
}

bool MergeTreeIndexGin::mayBenefitFromIndexForIn(const ASTPtr &node __attribute__((unused))) const
{
    return false;
}

MergeTreeIndexPtr ginIndexCreator(const IndexDescription & index)
{
    size_t ngrams = index.arguments[0].get<size_t>();
    return std::make_shared<MergeTreeIndexGin>(index, ngrams);
}

void ginIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    if (index.arguments.size() != 1)
        throw Exception("Gin index must have exactly one argument.", ErrorCodes::INCORRECT_QUERY);
    else if (index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception("Gin index argument must be positive integer.", ErrorCodes::INCORRECT_QUERY);
}

}
