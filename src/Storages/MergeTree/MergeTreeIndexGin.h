#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/SetVariants.h>

#include <memory>
#include <set>


namespace DB
{

class MergeTreeIndexGin;

struct MergeTreeIndexGranuleGin final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleGin(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t ngrams_);

    MergeTreeIndexGranuleGin(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t ngrams_,
        MutableColumns && columns_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    size_t size() const { return block.rows(); }
    bool empty() const override { return !size(); }

    ~MergeTreeIndexGranuleGin() override = default;

    String index_name;
    size_t ngrams;
    Block index_sample_block;
    Block block;
};


struct MergeTreeIndexAggregatorGin final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorGin(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t ngrams_);

    ~MergeTreeIndexAggregatorGin() override = default;

    size_t size() const { return 0; }
    bool empty() const override { return !size(); }

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    /// return true if has new data
    template <typename Method>
    bool buildFilter(
            Method & method,
            const ColumnRawPtrs & column_ptrs,
            IColumn::Filter & filter,
            size_t pos,
            size_t limit) const;

    String index_name;
    size_t ngrams;
    Block index_sample_block;

    MutableColumns columns;
};


class MergeTreeIndexConditionGin final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionGin(
        const String & index_name_,
        const Block & index_sample_block_,
        const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionGin() override = default;
private:
    void traverseAST(ASTPtr & node) const;
    bool atomFromAST(ASTPtr & node) const;
    static bool operatorFromAST(ASTPtr & node);

    bool checkASTUseless(const ASTPtr & node, bool atomic = false) const;


    String index_name;
    Block index_sample_block;

    bool useless;
    std::set<String> key_columns;
    ASTPtr expression_ast;
    ExpressionActionsPtr actions;
};


class MergeTreeIndexGin final : public IMergeTreeIndex
{
public:
    MergeTreeIndexGin(
        const IndexDescription & index_,
        size_t ngrams_)
        : IMergeTreeIndex(index_)
        , ngrams(ngrams_)
    {}

    ~MergeTreeIndexGin() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
            const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    size_t ngrams = 0;
};

}
