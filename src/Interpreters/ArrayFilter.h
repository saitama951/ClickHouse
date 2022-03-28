#pragma once

#include <vector>
#include <base/types.h>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <Common/Allocator.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>


namespace DB
{
struct ArrayFilterParameters
{
    ArrayFilterParameters(size_t ngrams_);

    size_t ngrams;
};

class ArrayFilter
{
public:
    using UnderType = UInt64;
    using Container = std::vector<UnderType>;

    explicit ArrayFilter(const ArrayFilterParameters & params);

    bool find(const char * data, size_t len);
    void add(const char * data, size_t len);
    void clear();
    size_t size() const {return filter.size();}

    bool contains(const ArrayFilter & af);
    void sort();

    const Container & getFilter() const { return filter; }
    Container & getFilter() { return filter; }

    /// For debug.
    UInt64 isEmpty() const;

    friend bool operator== (const ArrayFilter & a, const ArrayFilter & b);
private:

    [[maybe_unused]]size_t ngrams;
    Container filter;

public:
    static UnderType convert(const char* data, size_t len);
    static const String &getName();
};

using ArrayFilterPtr = std::shared_ptr<ArrayFilter>;

bool operator== (const ArrayFilter & a, const ArrayFilter & b);    
}
