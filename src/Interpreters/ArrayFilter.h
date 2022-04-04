#pragma once

#include <vector>
#include <map>
#include <base/types.h>
#include <base/extended_types.h>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <Common/Allocator.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>


namespace DB
{
using BitSet = UInt128;
constexpr int MAX_TERM_LENGTH = sizeof(BitSet) * 8;    
struct ArrayFilterParameters
{
    ArrayFilterParameters(size_t ngrams_);

    size_t ngrams;
};

class ArrayFilter
{
public:
    using UnderType = UInt64;
    using Container = std::map<UnderType, BitSet>;

    explicit ArrayFilter(const ArrayFilterParameters & params);

    bool find(const char * data, size_t len);
    void add(const char * data, size_t len, int index);
    void clear();
    size_t size() const {return filter.size();}

    bool contains(const ArrayFilter & af, bool searchFromStart = false);
    bool contains(const std::string& str, bool searchFromStart) const;

    const Container & getFilter() const { return filter; }
    Container & getFilter() { return filter; }

    /// For debug.
    UInt64 isEmpty() const;

    friend bool operator== (const ArrayFilter & a, const ArrayFilter & b);
    void setMatchString(const char * data, size_t len)
    {
        matchString = String(data, len);
    }
    String getMatchString() const { return matchString;}
private:

    [[maybe_unused]]size_t ngrams;
    Container filter;
    String matchString;
public:
    static UnderType convert(const char* data, size_t len);
    static void setIndex(BitSet& bits, int index);
    static const String &getName();
    static bool getNextInString(const char* data, size_t length, size_t* __restrict pos, size_t* __restrict token_start, size_t* __restrict token_length);

    bool match(const String& str, bool searchFromStart) const;
};

using ArrayFilterPtr = std::shared_ptr<ArrayFilter>;

bool operator== (const ArrayFilter & a, const ArrayFilter & b);    
}
