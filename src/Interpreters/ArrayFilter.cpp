#include <Interpreters/ArrayFilter.h>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
ArrayFilterParameters::ArrayFilterParameters(size_t ngrams_)
    : ngrams(ngrams_)
{
    if (ngrams == 0 || ngrams > 8)
        throw Exception("The size of array filter cannot be zero or greater than 8", ErrorCodes::BAD_ARGUMENTS);
}


ArrayFilter::ArrayFilter(const ArrayFilterParameters & params)
    : ngrams(params.ngrams)
{
}

ArrayFilter::UnderType ArrayFilter::convert(const char* data, size_t len)
{
    ArrayFilter::UnderType result{0};
    size_t n = std::min(sizeof(ArrayFilter::UnderType), len);
    for (size_t i = 0; i < n; ++i)
    {
        result <<= 8;
        result += data[i];
    }
    return result;
}
bool ArrayFilter::find(const char * data, size_t len)
{
    assert(len <= 8);
    auto grams = convert(data, len);    
    return std::binary_search(filter.cbegin(), filter.cend(), grams);
}

void ArrayFilter::add(const char * data, size_t len)
{
    auto grams = convert(data, len);
    // we should add in batch, then sort and remove duplicates(call ArrayFilter::sort())
    filter.push_back(grams);
}

void ArrayFilter::sort()
{
     std::sort(filter.begin(), filter.end());
     filter.erase( std::unique( filter.begin(), filter.end() ), filter.end() );
}

void ArrayFilter::clear()
{
    filter.clear();
}

bool ArrayFilter::contains(const ArrayFilter & af)
{
    for (size_t i = 0; i < af.size(); ++i)
    {
         if(!std::binary_search(filter.cbegin(), filter.cend(), af.filter[i]))
            return false;
    }
    return true;
}
const String &ArrayFilter::getName()
 { 
     static String name("ngramaf_v1");
     return name;
}

UInt64 ArrayFilter::isEmpty() const
{
    return filter.size() == 0;
}

bool operator== (const ArrayFilter & a, const ArrayFilter & b)
{
    if(a.size() != b.size())
        return false;
    
    for (size_t i = 0; i < a.size(); ++i)
        if (a.filter[i] != b.filter[i])
            return false;
    return true;
}
}
