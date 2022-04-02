#include <string>
#include <Interpreters/ArrayFilter.h>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Storages/MergeTree/MergeTreeIndexFullText.h>

using namespace std;

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
bool ArrayFilter::getNextInString(const char* data, size_t length, size_t* __restrict pos, size_t* __restrict token_start, size_t* __restrict token_length)
{
    *token_start = *pos;
    *token_length = 0;

    while (*pos < length)
    {
        if (data[*pos] == ' ' || data[*pos] == '%')
        {
            /// Finish current token if any
            if (*token_length > 0)
                return true;
            *token_start = ++ * pos;
        }
        else
        {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++* pos;
            ++* token_length;
        }
    }

    return *token_length > 0;
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

void ArrayFilter::setIndex(BitSet& bits, int index)
{
    if (index < 64)
        bits.items[0] |= (1ULL << index);
    else
        bits.items[1] |= (1ULL << (index-64));
}


bool ArrayFilter::find(const char * data, size_t len)
{
    assert(len <= 8);
    auto bigram = convert(data, len);    
    return filter.find(bigram) != filter.cend();
}

void ArrayFilter::add(const char * data, size_t len, int index)
{
    assert(index < MAX_TERM_LENGTH);
    auto grams = convert(data, len);
    auto it(filter.find(grams));

    if (it != filter.end())
    {
        setIndex(it->second, index);
    }
    else
    {
        BitSet bits{0};
        setIndex(bits, index);
        filter[grams] = bits;
    }
}

void ArrayFilter::clear()
{
    filter.clear();
}

bool ArrayFilter::match(const String& str) const
{
    DB::NgramTokenExtractor ngram_extractor1(1);

    size_t cur = 0;
    size_t bigram_start = 0;
    size_t bigram_len = 0;
    int index = 0;

    size_t bigram_start1 = 0;
    size_t bigram_len1 = 0;

    vector<uint64_t> bigram_array;

    // split string into an array of bigrams
    while (cur < str.size() && ngram_extractor1.nextInString(str.data(), str.size(), &cur, &bigram_start, &bigram_len))
    {
        if (index % 2 == 0) //not found whole bigram yet
        {
            bigram_start1 = bigram_start;
            bigram_len1 = bigram_len;
        }
        else
        {
            bigram_array.push_back(convert(str.data() + bigram_start1, bigram_len1 + bigram_len));
            bigram_start1 = 0;
            bigram_len1 = 0;
        }
        index++;
    }

    // handle the case that the string has odd number of utf8 chars
    if (bigram_len1)
    {
        bigram_array.push_back(convert(str.data() + bigram_start1, bigram_len1));
    }

    for (size_t i = 0; i < bigram_array.size(); i++)
    {
        auto it{ filter.find(bigram_array[i]) };
        if (it == filter.end())
            return false;

        auto bitmap = it->second;
            
        if (i + 1 < bigram_array.size())
        {
            auto itNext{ filter.find(bigram_array[i + 1]) };
            if (itNext == filter.end())
                return false;

            //for each bigram in the bitmap, we need to test if the bigram is the next in binary_array
            bitmap <<= 2;
            bitmap &= itNext->second;

            if(bitmap == 0)
                return false;
        }
    }
    return true;
}
bool ArrayFilter::contains(const ArrayFilter & af)
{
    return contains(af.getMatchString());
}

bool ArrayFilter::contains(const string &str) const
{
    //tokenize input string
    // for each token, check if it matches
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    while (cur < str.size() && getNextInString(str.data(), str.size(), &cur, &token_start, &token_len))
    {
        if (!match(string(str.data() + token_start, token_len)))
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
    return a.getFilter() == b.getFilter();
}
}
