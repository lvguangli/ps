//
// Created by Sahara on 28/06/2017.
//

#ifndef PS_STRINGBUILDER_H
#define PS_STRINGBUILDER_H
#include <list>
#include <string>
#include <numeric>

template <typename chr>
class StringBuilder {
    typedef std::basic_string<chr> string_t;
    typedef std::list<string_t> container_t; // Reasons not to use vector below.
    typedef typename string_t::size_type size_type; // Reuse the size type in the string.
    container_t m_Data;
    size_type m_totalSize;
    void append(const string_t &src) {
        m_Data.push_back(src);
        m_totalSize += src.size();
    }
public:
    StringBuilder() {
        m_totalSize = 0;
    }
    StringBuilder & Append(const string_t &src) {
        append(src);
        return *this; // allow chaining.
    }
    // Note the use of reserve() to avoid reallocations.
    string_t toString() const {
        string_t result;
        // The whole point of the exercise!
        // If the container has a lot of strings, reallocation (each time the result grows) will take a serious toll,
        // both in performance and chances of failure.
        // I measured (in code I cannot publish) fractions of a second using 'reserve', and almost two minutes using +=.
        result.reserve(m_totalSize + 1);
        // result = std::accumulate(m_Data.begin(), m_Data.end(), result); // This would lose the advantage of 'reserve'
        for (auto iter = m_Data.begin(); iter != m_Data.end(); ++iter) {
            result += *iter;
        }
        return result;
    }

};
#endif //PS_STRINGBUILDER_H
