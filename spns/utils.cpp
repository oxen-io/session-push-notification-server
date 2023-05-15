#include "utils.hpp"

#include <oxen/log.hpp>

extern "C" {
#include "sys/resource.h"
}

namespace spns {

namespace log = oxen::log;

auto cat = log::Cat("utils");

std::vector<std::string_view> split(std::string_view str, std::string_view delim, bool trim) {
    std::vector<std::string_view> results;
    // Special case for empty delimiter: splits on each character boundary:
    if (delim.empty()) {
        results.reserve(str.size());
        for (size_t i = 0; i < str.size(); i++)
            results.emplace_back(str.data() + i, 1);
        return results;
    }

    for (size_t pos = str.find(delim); pos != std::string_view::npos; pos = str.find(delim)) {
        if (!trim || !results.empty() || pos > 0)
            results.push_back(str.substr(0, pos));
        str.remove_prefix(pos + delim.size());
    }
    if (!trim || str.size())
        results.push_back(str);
    else
        while (!results.empty() && results.back().empty())
            results.pop_back();
    return results;
}

std::vector<std::string_view> split_any(std::string_view str, std::string_view delims, bool trim) {
    if (delims.empty())
        return split(str, delims, trim);
    std::vector<std::string_view> results;
    for (size_t pos = str.find_first_of(delims); pos != std::string_view::npos;
         pos = str.find_first_of(delims)) {
        if (!trim || !results.empty() || pos > 0)
            results.push_back(str.substr(0, pos));
        size_t until = str.find_first_not_of(delims, pos + 1);
        if (until == std::string_view::npos)
            str.remove_prefix(str.size());
        else
            str.remove_prefix(until);
    }
    if (!trim || str.size())
        results.push_back(str);
    else
        while (!results.empty() && results.back().empty())
            results.pop_back();
    return results;
}

void fiddle_rlimit_nofile() {
    struct rlimit nofile {};
    auto rc = getrlimit(RLIMIT_NOFILE, &nofile);
    if (rc != 0) {
        // log about failure
    } else if (nofile.rlim_cur < 10000 && nofile.rlim_cur < nofile.rlim_max) {
        auto new_lim = std::min<rlim_t>(10000, nofile.rlim_max);
        log::warning(cat, "NOFILE limit is only {}; increasing to {}", nofile.rlim_cur, new_lim);
        nofile.rlim_cur = new_lim;
        rc = setrlimit(RLIMIT_NOFILE, &nofile);
        if (rc != 0)
            log::error(
                    cat,
                    "Failed to increase fd limit: {}; connections may fail!",
                    std::strerror(rc));
    }
}

static_assert(digits(0) == 1);
static_assert(digits(9) == 1);
static_assert(digits(10) == 2);
static_assert(digits(99) == 2);
static_assert(digits(100) == 3);

}  // namespace spns
