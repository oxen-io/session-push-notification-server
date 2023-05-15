#pragma once

#include <fmt/format.h>

#include <charconv>
#include <chrono>
#include <string>
#include <vector>

namespace spns {

using steady_clock = std::chrono::steady_clock;
using system_clock = std::chrono::system_clock;
using steady_time = steady_clock::time_point;
using system_time = system_clock::time_point;

using namespace std::literals;

using bstring = std::basic_string<std::byte>;
using bstring_view = std::basic_string_view<std::byte>;

using ustring = std::basic_string<unsigned char>;
using ustring_view = std::basic_string_view<unsigned char>;

inline std::string_view as_sv(bstring_view s) {
    return {reinterpret_cast<const char*>(s.data()), s.size()};
}
inline std::string copy_str(bstring_view s) {
    return std::string{as_sv(s)};
}

inline bstring_view as_bsv(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

inline ustring_view as_usv(std::string_view s) {
    return {reinterpret_cast<const unsigned char*>(s.data()), s.size()};
}
inline ustring_view as_usv(bstring_view s) {
    return {reinterpret_cast<const unsigned char*>(s.data()), s.size()};
}

// Can replace this with a `using namespace oxen::log::literals` if we start using oxen-logging
namespace detail {

    // Internal implementation of _format that holds the format temporarily until the (...) operator
    // is invoked on it.  This object cannot be moved, copied but only used ephemerally in-place.
    struct fmt_wrapper {
      private:
        std::string_view format;

        // Non-copyable and non-movable:
        fmt_wrapper(const fmt_wrapper&) = delete;
        fmt_wrapper& operator=(const fmt_wrapper&) = delete;
        fmt_wrapper(fmt_wrapper&&) = delete;
        fmt_wrapper& operator=(fmt_wrapper&&) = delete;

      public:
        constexpr explicit fmt_wrapper(const char* str, const std::size_t len) : format{str, len} {}

        /// Calling on this object forwards all the values to fmt::format, using the format string
        /// as provided during construction (via the "..."_format user-defined function).
        template <typename... T>
        auto operator()(T&&... args) && {
            return fmt::format(format, std::forward<T>(args)...);
        }
    };

}  // namespace detail

inline detail::fmt_wrapper operator""_format(const char* str, size_t len) {
    return detail::fmt_wrapper{str, len};
}

/// Splits a string on some delimiter string and returns a vector of string_view's pointing into the
/// pieces of the original string.  The pieces are valid only as long as the original string remains
/// valid.  Leading and trailing empty substrings are not removed.  If delim is empty you get back a
/// vector of string_views each viewing one character.  If `trim` is true then leading and trailing
/// empty values will be suppressed.
///
///     auto v = split("ab--c----de", "--"); // v is {"ab", "c", "", "de"}
///     auto v = split("abc", ""); // v is {"a", "b", "c"}
///     auto v = split("abc", "c"); // v is {"ab", ""}
///     auto v = split("abc", "c", true); // v is {"ab"}
///     auto v = split("-a--b--", "-"); // v is {"", "a", "", "b", "", ""}
///     auto v = split("-a--b--", "-", true); // v is {"a", "", "b"}
///
std::vector<std::string_view> split(
        std::string_view str, std::string_view delim, bool trim = false);

/// Splits a string on any 1 or more of the given delimiter characters and returns a vector of
/// string_view's pointing into the pieces of the original string.  If delims is empty this works
/// the same as split().  `trim` works like split (suppresses leading and trailing empty string
/// pieces).
///
///     auto v = split_any("abcdedf", "dcx"); // v is {"ab", "e", "f"}
std::vector<std::string_view> split_any(
        std::string_view str, std::string_view delims, bool trim = false);

// Returns unix timestamp seconds for the given system clock time
inline int64_t unix_timestamp(system_time t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

inline bool starts_with(std::string_view string, std::string_view prefix) {
    return string.substr(0, prefix.size()) == prefix;
}

inline bool ends_with(std::string_view string, std::string_view suffix) {
    return string.size() >= suffix.size() && string.substr(string.size() - suffix.size()) == suffix;
}

// Returns unix timestamp seconds for the current time.
inline int64_t unix_timestamp() {
    return unix_timestamp(system_clock::now());
}

/// Parses an integer of some sort from a string, requiring that the entire string be consumed
/// during parsing.  Return false if parsing failed, sets `value` and returns true if the entire
/// string was consumed.
template <typename T>
bool parse_int(const std::string_view str, T& value, int base = 10) {
    T tmp;
    auto* strend = str.data() + str.size();
    auto [p, ec] = std::from_chars(str.data(), strend, tmp, base);
    if (ec != std::errc() || p != strend)
        return false;
    value = tmp;
    return true;
}

void fiddle_rlimit_nofile();

constexpr int digits(size_t val) {
    int i = 0;
    do {
        ++i;
        val /= 10;
    } while (val);
    return i;
}

}  // namespace spns
