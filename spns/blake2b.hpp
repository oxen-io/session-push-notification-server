#pragma once

#include <sodium/crypto_generichash_blake2b.h>

#include <cassert>
#include <charconv>
#include <string>
#include <string_view>

#include "bytes.hpp"
#include "utils.hpp"

namespace spns {

namespace detail {
    template <typename T, std::enable_if_t<std::is_integral_v<T> && (sizeof(T) <= 8), int> = 0>
    void blake2b_update(crypto_generichash_blake2b_state& s, const T& val) {
        char buf[20];
        auto [end, ec] = std::to_chars(std::begin(buf), std::end(buf), val);
        assert(ec == std::errc());
        crypto_generichash_blake2b_update(
                &s, reinterpret_cast<const unsigned char*>(buf), end - buf);
    }

    template <typename Char, std::enable_if_t<sizeof(Char) == 1, int> = 0>
    void blake2b_update(
            crypto_generichash_blake2b_state& s, const std::basic_string_view<Char>& val) {
        crypto_generichash_blake2b_update(
                &s, reinterpret_cast<const unsigned char*>(val.data()), val.size());
    }

    template <typename Char, std::enable_if_t<sizeof(Char) == 1, int> = 0>
    void blake2b_update(crypto_generichash_blake2b_state& s, const std::basic_string<Char>& val) {
        crypto_generichash_blake2b_update(
                &s, reinterpret_cast<const unsigned char*>(val.data()), val.size());
    }

    template <typename T, std::enable_if_t<is_bytes<T>, int> = 0>
    void blake2b_update(crypto_generichash_blake2b_state& s, const T& val) {
        crypto_generichash_blake2b_update(
                &s, reinterpret_cast<const unsigned char*>(val.data()), val.SIZE);
    }
}  // namespace detail

template <typename Hash, typename... T, std::enable_if_t<is_bytes<Hash>, int> = 0>
void blake2b_keyed(Hash& result, ustring_view key, const T&... args) {
    crypto_generichash_blake2b_state s;
    crypto_generichash_blake2b_init(&s, key.data(), key.size(), result.SIZE);
    (detail::blake2b_update(s, args), ...);
    crypto_generichash_blake2b_final(&s, result, result.SIZE);
}

template <typename Hash = Blake2B_32, typename... T, std::enable_if_t<is_bytes<Hash>, int> = 0>
Hash blake2b_keyed(ustring_view key, const T&... args) {
    Hash result;
    blake2b_keyed(result, key, args...);
    return result;
}

template <typename Hash = Blake2B_32, typename... T>
Hash blake2b_keyed(std::string_view key, const T&... args) {
    return blake2b_keyed<Hash>(as_usv(key), args...);
}

template <typename Hash = Blake2B_32, typename... T>
Hash blake2b_keyed(bstring_view key, const T&... args) {
    return blake2b_keyed<Hash>(as_usv(key), args...);
}

template <typename Hash = Blake2B_32, typename... T>
Hash blake2b(const T&... args) {
    return blake2b_keyed<Hash>(""sv, args...);
}

}  // namespace spns
