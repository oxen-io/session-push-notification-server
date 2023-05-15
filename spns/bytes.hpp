#pragma once

#include <fmt/format.h>
#include <oxenc/base64.h>
#include <oxenc/hex.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <string_view>

namespace spns {

template <size_t N>
struct bytes : std::array<std::byte, N> {
    static constexpr size_t SIZE = N;

    using std::array<std::byte, N>::data;
    std::basic_string_view<std::byte> view() const { return {data(), SIZE}; }
    std::string_view sv() const { return {reinterpret_cast<const char*>(data()), SIZE}; }
    std::basic_string_view<unsigned char> usv() const {
        return {reinterpret_cast<const unsigned char*>(data()), SIZE};
    }

    std::string hex() const { return oxenc::to_hex(this->begin(), this->end()); }

    // Implicit conversion to unsigned char* for easier passing into libsodium functions
    template <typename T, typename = std::enable_if_t<std::is_same_v<T, unsigned char*>>>
    constexpr operator T() noexcept {
        return reinterpret_cast<unsigned char*>(this->data());
    }
    template <typename T, typename = std::enable_if_t<std::is_same_v<T, const unsigned char*>>>
    constexpr operator T() const noexcept {
        return reinterpret_cast<const unsigned char*>(this->data());
    }
};

struct is_bytes_impl {
    template <size_t N>
    static std::true_type check(bytes<N>*);
    static std::false_type check(...);
};

template <typename T>
inline constexpr bool is_bytes = decltype(is_bytes_impl::check(static_cast<T*>(nullptr)))::value;

struct AccountID : bytes<33> {};
struct Ed25519PK : bytes<32> {};
struct X25519PK : bytes<32> {};
struct X25519SK : bytes<32> {};
struct SubkeyTag : bytes<32> {};
struct Signature : bytes<64> {};
struct EncKey : bytes<32> {};

struct Blake2B_32 : bytes<32> {};

template <typename T, std::enable_if_t<is_bytes<T>, int> = 0>
inline std::basic_string_view<std::byte> as_bsv(const T& v) {
    return {reinterpret_cast<const std::byte*>(v.data()), T::SIZE};
}

template <typename T, std::enable_if_t<is_bytes<T>, int> = 0>
inline std::basic_string_view<unsigned char> as_usv(const T& v) {
    return {reinterpret_cast<const unsigned char*>(v.data()), v.size()};
}

// std::hash-implementing class that "hashes" by just reading the size_t-size bytes starting at the
// 16th byte.
template <typename T, typename = std::enable_if_t<is_bytes<T> && (T::SIZE >= 32)>>
struct bytes_simple_hasher {
    size_t operator()(const T& x) const {
        size_t hash;
        std::memcpy(&hash, x.data() + 16, sizeof(hash));
        return hash;
    }
};

template <typename T, typename = std::enable_if_t<is_bytes<T>>>
void from_hex_or_b64(T& val, std::string_view input) {
    if (input.size() == T::SIZE) {
        std::memcpy(val.data(), input.data(), T::SIZE);
        return;
    }
    if (input.size() == 2 * T::SIZE && oxenc::is_hex(input)) {
        oxenc::from_hex(input.begin(), input.end(), val.begin());
        return;
    }
    while (!input.empty() && input.back() == '=')
        input.remove_suffix(1);
    if (input.size() == oxenc::to_base64_size(T::SIZE, false) && oxenc::is_base64(input)) {
        oxenc::from_base64(input.begin(), input.end(), val.begin());
        return;
    }

    throw std::invalid_argument{"Invalid value: expected bytes, hex, or base64"};
}

template <typename T, typename = std::enable_if_t<is_bytes<T>>>
T from_hex_or_b64(std::string_view input) {
    T val;
    from_hex_or_b64(val, input);
    return val;
}

}  // namespace spns

namespace std {

template <>
struct hash<spns::AccountID> : spns::bytes_simple_hasher<spns::AccountID> {};
template <>
struct hash<spns::Ed25519PK> : spns::bytes_simple_hasher<spns::Ed25519PK> {};
template <>
struct hash<spns::X25519PK> : spns::bytes_simple_hasher<spns::X25519PK> {};
template <>
struct hash<spns::Blake2B_32> : spns::bytes_simple_hasher<spns::Blake2B_32> {};

}  // namespace std

namespace fmt {

template <typename T, typename Char>
struct formatter<T, Char, std::enable_if_t<spns::is_bytes<T>>> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const T& val, FormatContext& ctx) const {
        return formatter<std::string_view>::format(val.hex(), ctx);
    }
};

}  // namespace fmt
