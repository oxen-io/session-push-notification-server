#include "pg.hpp"

#include <oxen/log.hpp>

namespace spns {

namespace log = oxen::log;
static auto cat = log::Cat("pg");

PGConnPool::PGConnPool(std::string pg_connect, int initial_conns) :
        pg_connect_{std::move(pg_connect)} {
    log::info(cat, "Connecting to postgresql database @ {}", pg_connect_);
    auto conn0 = make_conn();
    if (initial_conns > 0) {
        idle_conns_.emplace_back(std::move(conn0), steady_clock::now());
        for (int i = 1; i < initial_conns; i++)
            idle_conns_.emplace_back(make_conn(), steady_clock::now());
    }
}

PGConn PGConnPool::get() {
    std::unique_ptr<pqxx::connection> conn;
    while (!conn) {
        conn = pop_conn();
        if (!conn)  // no conn available
            break;
        else if (!conn->is_open())
            conn.reset();  // found one, but it's dead; try again
    }
    clear_idle_conns();

    if (!conn)
        conn = make_conn();
    return PGConn{*this, std::move(conn)};
}

void PGConnPool::release(std::unique_ptr<pqxx::connection> conn) {
    {
        std::lock_guard lock{mutex_};
        idle_conns_.emplace_back(std::move(conn), steady_clock::now());
    }
    clear_idle_conns();
}

void PGConnPool::clear_idle_conns() {
    std::lock_guard lock{mutex_};
    if (max_idle >= 0)
        while (idle_conns_.size() > max_idle)
            idle_conns_.pop_front();

    if (max_idle_time > 0s) {
        auto cutoff = steady_clock::now() - max_idle_time;
        while (idle_conns_.front().second < cutoff)
            idle_conns_.pop_front();
    }
}

std::unique_ptr<pqxx::connection> PGConnPool::pop_conn() {
    std::lock_guard lock{mutex_};
    if (!idle_conns_.empty()) {
        auto conn = std::move(idle_conns_.back().first);
        idle_conns_.pop_back();
        return conn;
    }
    return nullptr;
}

std::unique_ptr<pqxx::connection> PGConnPool::make_conn() {
    log::debug(cat, "Creating pg connection");
    std::lock_guard lock{mutex_};
    count_++;
    return std::make_unique<pqxx::connection>(pg_connect_);
}

PGConn::~PGConn() {
    if (conn_)
        pool_.release(std::move(conn_));
}

}  // namespace spns

namespace pqxx {

spns::Int16ArrayLoader string_traits<spns::Int16ArrayLoader>::from_string(std::string_view in) {
    if (in.size() <= 2)
        return {};
    auto* pos = in.data();
    assert(*pos == '{');
    pos++;
    auto* back = &in.back();
    assert(*back == '}');
    spns::Int16ArrayLoader vals;
    vals.a.reserve(std::count(pos, back, ','));
    while (pos < back) {
        auto& ns = vals.a.emplace_back();
        auto [ptr, ec] = std::from_chars(pos, back, ns);
        assert(ec == std::errc());
        assert(ptr == back || *ptr == ',');
        pos = ptr + 1;
    }
    return vals;
}

}  // namespace pqxx
