#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <memory>
#include <oxen/log.hpp>

#include "bytes.hpp"
#include "config.hpp"
#include "hive/subscription.hpp"
#include "hivemind.hpp"

namespace py = pybind11;
using namespace py::literals;

struct HiveMindController {
    std::unique_ptr<spns::HiveMind> hivemind;

    HiveMindController(spns::Config conf) : hivemind{new spns::HiveMind{std::move(conf)}} {}

    void stop() { hivemind.reset(); }
};

namespace pybind11::detail {
template <typename T>
struct type_caster<T, std::enable_if_t<spns::is_bytes<T>>> {

    PYBIND11_TYPE_CASTER(T, const_name("bytes") + const_name<T::SIZE>());

    bool load(handle src, bool) {
        if (py::isinstance<py::bytes>(src)) {
            auto sv = py::cast<py::bytes>(src).operator std::string_view();
            if (sv.size() == T::SIZE) {
                std::memcpy(value.data(), sv.data(), T::SIZE);
                return true;
            }
        }
        return false;
    }

    static handle cast(const T& src, return_value_policy /* policy */, handle /* parent */) {
        return py::bytes(reinterpret_cast<const char*>(src.data()), src.size());
    }
};
}  // namespace pybind11::detail

PYBIND11_MODULE(core, m) {

    using namespace spns;

    // Python is kind of a pain in the ass about destruction, so use a wrapper class around the
    // actual HiveMind that lets us explicitly destruct.
    py::class_<HiveMindController>{m, "HiveMind"}
            .def(py::init<Config>(),
                 "Construts and starts a new HiveMind instance.  The instance will continue to run "
                 "until `.stop()` is called.",
                 "config"_a)
            .def("stop", &HiveMindController::stop);

    py::class_<Config>{m, "Config"}
            .def(py::init<>())
            .def_readwrite(
                    "oxend_rpc",
                    &Config::oxend_rpc,
                    "oxenmq address of the companion oxend RPC to use")
            .def_readwrite("pg_connect", &Config::pg_connect, "postgresql connection URL")
            .def_readwrite(
                    "hivemind_sock", &Config::hivemind_sock, "local hivemind admin oxenmq socket")
            .def_readwrite(
                    "hivemind_curve",
                    &Config::hivemind_curve,
                    "optional secondary hivemind curve-enabled listening socket")
            .def_readwrite(
                    "hivemind_curve_admin",
                    &Config::hivemind_curve_admin,
                    "set of X25519 pubkeys recognized as admin for incoming `hivemind_curve` "
                    "connections")
            .def_readwrite(
                    "pubkey",
                    &Config::pubkey,
                    "X25519 server pubkey; must be set (the default value will not work)")
            .def_readwrite(
                    "privkey",
                    &Config::privkey,
                    "X25519 server privkey; must be set (the default value will not work)")
            .def_property(
                    "filter_lifetime",
                    [](Config& self) { return self.filter_lifetime.count(); },
                    [](Config& self, int64_t seconds) { self.filter_lifetime = 1s * seconds; },
                    "the notification replay filter lifetime, in seconds")
            .def_property(
                    "notifier_wait",
                    [](Config& self) { return self.notifier_wait.count(); },
                    [](Config& self, int64_t milliseconds) {
                        self.notifier_wait = 1ms * milliseconds;
                    },
                    "how long, in milliseconds, after initialization to wait for notifier servers "
                    "to register themselves with the HiveMind instance")
            .def_readwrite(
                    "notifiers_expected",
                    &Config::notifiers_expected,
                    "Set of notification services that we expect; if non-empty then we will stop "
                    "the `notifier_wait` time early once we have registered notifiers for all the "
                    "values set here.")
            .def_property(
                    "subs_interval",
                    [](Config& self) { return self.subs_interval.count(); },
                    [](Config& self, int64_t seconds) { self.subs_interval = 1s * seconds; },
                    "how frequently, in seconds, between subscription rechecks (for push renewals, "
                    "expiries, etc.)")
            .def_readwrite(
                    "max_pending_connects",
                    &Config::max_pending_connects,
                    "maximum number of permitted simultaneous connection attempts.  (This is not "
                    "the number of simultaneous connections, just how many we new connections we "
                    "will attempt at once");

    class Logger {};
    py::class_<Logger>{m, "logger"}
            .def_static(
                    "start",
                    [](const std::string& out) {
                        oxen::log::clear_sinks();
                        if (out == "stdout" || out == "-" || out == "")
                            oxen::log::add_sink(oxen::log::Type::Print, "stdout");
                        else if (out == "stderr")
                            oxen::log::add_sink(oxen::log::Type::Print, "stderr");
                        else
                            oxen::log::add_sink(oxen::log::Type::File, out);
                    })
            .def_static(
                    "set_level",
                    [](const std::string& level) {
                        oxen::log::reset_level(oxen::log::level_from_string(level));
                    },
                    "Sets/resets the log level of all spns.core log categories to the given "
                    "value.\n"
                    "Can be any of 'trace', 'debug', 'info', 'warn', 'error', 'critical', or "
                    "'none'.",
                    "level"_a)
            .def_static(
                    "set_level",
                    [](const std::string& cat, const std::string& level) {
                        oxen::log::set_level(cat, oxen::log::level_from_string(level));
                    },
                    "Sets/resets the log level of a single spns.core log categories to the given "
                    "value.\n"
                    "Can be any of 'trace', 'debug', 'info', 'warning', 'error', 'critical', or "
                    "'none'.",
                    "category"_a,
                    "level"_a)
            .def_static(
                    "get_level",
                    [](const std::string& cat) { oxen::log::get_level(cat); },
                    "Gets the log level of the given spns.core log category")
            .def_static(
                    "get_level",
                    [](const std::string& cat) { oxen::log::get_level(cat); },
                    "Gets the log level of the given spns.core log category")
            .def_static(
                    "get_level",
                    []() { oxen::log::get_level_default(); },
                    "Gets the default log level of spns.core categories (those that have not been "
                    "changed via a category-specific `set_level`)")
            //
            ;

    static_assert(
            static_cast<int>(hive::SUBSCRIBE::_END) == 6,
            "pybind11 binding is missing SUBSCRIBE enum elements");

    py::enum_<hive::SUBSCRIBE>{m, "SUBSCRIBE"}
            .value("OK", hive::SUBSCRIBE::OK)
            .value("BAD_INPUT", hive::SUBSCRIBE::BAD_INPUT)
            .value("SERVICE_NOT_AVAILABLE", hive::SUBSCRIBE::SERVICE_NOT_AVAILABLE)
            .value("SERVICE_TIMEOUT", hive::SUBSCRIBE::SERVICE_TIMEOUT)
            .value("ERROR", hive::SUBSCRIBE::ERROR)
            .value("INTERNAL_ERROR", hive::SUBSCRIBE::INTERNAL_ERROR)
            .export_values();
}
