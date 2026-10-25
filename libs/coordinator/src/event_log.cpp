#include "radahn/coordinator/event_log.hpp"

#include <iostream>
#include <mutex>
#include <sstream>

namespace radahn::coordinator {

namespace {

std::mutex g_event_log_mutex;

}  // namespace

void log_event(
    std::string_view event,
    std::initializer_list<
        std::pair<std::string_view, std::string>
    > fields
) {
    std::ostringstream line;

    line << "event=" << event;

    for (const auto& [key, value] : fields) {
        line << ' ' << key << '=' << value;
    }

    const std::lock_guard lock{g_event_log_mutex};

    std::cout << line.str() << '\n';
}

}  // namespace radahn::coordinator
