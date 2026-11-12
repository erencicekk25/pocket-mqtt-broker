#include "TopicTree.hpp"

#include <algorithm>

namespace pocketmqtt {
namespace {

constexpr std::string_view kSingleLevel = "+";
constexpr std::string_view kMultiLevel = "#";

/// Splits "a/b//c" into {"a", "b", "", "c"}. Empty levels are legal in MQTT.
std::vector<std::string_view> splitLevels(std::string_view topic) {
    std::vector<std::string_view> levels;
    levels.reserve(8);
    std::size_t start = 0;
    for (;;) {
        const std::size_t slash = topic.find('/', start);
        if (slash == std::string_view::npos) {
            levels.push_back(topic.substr(start));
            return levels;
        }
        levels.push_back(topic.substr(start, slash - start));
        start = slash + 1;
    }
}

bool isSystemTopic(std::string_view topic) noexcept { return !topic.empty() && topic.front() == '$'; }

void mergeMax(TopicTree::MatchResult& out, TopicTree::SubscriberId subscriber, mqtt::QoS qos) {
    const auto [it, inserted] = out.emplace(subscriber, qos);
    if (!inserted && qos > it->second) {
        it->second = qos;  // Overlapping filters: deliver once with the highest QoS.
    }
}

}  // namespace

bool isValidTopicName(std::string_view topic) noexcept {
    return !topic.empty() && topic.size() <= 0xFFFF && topic.find_first_of("+#") == std::string_view::npos &&
           mqtt::isValidMqttUtf8(topic);
}

bool isValidTopicFilter(std::string_view filter) noexcept {
    if (filter.empty() || filter.size() > 0xFFFF || !mqtt::isValidMqttUtf8(filter)) {
        return false;
    }
    std::size_t levelStart = 0;
    for (std::size_t i = 0; i <= filter.size(); ++i) {
        if (i != filter.size() && filter[i] != '/') {
            continue;
        }
        const std::string_view level = filter.substr(levelStart, i - levelStart);
        const bool hasWildcard = level.find_first_of("+#") != std::string_view::npos;
        if (hasWildcard && level.size() != 1) {
            return false;  // "sport+" or "a#": wildcards must occupy a whole level.
        }
        if (level == kMultiLevel && i != filter.size()) {
            return false;  // '#' must be the last level.
        }
        levelStart = i + 1;
    }
    return true;
}

bool topicMatches(std::string_view filter, std::string_view topic) noexcept {
    if (isSystemTopic(topic) && !filter.empty() && (filter.front() == '+' || filter.front() == '#')) {
        return false;  // MQTT-4.7.2-1
    }
    std::size_t f = 0;
    std::size_t t = 0;
    for (;;) {
        const std::size_t fEnd = std::min(filter.find('/', f), filter.size());
        const std::string_view filterLevel = filter.substr(f, fEnd - f);
        if (filterLevel == kMultiLevel) {
            return true;  // Matches the parent level and everything below it.
        }
        if (t > topic.size()) {
            return false;  // Topic ran out of levels before the filter did.
        }
        const std::size_t tEnd = std::min(topic.find('/', t), topic.size());
        const std::string_view topicLevel = topic.substr(t, tEnd - t);
        if (filterLevel != kSingleLevel && filterLevel != topicLevel) {
            return false;
        }
        const bool filterDone = fEnd == filter.size();
        const bool topicDone = tEnd == topic.size();
        if (filterDone || topicDone) {
            if (filterDone && topicDone) return true;
            // "sport/#" also matches "sport": the only allowed leftover is a trailing "/#".
            return topicDone && filter.substr(fEnd) == "/#";
        }
        f = fEnd + 1;
        t = tEnd + 1;
    }
}

void TopicTree::subscribe(std::string_view filter, SubscriberId subscriber, mqtt::QoS qos) {
    Node* node = &root_;
    for (const std::string_view level : splitLevels(filter)) {
        auto it = node->children.find(level);
        if (it == node->children.end()) {
            it = node->children.emplace(std::string(level), std::make_unique<Node>()).first;
        }
        node = it->second.get();
    }
    const auto [it, inserted] = node->subscribers.insert_or_assign(subscriber, qos);
    (void)it;
    if (inserted) {
        ++subscriptionCount_;
    }
}

bool TopicTree::unsubscribe(std::string_view filter, SubscriberId subscriber) {
    const bool removed = removeFrom(root_, splitLevels(filter), 0, subscriber);
    if (removed) {
        --subscriptionCount_;
    }
    return removed;
}

bool TopicTree::removeFrom(Node& node, const std::vector<std::string_view>& levels,
                           std::size_t index, SubscriberId subscriber) {
    if (index == levels.size()) {
        return node.subscribers.erase(subscriber) > 0;
    }
    const auto it = node.children.find(levels[index]);
    if (it == node.children.end()) {
        return false;
    }
    const bool removed = removeFrom(*it->second, levels, index + 1, subscriber);
    if (removed && it->second->empty()) {
        node.children.erase(it);  // Prune so the trie never accumulates dead branches.
    }
    return removed;
}

void TopicTree::match(std::string_view topic, MatchResult& out) const {
    out.clear();
    const std::vector<std::string_view> levels = splitLevels(topic);
    matchLevel(root_, levels, 0, out);
}

void TopicTree::collect(const Node& node, MatchResult& out) {
    for (const auto& [subscriber, qos] : node.subscribers) {
        mergeMax(out, subscriber, qos);
    }
}

void TopicTree::matchLevel(const Node& node, const std::vector<std::string_view>& levels,
                           std::size_t index, MatchResult& out) {
    const auto multi = node.children.find(kMultiLevel);

    if (index == levels.size()) {
        collect(node, out);
        if (multi != node.children.end()) {
            collect(*multi->second, out);  // "a/#" matches "a".
        }
        return;
    }

    // Wildcards in the first level must not match "$SYS/..."-style topics.
    const bool wildcardsAllowed = index != 0 || !isSystemTopic(levels[0]);
    if (wildcardsAllowed) {
        if (multi != node.children.end()) {
            collect(*multi->second, out);
        }
        const auto single = node.children.find(kSingleLevel);
        if (single != node.children.end()) {
            matchLevel(*single->second, levels, index + 1, out);
        }
    }
    const auto exact = node.children.find(levels[index]);
    if (exact != node.children.end()) {
        matchLevel(*exact->second, levels, index + 1, out);
    }
}

}  // namespace pocketmqtt
