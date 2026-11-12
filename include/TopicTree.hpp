#pragma once

#include "MqttProtocol.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace pocketmqtt {

/// A topic name that can be published to: non-empty, valid UTF-8, no wildcards.
[[nodiscard]] bool isValidTopicName(std::string_view topic) noexcept;

/// A subscription filter: non-empty, valid UTF-8, `+` and `#` only as whole levels,
/// `#` only as the last level.
[[nodiscard]] bool isValidTopicFilter(std::string_view filter) noexcept;

/// Reference matcher for a single filter/topic pair (MQTT 3.1.1 section 4.7), including the
/// rule that wildcards at the first level never match topics starting with `$`.
[[nodiscard]] bool topicMatches(std::string_view filter, std::string_view topic) noexcept;

/// Subscription index organised as a trie keyed by topic levels.
///
/// Matching a published topic walks only the branches that can match (`exact`, `+`, `#`) at
/// each level, so its cost depends on topic depth and matching subscriptions, not on the
/// total number of subscriptions in the broker.
class TopicTree {
public:
    using SubscriberId = std::uint64_t;
    /// Matched subscribers with the maximum QoS over all of their matching filters.
    using MatchResult = std::unordered_map<SubscriberId, mqtt::QoS>;

    TopicTree() = default;
    TopicTree(const TopicTree&) = delete;
    TopicTree& operator=(const TopicTree&) = delete;
    TopicTree(TopicTree&&) noexcept = default;
    TopicTree& operator=(TopicTree&&) noexcept = default;

    /// Adds or replaces (MQTT-3.8.4-3) the subscription of `subscriber` to `filter`.
    /// `filter` must satisfy isValidTopicFilter().
    void subscribe(std::string_view filter, SubscriberId subscriber, mqtt::QoS qos);

    /// Returns true if the subscription existed. Empty branches are pruned.
    bool unsubscribe(std::string_view filter, SubscriberId subscriber);

    /// Clears `out` and fills it with every subscriber whose filter matches `topic`.
    void match(std::string_view topic, MatchResult& out) const;

    [[nodiscard]] std::size_t size() const noexcept { return subscriptionCount_; }

private:
    struct Node {
        // std::less<> enables lookups by string_view without allocating a key string.
        std::map<std::string, std::unique_ptr<Node>, std::less<>> children;
        std::unordered_map<SubscriberId, mqtt::QoS> subscribers;

        [[nodiscard]] bool empty() const noexcept { return children.empty() && subscribers.empty(); }
    };

    static void collect(const Node& node, MatchResult& out);
    static void matchLevel(const Node& node, const std::vector<std::string_view>& levels,
                           std::size_t index, MatchResult& out);
    static bool removeFrom(Node& node, const std::vector<std::string_view>& levels,
                           std::size_t index, SubscriberId subscriber);

    Node root_;
    std::size_t subscriptionCount_ = 0;
};

}  // namespace pocketmqtt
