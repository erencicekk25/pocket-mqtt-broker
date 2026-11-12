#include "TestFramework.hpp"
#include "TopicTree.hpp"

#include <string>
#include <vector>

using pocketmqtt::isValidTopicFilter;
using pocketmqtt::isValidTopicName;
using pocketmqtt::topicMatches;
using pocketmqtt::TopicTree;
using pocketmqtt::mqtt::QoS;

TEST_CASE(topic_filter_validation) {
    for (const char* valid : {"#", "+", "a/b/c", "sensors/+/temp", "sensors/#", "+/+", "/", "a//b",
                              "+/tennis/#", "$SYS/#"}) {
        CHECK(isValidTopicFilter(valid));
    }
    for (const char* invalid : {"", "sport+", "sport/#/ranking", "a/b#", "#/a", "a/++/b"}) {
        CHECK(!isValidTopicFilter(invalid));
    }
}

TEST_CASE(topic_name_validation) {
    CHECK(isValidTopicName("sensors/kitchen/temp"));
    CHECK(isValidTopicName("/"));
    CHECK(!isValidTopicName(""));
    CHECK(!isValidTopicName("sensors/+/temp"));
    CHECK(!isValidTopicName("sensors/#"));
}

TEST_CASE(topic_matching_follows_spec_examples) {
    // Examples from MQTT 3.1.1 section 4.7.
    CHECK(topicMatches("sport/tennis/player1/#", "sport/tennis/player1"));
    CHECK(topicMatches("sport/tennis/player1/#", "sport/tennis/player1/ranking"));
    CHECK(topicMatches("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon"));
    CHECK(topicMatches("sport/#", "sport"));
    CHECK(topicMatches("sport/tennis/+", "sport/tennis/player1"));
    CHECK(!topicMatches("sport/tennis/+", "sport/tennis/player1/ranking"));
    CHECK(!topicMatches("sport/+", "sport"));
    CHECK(topicMatches("sport/+", "sport/"));
    CHECK(topicMatches("+/+", "/finance"));
    CHECK(topicMatches("/+", "/finance"));
    CHECK(!topicMatches("+", "/finance"));
    CHECK(topicMatches("sensors/+/temp", "sensors/kitchen/temp"));
    CHECK(!topicMatches("sensors/+/temp", "sensors/kitchen/humidity"));
    // Wildcards at the first level never match $-topics.
    CHECK(!topicMatches("#", "$SYS/uptime"));
    CHECK(!topicMatches("+/uptime", "$SYS/uptime"));
    CHECK(topicMatches("$SYS/#", "$SYS/uptime"));
}

TEST_CASE(tree_merges_overlapping_subscriptions_with_max_qos) {
    TopicTree tree;
    tree.subscribe("home/#", 1, QoS::AtMostOnce);
    tree.subscribe("home/+/light", 1, QoS::AtLeastOnce);
    tree.subscribe("home/kitchen/light", 2, QoS::AtMostOnce);

    TopicTree::MatchResult result;
    tree.match("home/kitchen/light", result);
    CHECK_EQ(result.size(), 2u);
    CHECK_EQ(result.at(1), QoS::AtLeastOnce);
    CHECK_EQ(result.at(2), QoS::AtMostOnce);
}

TEST_CASE(tree_resubscribe_replaces_qos_and_unsubscribe_prunes) {
    TopicTree tree;
    tree.subscribe("a/b", 7, QoS::AtMostOnce);
    tree.subscribe("a/b", 7, QoS::AtLeastOnce);
    CHECK_EQ(tree.size(), 1u);

    TopicTree::MatchResult result;
    tree.match("a/b", result);
    CHECK_EQ(result.at(7), QoS::AtLeastOnce);

    CHECK(tree.unsubscribe("a/b", 7));
    CHECK(!tree.unsubscribe("a/b", 7));
    CHECK_EQ(tree.size(), 0u);
    tree.match("a/b", result);
    CHECK(result.empty());
}

TEST_CASE(tree_agrees_with_reference_matcher) {
    const std::vector<std::string> filters = {
        "#",       "+",          "+/+",          "a",         "a/#",        "a/+",
        "a/+/c",   "a/b/c",      "a/b/#",        "+/b/+",     "/+",         "+/#",
        "$SYS/#",  "$SYS/+",     "a//c",         "a/+/+/d",   "+/+/+",      "b/#"};
    const std::vector<std::string> topics = {
        "a",     "a/b",     "a/b/c",   "a/b/c/d", "a//c",       "/a",       "/",
        "b",     "b/b/b",   "x/b/y",   "a/x/c",   "$SYS/load",  "$SYS",     "a/b/x/d"};

    TopicTree tree;
    for (std::size_t i = 0; i < filters.size(); ++i) {
        tree.subscribe(filters[i], i, QoS::AtMostOnce);
    }

    TopicTree::MatchResult result;
    for (const std::string& topic : topics) {
        tree.match(topic, result);
        for (std::size_t i = 0; i < filters.size(); ++i) {
            const bool expected = topicMatches(filters[i], topic);
            const bool actual = result.count(i) != 0;
            if (expected != actual) {
                ::pmtest::fail(__FILE__, __LINE__,
                               "filter '" + filters[i] + "' vs topic '" + topic + "': tree says " +
                                   (actual ? "match" : "no match"));
            }
        }
    }
}
