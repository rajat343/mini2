#pragma once
#include <string>
#include <map>

// Data structure to hold a node's config
struct NodeConfig {
    std::string selfName;            // e.g. "B"
    std::string listenAddress;       // e.g. "0.0.0.0:50052"
    std::map<std::string, std::string> neighbors; // key=neighborName, value="ip:port"
};

bool loadNodeConfig(const std::string& jsonFile,
                    const std::string& nodeName,
                    NodeConfig& outConfig);
