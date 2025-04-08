#include "read_config.h"
#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>  // single header library

using json = nlohmann::json;

bool loadNodeConfig(const std::string& jsonFile,
                    const std::string& nodeName,
                    NodeConfig& outConfig) {
    // Open the JSON file
    std::ifstream ifs(jsonFile);
    if (!ifs.is_open()) {
        std::cerr << "Error: Could not open JSON file: " << jsonFile << std::endl;
        return false;
    }

    // Parse the JSON
    json j;
    try {
        ifs >> j;
    } catch (std::exception& e) {
        std::cerr << "Error parsing JSON: " << e.what() << std::endl;
        return false;
    }

    // We expect something like: { "nodes": [ { "self":"A", "listen_address":"...", "neighbors": {...} } ] }
    if (!j.contains("nodes") || !j["nodes"].is_array()) {
        std::cerr << "Invalid JSON schema: missing 'nodes' array\n";
        return false;
    }

    // Find the object where "self" == nodeName
    for (auto& item : j["nodes"]) {
        if (!item.contains("self")) continue;
        if (item["self"].get<std::string>() == nodeName) {
            // Found our node
            outConfig.selfName = nodeName;
            // listen_address
            if (item.contains("listen_address")) {
                outConfig.listenAddress = item["listen_address"].get<std::string>();
            } else {
                std::cerr << "Node " << nodeName << " has no 'listen_address'\n";
                return false;
            }
            // neighbors
            if (item.contains("neighbors")) {
                auto neighObj = item["neighbors"];
                if (!neighObj.is_object()) {
                    std::cerr << "Node " << nodeName << " 'neighbors' is not object\n";
                    return false;
                }
                // Convert to map
                for (auto& [k, v] : neighObj.items()) {
                    outConfig.neighbors[k] = v.get<std::string>();
                }
            }
            // done
            return true;
        }
    }

    // If we got here, no matching node found
    std::cerr << "No config entry found for node '" << nodeName << "' in " << jsonFile << std::endl;
    return false;
}
