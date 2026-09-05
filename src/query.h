#pragma once

#include <string>
#include <unordered_map>

#include "result.h"

namespace pgvectorbench {

struct DataSet;
class ClientFactory;

// Throws on invalid input or failed queries; never returns partial statistics.
QueryResult query(const DataSet *dataset, const ClientFactory *cf,
                  const std::unordered_map<std::string, std::string> &options);

} // namespace pgvectorbench
