#pragma once

#include <unordered_map>

#include "dataset/dataset.h"
#include "result.h"
#include "utils/client_factory.h"

namespace pgvectorbench {
using PhaseOptions = std::unordered_map<std::string, std::string>;

SetupResult setup(const DataSet *, const ClientFactory *, const PhaseOptions &);
LoadResult load(const DataSet *, const ClientFactory *, const PhaseOptions &);
IndexResult create_index(const DataSet *, const ClientFactory *, const PhaseOptions &);
void teardown(const DataSet *, const ClientFactory *, const PhaseOptions &);
} // namespace pgvectorbench
