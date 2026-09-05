#pragma once

#include "dataset/dataset.h"
#include "result.h"
#include "utils/client_factory.h"

namespace pgvectorbench {

EnvironmentResult inspectEnvironment(const ClientFactory &factory);
std::map<std::string, std::string> readServerSettings(Client &client);
TableResult inspectTable(const ClientFactory &factory, const std::string &table);
TableResult preflight(const ClientFactory &factory, const DataSet &dataset,
                      const std::string &table, bool query = false);

} // namespace pgvectorbench
