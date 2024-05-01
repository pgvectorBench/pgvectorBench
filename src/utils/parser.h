#pragma once

#include <algorithm>
#include <functional>
#include <sstream>
#include <string>
#include <unordered_map>

namespace pgvectorbench {

class CSVParser {
public:
  static void parseLine(const std::string &line,
                        std::function<void(std::string &)> const &token_handler,
                        char sep = ',') {
    std::istringstream iss(line);
    std::string token;

    while (std::getline(iss, token, sep)) {
      // remove white spaces
      token.erase(std::remove_if(token.begin(), token.end(), ::isspace),
                  token.end());

      if (!token.empty())
        token_handler(token);
    }
  }
};
} // namespace pgvectorbench
