#pragma once

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace pgvectorbench {

class Util {
public:
  static std::optional<std::string>
  getValueFromMap(const std::unordered_map<std::string, std::string> &map,
                  const std::string &key) {
    auto it = map.find(key);
    if (it != map.end()) {
      return it->second;
    } else {
      return std::nullopt;
    }
  }
};

template <typename T> class Percentile {
public:
  Percentile(bool less_better) : less_better_(less_better), sorted_(false) {}

  void add(const T &x) {
    elements.emplace_back(x);
    sorted_ = false;
  }

  void add(const T *array, size_t count) {
    size_t n = elements.size();
    elements.resize(n + count);
    memcpy(elements.data() + n, array, count * sizeof(T));
    sorted_ = false;
  }

  T best() {
    if (elements.empty()) {
      throw std::runtime_error("no data to profile!");
    }
    prepareProfile();
    return elements.front();
  }

  T worst() {
    if (elements.empty()) {
      throw std::runtime_error("no data to profile!");
    }
    prepareProfile();
    return elements.back();
  }

  double average() {
    if (elements.empty()) {
      throw std::runtime_error("no data to calculate average!");
    }
    size_t count = elements.size();
    double sum = .0;
    for (auto ele : elements) {
      sum += static_cast<double>(ele);
    }
    return sum / count;
  }

  T operator()(double percentage) {
    if (percentage < 0.0 || percentage > 100.0) {
      throw std::runtime_error("percentage should be within [0.0, 100.0]!");
    }
    if (elements.empty()) {
      throw std::runtime_error("no data to profile!");
    }
    prepareProfile();
    size_t count = elements.size();
    size_t n = std::min(
        count,
        std::max<size_t>(1, (size_t)std::ceil(count * percentage / 100.0)));
    return elements[n - 1];
  }

private:
  bool less_better_;
  bool sorted_;
  std::vector<T> elements;

  void prepareProfile() {
    if (!sorted_) {
      if (less_better_) {
        std::sort(elements.begin(), elements.end(), std::less<T>());
      } else {
        std::sort(elements.begin(), elements.end(), std::greater<T>());
      }
      sorted_ = true;
    }
  }
};

} // namespace pgvectorbench
