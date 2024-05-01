#pragma once

#include <cassert>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace pgvectorbench {

namespace util {

class FileReader {
public:
  FileReader(const std::string &filename) : filename_(filename) {}

  ~FileReader() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

  void open() {
    assert(fd_ == -1);
    do {
      fd_ = ::open(filename_.c_str(), O_RDONLY);
    } while (fd_ < 0 && errno == EINTR);
    if (fd_ < 0) {
      throw std::runtime_error("Error opening file: " + filename_);
    }

    struct stat fileInfo;
    if (fstat(fd_, &fileInfo) != 0) {
      close(fd_);
      fd_ = -1;
      throw std::runtime_error("Error getting file size: " + filename_);
    }

    filesize_ = fileInfo.st_size;
  }

  size_t filesize() {
    assert(fd_ != -1);
    return filesize_;
  }

  void read(char *buffer, size_t n, size_t offset) {
    size_t left = n;
    ssize_t r = -1;
    char *ptr = buffer;
    while (left > 0) {
      r = pread(fd_, ptr, left, static_cast<off_t>(offset));
      if (r <= 0) {
        if (r == -1 && errno == EINTR) {
          continue;
        }
        throw std::runtime_error("Error happened when read");
      }
      ptr += r;
      offset += r;
      left -= r;
    }
  }

private:
  std::string filename_;
  int fd_{-1};
  size_t filesize_{0};
};

} // namespace util
} // namespace pgvectorbench
