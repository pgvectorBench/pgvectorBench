#pragma once

#include <functional>
#include <libpq-fe.h>
#include <memory>
#include <spdlog/spdlog.h>

namespace pgvectorbench {

class Client {
public:
  Client(PGconn *conn) : connection_(conn) {}

  ~Client() { PQfinish(connection_); }

  // return false if execute failed
  bool executeQuery(const char *query,
                    std::function<bool(PGresult *)> const &resultHandler) {
    assert(query != nullptr);
    PGresult *res = PQexec(connection_, query);
    if (PQresultStatus(res) == PGRES_COMMAND_OK ||
        PQresultStatus(res) == PGRES_TUPLES_OK) {
      if (resultHandler(res)) {
        SPDLOG_DEBUG("process result succeed");
        PQclear(res);
        return true;
      }
      SPDLOG_ERROR("process result failed: {}", query);
    } else {
      SPDLOG_ERROR("query: {} failed with {}", query,
                   PQresultErrorMessage(res));
    }
    PQclear(res);
    return false;
  }

  bool copy(const char *copy_table_stmt, const char *buffer, size_t length,
            std::function<bool(PGresult *)> const &resultHandler) {

    std::unique_ptr<PGresult, decltype(&PQclear)> res(
        PQexec(connection_, copy_table_stmt), &PQclear);

    SPDLOG_DEBUG("copy detail: {}, {}", copy_table_stmt, buffer);

    if (PQresultStatus(res.get()) != PGRES_COPY_IN) {
      SPDLOG_ERROR("unexpected COPY response: {}",
                   PQresultErrorMessage(res.get()));
      return false;
    }

    auto nr = PQputCopyData(connection_, buffer, length);
    if (nr != 1) {
      SPDLOG_ERROR("put data into COPY stream failed: {}",
                   PQerrorMessage(connection_));
      return false;
    }

    nr = PQputCopyEnd(connection_, NULL);
    if (nr != 1) {
      SPDLOG_ERROR("close data into COPY stream failed: {}",
                   PQerrorMessage(connection_));
      return false;
    }

    res.reset(PQgetResult(connection_));
    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
      SPDLOG_ERROR("excute COPY command failed: {}",
                   PQresultErrorMessage(res.get()));
      return false;
    }

    if (resultHandler(res.get())) {
      SPDLOG_DEBUG("process result succeed");
      return true;
    }

    SPDLOG_ERROR("process result failed");
    return false;
  }

private:
  PGconn *connection_;
};

class ClientFactory {
public:
  ~ClientFactory() = default;

  class Builder {
  public:
    Builder() {}

    Builder &setHost(const std::string &host) {
      pghost_ = host;
      return *this;
    }

    Builder &setPort(const std::string &port) {
      pgport_ = port;
      return *this;
    }

    Builder &setUser(const std::string &user) {
      user_ = user;
      return *this;
    }

    Builder &setPassword(const std::string &password) {
      password_ = password;
      return *this;
    }

    Builder &setDBName(const std::string &dbname) {
      dbname_ = dbname;
      return *this;
    }

    Builder &setProgname(const std::string progname) {
      progname_ = progname;
      return *this;
    }

    std::unique_ptr<ClientFactory> build() {
      std::vector<std::string> keywords;
      std::vector<std::string> values;
      // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
      if (!pghost_.empty()) {
        keywords.emplace_back("host");
        values.emplace_back(std::move(pghost_));
      }

      if (!pgport_.empty()) {
        keywords.emplace_back("port");
        values.emplace_back(std::move(pgport_));
      }

      if (!user_.empty()) {
        keywords.emplace_back("user");
        values.emplace_back(std::move(user_));
      }

      if (!password_.empty()) {
        keywords.emplace_back("password");
        values.emplace_back(std::move(password_));
      }

      if (!dbname_.empty()) {
        keywords.emplace_back("dbname");
        values.emplace_back(std::move(dbname_));
      }

      keywords.emplace_back("application_name");
      values.emplace_back(std::move(progname_));

      return std::make_unique<ClientFactory>(std::move(keywords),
                                             std::move(values));
    }

  private:
    std::string pghost_;
    std::string pgport_;
    std::string user_;
    std::string password_;
    std::string dbname_;
    std::string progname_{"pgvectorbench"};
  };

  static Builder createBuilder() { return Builder(); }

  std::unique_ptr<Client> createClient() const {
    PGconn *conn = PQconnectdbParams(keywords_.data(), values_.data(),
                                     1 /* expand_dbname */);
    if (!conn) {
      SPDLOG_ERROR("connecting to database failed");
      return nullptr;
    }

    if (PQstatus(conn) != CONNECTION_OK) {
      SPDLOG_ERROR("connection failed: {}", PQerrorMessage(conn));
      PQfinish(conn);
      return nullptr;
    }

    return std::make_unique<Client>(conn);
  }

  bool pingServer() {
    const auto result =
        PQpingParams(keywords_.data(), values_.data(), 1 /* expand_dbname */);
    switch (result) {
    case PQPING_OK:
      return true;
    case PQPING_REJECT:
      SPDLOG_ERROR("ping server rejected");
      return false;
    case PQPING_NO_RESPONSE:
      SPDLOG_ERROR("ping server no response");
      return false;
    case PQPING_NO_ATTEMPT:
      SPDLOG_ERROR("ping server client no attempt");
      return false;
    default:;
      SPDLOG_ERROR("should not be here");
    }

    return false;
  }

  ClientFactory(std::vector<std::string> keywords,
                std::vector<std::string> values)
      : keywords_holder(std::move(keywords)), values_holder(std::move(values)) {
    assert(keywords_holder.size() == values_holder.size());
    for (int i = 0; i < keywords_holder.size(); i++) {
      keywords_.push_back(keywords_holder[i].c_str());
      values_.push_back(values_holder[i].c_str());
    }
    keywords_.push_back(NULL);
    values_.push_back(NULL);
  }

private:
  std::vector<std::string> keywords_holder;
  std::vector<std::string> values_holder;
  std::vector<char const *> keywords_;
  std::vector<char const *> values_;
};

} // namespace pgvectorbench
