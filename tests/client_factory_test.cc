#include <cstdlib>
#include <memory>

#include "utils/client_factory.h"

namespace {

bool commandSucceeded(PGresult *result) {
  return PQresultStatus(result) == PGRES_COMMAND_OK;
}

bool tuplesSucceeded(PGresult *result) {
  return PQresultStatus(result) == PGRES_TUPLES_OK;
}

} // namespace

int main() {
  const char *database = std::getenv("PGVECTORBENCH_TEST_DATABASE");
  if (database == nullptr) {
    return 77;
  }

  auto factory = pgvectorbench::ClientFactory::createBuilder()
                     .setDBName(database)
                     .build();
  auto client = factory->createClient();
  if (!client) {
    return 1;
  }

  if (!client->executeQuery("CREATE TEMP TABLE copy_test (id integer);",
                            commandSucceeded)) {
    return 1;
  }

  constexpr char copy_statement[] = "COPY copy_test FROM STDIN";
  constexpr char valid_row[] = "1\n";
  if (!client->copy(copy_statement, valid_row, sizeof(valid_row) - 1,
                    commandSucceeded)) {
    return 1;
  }

  if (!client->executeQuery("SELECT 1;", tuplesSucceeded)) {
    return 1;
  }

  constexpr char invalid_row[] = "not-an-integer\n";
  if (client->copy(copy_statement, invalid_row, sizeof(invalid_row) - 1,
                   commandSucceeded)) {
    return 1;
  }

  return client->executeQuery("SELECT 1;", tuplesSucceeded) ? 0 : 1;
}
