#include <cstdlib>
#include <memory>

#include <gtest/gtest.h>

#include "utils/client_factory.h"

namespace {

bool commandSucceeded(PGresult *result) {
  return PQresultStatus(result) == PGRES_COMMAND_OK;
}

bool tuplesSucceeded(PGresult *result) {
  return PQresultStatus(result) == PGRES_TUPLES_OK;
}

} // namespace

TEST(ClientFactoryTest, ConnectionRemainsUsableAfterSuccessfulAndFailedCopy) {
  const char *database = std::getenv("PGVECTORBENCH_TEST_DATABASE");
  if (database == nullptr) {
    GTEST_SKIP() << "Set PGVECTORBENCH_TEST_DATABASE to run database tests";
  }

  auto factory = pgvectorbench::ClientFactory::createBuilder()
                     .setDBName(database)
                     .build();
  auto client = factory->createClient();
  ASSERT_NE(client, nullptr);

  ASSERT_TRUE(client->executeQuery("CREATE TEMP TABLE copy_test (id integer);",
                                   commandSucceeded));

  constexpr char copy_statement[] = "COPY copy_test FROM STDIN";
  constexpr char valid_row[] = "1\n";
  ASSERT_TRUE(client->copy(copy_statement, valid_row, sizeof(valid_row) - 1,
                           commandSucceeded));

  ASSERT_TRUE(client->executeQuery("SELECT 1;", tuplesSucceeded));

  constexpr char invalid_row[] = "not-an-integer\n";
  EXPECT_FALSE(client->copy(copy_statement, invalid_row, sizeof(invalid_row) - 1,
                            commandSucceeded));

  EXPECT_TRUE(client->executeQuery("SELECT 1;", tuplesSucceeded));
}
