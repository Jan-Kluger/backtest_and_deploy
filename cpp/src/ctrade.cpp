#include <iostream>
#include <libpq-fe.h>
#include <stdexcept>

double get_most_recent_price() {
  PGconn *conn = PQconnectdb("host=localhost dbname=ctrade");

  if (PQstatus(conn) != CONNECTION_OK) {
    std::cerr << "Connection failed: " << PQerrorMessage(conn) << std::endl;
    PQfinish(conn);
    return 1;
  }

  PGresult *res =
      PQexec(conn, "SELECT ts, close FROM btcusdt_1m ORDER BY ts DESC LIMIT 1");

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
    PQclear(res);
    PQfinish(conn);
    return 1;
  }

  if (PQntuples(res) == 0) {
    PQclear(res);
    PQfinish(conn);
    throw std::runtime_error("No rows returned");
  }

  double price = std::stod(PQgetvalue(res, 0, 1));

  PQclear(res);
  PQfinish(conn);

  return price;
}
