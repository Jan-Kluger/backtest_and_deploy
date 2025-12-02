#include <iostream>
#include <libpq-fe.h>

int main() {
    PGconn* conn = PQconnectdb("host=localhost dbname=crypto");

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return 1;
    }

    PGresult* res = PQexec(conn,
        "SELECT open_time, close FROM btcusdt_1m ORDER BY open_time DESC LIMIT 1");

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
        PQclear(res);
        PQfinish(conn);
        return 1;
    }

    if (PQntuples(res) > 0) {
        std::cout << PQgetvalue(res, 0, 0) << " " << PQgetvalue(res, 0, 1) << std::endl;
    }

    PQclear(res);
    PQfinish(conn);
    return 0;
}

