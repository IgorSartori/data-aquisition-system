#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <boost/asio.hpp>
#include <ctime>
#include <iomanip>
#include <sstream>

#pragma pack(push, 1)
struct LogRecord {
    char sensor_id[32]; // supondo um ID de sensor de até 32 caracteres
    std::time_t timestamp; // timestamp UNIX
    double value; // valor da leitura
};
#pragma pack(pop)

using boost::asio::ip::tcp;

std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

std::string time_t_to_string(std::time_t time) {
    std::tm* tm = std::localtime(&time);
    std::ostringstream ss;
    ss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket, std::vector<LogRecord>& log_records)
        : socket_(std::move(socket)), log_records_(log_records) {}

    void Start() {
        ReadMessage();
    }

private:
    void ReadMessage() {
        auto self(shared_from_this());
        boost::asio::async_read_until(socket_, buffer_, "\r\n",
            [this, self](const boost::system::error_code& ec, std::size_t length) {
                if (!ec) {
                    std::istream is(&buffer_);
                    std::string message;
                    std::getline(is, message);

                    // Check if the message is a query for a specific record
                    if (message.find("GET|") == 0) {
                        ProcessQuery(message);
                    } else {
                        ProcessMessage(message);
                    }
                }
            });
    }

    void ProcessQuery(const std::string& query) {
        // Parse the query to extract sensor ID and the number of records
        std::string sensor_id;
        int num_records;
        // Example: GET|SENSOR_001|10
        if (sscanf(query.c_str(), "GET|%31[^|]|%d", sensor_id, &num_records) == 2) {
            SendRecords(sensor_id, num_records);
        } else {
            WriteMessage("ERROR|INVALID_QUERY\r\n");
        }
    }

    void ProcessMessage(const std::string& message) {
        // Parse the incoming message and add the log record to log_records_
        LogRecord record;

        if (sscanf(message.c_str(), "LOG|%31[^|]|%d-%d-%dT%d:%d:%d|%lf",
                record.sensor_id,
                &record.timestamp,
                &record.value) == 3) {
            log_records_.push_back(record);
            WriteMessage("OK\r\n");

            // Append the record to a log file
            std::ofstream log_file(record.sensor_id, std::ios::app | std::ios::binary);
            log_file.write(reinterpret_cast<char*>(&record), sizeof(LogRecord));
        } else {
            WriteMessage("ERROR|INVALID_MESSAGE\r\n");
        }
    }

    void SendRecords(const std::string& sensor_id, int num_records) {
        // Find and send the last "num_records" records for the specified sensor_id.
        std::vector<LogRecord> results;

        std::ifstream log_file(sensor_id, std::ios::binary);
        if (log_file) {
            LogRecord record;
            while (log_file.read(reinterpret_cast<char*>(&record), sizeof(LogRecord))) {
                results.push_back(record);
            }
        }

        if (results.size() > num_records) {
            results.erase(results.begin(), results.end() - num_records);
        }

        for (const auto& record : results) {
            std::ostringstream oss;
            oss << "LOG|" << record.sensor_id << "|"
                << time_t_to_string(record.timestamp) << "|" << record.value << "\r\n";
            WriteMessage(oss.str());
        }
    }

    void WriteMessage(const std::string& message) {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(message),
            [this, self, message](const boost::system::error_code& ec, std::size_t /*length*/) {
                if (!ec) {
                    // Message sent successfully
                    ReadMessage(); // Continue reading the next message.
                }
            });
    }

    tcp::socket socket_;
    boost::asio::streambuf buffer_;
    std::vector<LogRecord>& log_records_;
};

class Server {
public:
    Server(boost::asio::io_context& io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        Accept();
    }

private:
    void Accept() {
        acceptor_.async_accept(
            [this](const boost::system::error_code& ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<Session>(std::move(socket), log_records_)->Start();
                }
                Accept();  // Continue accepting new connections.
            });
    }

    tcp::acceptor acceptor_;
    std::vector<LogRecord> log_records_;
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: data_acquisition_server <port>\n";
        return 1;
    }

    boost::asio::io_context io_context;

    Server server(io_context, std::atoi(argv[1]));

    io_context.run();

    return 0;
}
