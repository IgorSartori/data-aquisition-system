// Igor Miranda Sartori 2020056784
// Júlia de Castro 2020056830

#include <iostream>
#include <vector>
#include <boost/asio.hpp>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <fstream>

using boost::asio::ip::tcp;

struct SensorData {
    char id[32];
    time_t timestamp;
    double reading;
};

std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

void storeSensorData(const std::string& filename, const SensorData& data) {
    std::ofstream file(filename, std::ios::out | std::ios::binary | std::ios::app);
    if (file.is_open()) {
        file.write(reinterpret_cast<const char*>(&data), sizeof(SensorData));
        file.close();
    } else {
        std::cout << "Error opening the file. Please try again." << std::endl;
    }
}

std::string retrieveSensorData(const std::string& filename, int numRecordsToRetrieve) {
    std::ifstream file(filename, std::ios::in | std::ios::binary);
    
    if (!file.is_open()) {
        return "ERROR|INVALID_SENSOR_ID\r\n";
    }

    std::ostringstream data;
    data << numRecordsToRetrieve;

    for (int i = 0; i < numRecordsToRetrieve; i++) {
        SensorData record;
        file.read(reinterpret_cast<char*>(&record), sizeof(SensorData));

        if (record.reading >= -0.000001 && record.reading <= 0.00001) {
            file.close();
            return "ERROR|INVALID_SENSOR_ID\r\n";
        }

        data << ";" << record.timestamp << "|" << record.reading;
    }

    file.close();
    data << "\r\n";
    return data.str();
}

std::vector<std::string> splitString(const std::string& input, char delimiter) {
    std::vector<std::string> parts;
    std::istringstream iss(input);
    std::string part;
    while (std::getline(iss, part, delimiter)) {
        parts.push_back(part);
    }
    return parts;
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() {
        readMessage();
    }

private:
void readMessage() {
    auto self(shared_from_this());

    boost::asio::async_read_until(socket_, buffer_, "\r\n",
        [this, self](const boost::system::error_code& error, size_t length) {
            if (!error) {
                std::istream is(&buffer_);
                std::string message(std::istreambuf_iterator<char>(is), {});

                std::cout << message << std::endl;

                std::vector<std::string> data = splitString(message, '|');
                std::string replyMessage;

                if (data[0] == "LOG") {
                    SensorData sensorData;
                    strcpy(sensorData.id, data[1].c_str());
                    sensorData.timestamp = string_to_time_t(data[2]);
                    sensorData.reading = stod(data[3]);

                    std::string filename = data[1] + ".dat";
                    storeSensorData(filename, sensorData);
                } else if (data[0] == "GET") {
                    int numRecords = stoi(data[2]);
                    std::string filename = data[1] + ".dat";
                    replyMessage = retrieveSensorData(filename, numRecords);
                }

                writeMessage(replyMessage);
            }
        }
    );
}


    void writeMessage(const std::string& message) {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(message),
            [this, self, message](const boost::system::error_code& ec, size_t length) {
                
                if (!ec) {
                    readMessage();
                }
            });
    }

    tcp::socket socket_;
    boost::asio::streambuf buffer_;
};

class Server {
public:
    Server(boost::asio::io_context& ioContext, short port)
        : acceptor_(ioContext, tcp::endpoint(tcp::v4(), port)) {
        acceptConnections();
    }

private:
    void acceptConnections() {
        acceptor_.async_accept(
            [this](const boost::system::error_code& ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<Session>(std::move(socket))->start();
                }
                acceptConnections();
            });
    }

    tcp::acceptor acceptor_;
};

int main() {
    boost::asio::io_context ioContext;
    Server server(ioContext, 9000);
    ioContext.run();
    return 0;
}