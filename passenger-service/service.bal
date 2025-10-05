import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/uuid;
import ballerina/time;

// Config (use environment vars in Docker)
const string KAFKA_BROKER = "localhost:9092";  // Change to kafka:9092 in Docker
const string MONGO_URI = "mongodb://localhost:27017";  // mongodb://mongodb:27017 in Docker
const string DB_NAME = "transport_db";

// Models (shared conceptually across services)
type User record {|
    string id;
    string username;
    string password;
    string role;
|};

type Route record {|
    string id;
    string name;
    string[] stops;
|};

type Trip record {|
    string id;
    string route_id;
    string schedule_time;
    string vehicle_type;
    string status;
    int seats_available;
|};

type Ticket record {|
    string id;
    string user_id;
    string trip_id;
    string type;
    string status;
    string created_at;
    string? paid_at;
    string? validated_at;
    string? expired_at;
    int? seat;
|};

type TicketRequest record {|
    string user_id;
    string trip_id;
    string type;
|};

// MongoDB client
mongodb:Connection mongoConn = { connection: MONGO_URI };
mongodb:Client mongoClient = check new (mongoConn, DB_NAME);

// Kafka producer
kafka:ProducerConfiguration producerConfig = {
    bootstrapServers: KAFKA_BROKER,
    clientId: "passenger-producer",
    acks: "all",
    retryCount: 3
};
kafka:Producer kafkaProducer = check new (producerConfig);

// HTTP service
service /passenger on new http:Listener(9001) {
    resource function post register(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string username = check payload.username;
        string password = check payload.password; // Hash in production

        map<json> doc = { "username": username, "password": password, "role": "passenger" };
        check mongoClient->insert("users", doc);
        return { "message": "Registered", "id": doc["_id"] };
    }

    resource function post login(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string username = check payload.username;
        string password = check payload.password;

        map<json> filter = { "username": username, "password": password };
        json|error user = mongoClient->findOne("users", filter);
        if user is json {
            return { "message": "Login success", "user_id": user._id };
        } else {
            return { "error": "Invalid credentials" };
        }
    }

    resource function get routes() returns json[]|error {
        mongodb:Cursor cursor = check mongoClient->find("routes");
        json[] routes = [];
        check from json r in cursor
            do { routes.push(r); };
        return routes;
    }

    resource function get trips() returns json[]|error {
        mongodb:Cursor cursor = check mongoClient->find("trips");
        json[] trips = [];
        check from json t in cursor
            do { trips.push(t); };
        return trips;
    }

    resource function post purchase(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        TicketRequest request = check payload.fromJsonWithType(TicketRequest);

        byte[] serialized = request.toJsonString().toBytes();
        check kafkaProducer->send({ topic: "ticket.requests", value: serialized });
        return { "message": "Ticket request sent" };
    }

    resource function get tickets/[string userId]() returns json[]|error {
        map<json> filter = { "user_id": userId };
        mongodb:Cursor cursor = check mongoClient->find("tickets", filter);
        json[] tickets = [];
        check from json t in cursor
            do { tickets.push(t); };
        return tickets;
    }
}

