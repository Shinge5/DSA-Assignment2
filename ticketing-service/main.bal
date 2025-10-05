import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;

configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongodbHost = "localhost";
configurable int mongodbPort = 27017;

service /ticketing on new http:Listener(9092) {
    private final kafka:Producer kafkaProducer;
    private final mongodb:Client dbClient;

    function init() returns error? {
        self.kafkaProducer = check new (kafkaBootstrapServers);
        self.dbClient = check new ("mongodb://" + mongodbHost + ":" + mongodbPort + "/ticketing_db");
    }

    resource function post createTicket(@http:Payload {mediaType: "application/json"} json payload) returns json|error {
        string ticketId = payload.ticketId.toString();
        check self.dbClient->insert({ticketId: ticketId, status: "CREATED"}, "tickets");
        check self.kafkaProducer->send({topic: "ticket.requests", value: payload.toJsonString().toBytes()});
        return {message: "Ticket " + ticketId + " created"};
    }
}