import ballerina/http;
import ballerina/kafka;

configurable string kafkaBootstrapServers = "localhost:9092";

service /payment on new http:Listener(9093) {
    private final kafka:Producer kafkaProducer;

    function init() returns error? {
        self.kafkaProducer = check new (kafkaBootstrapServers);
    }

    resource function post processPayment(@http:Payload {mediaType: "application/json"} json payload) returns json|error {
        string ticketId = payload.ticketId.toString();
        // Simulate payment processing
        check self.kafkaProducer->send({topic: "payments.processed", value: {"ticketId": ticketId, "status": "PAID"}.toJsonString().toBytes()});
        return {message: "Payment for ticket " + ticketId + " processed"};
    }
}