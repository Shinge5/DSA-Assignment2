import ballerina/http;
import ballerina/kafka;

configurable string kafkaBootstrapServers = "localhost:9092";

service /transport on new http:Listener(9091) {
    private final kafka:Producer kafkaProducer;

    function init() returns error? {
        self.kafkaProducer = check new (kafkaBootstrapServers);
    }

    resource function post createRoute(@http:Payload {mediaType: "application/json"} json payload) returns json|error {
        string routeId = payload.routeId.toString();
        check self.kafkaProducer->send({topic: "schedule.updates", value: payload.toJsonString().toBytes()});
        return {message: "Route " + routeId + " created and published"};
    }
}