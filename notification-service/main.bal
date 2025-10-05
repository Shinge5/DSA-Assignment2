import ballerina/http;
import ballerina/kafka;

configurable string kafkaBootstrapServers = "localhost:9092";

service /notification on new http:Listener(9094) {
    private final kafka:Consumer kafkaConsumer;

    function init() returns error? {
        self.kafkaConsumer = check new (kafkaBootstrapServers, "notification-group", ["ticket.requests", "schedule.updates"]);
    }

    resource function get .() returns json|error {
        kafka:ConsumerRecord[] records = check self.kafkaConsumer->poll(1000);
        if records.length() > 0 {
            json content = records[0].value.toJson();
            return {message: "Notification: " + content.toString()};
        }
        return {message: "No updates"};
    }
}