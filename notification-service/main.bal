import ballerina/io;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;
public function main() {
    // Config and clients same

kafka:ConsumerConfiguration consumerConfig = {
    bootstrapServers: KAFKA_BROKER,
    groupId: "notification-group",
    topics: ["schedule.updates", "ticket.validated"]
};
listener kafka:Listener kafkaListener = new (consumerConfig);

service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var rec in records {
            json payload = check value:fromBytes(rec.value).fromJsonStringWithType();
            if rec.topic == "schedule.updates" {
                log:printInfo(string `Trip ${payload.trip_id} updated to ${payload.status}`);
                // Extend: Query users for trip, send email
            } else if rec.topic == "ticket.validated" {
                log:printInfo(string `Ticket ${payload.ticket_id} validated for user ${payload.user_id}`);
            }
        }
        check caller->commit();
    }
}

// Optional health
service /notification on new http:Listener(9005) {
    resource function get health() returns string {
        return "OK";
    }
}
}
