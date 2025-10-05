import ballerina/io;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/time;

// Config and clients same

kafka:ConsumerConfiguration consumerConfig = {
    bootstrapServers: KAFKA_BROKER,
    groupId: "payment-group",
    topics: ["payments.process"]
};
listener kafka:Listener kafkaListener = new (consumerConfig);

service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var rec in records {
            json payload = check value:fromBytes(rec.value).fromJsonStringWithType();
            string ticketId = payload.ticket_id;
            decimal amount = check payload.amount;

            string status = "SUCCESS"; // Simulate (add random fail for testing)

            map<json> doc = { "ticket_id": ticketId, "amount": amount, "status": status, "paid_at": time:utcToString(time:utcNow()) };
            check mongoClient->insert("payments", doc);

            json confirm = { "ticket_id": ticketId, "status": status };
            byte[] serialized = confirm.toJsonString().toBytes();
            check kafkaProducer->send({ topic: "payments.processed", value: serialized });
        }
        check caller->commit();
    }
}

// Optional HTTP health
service /payment on new http:Listener(9004) {
    resource function get health() returns string {
        return "OK";
    }
}
