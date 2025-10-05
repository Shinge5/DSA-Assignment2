import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/uuid;
import ballerina/time;

// Config and models same

// Clients same

// Kafka consumer
kafka:ConsumerConfiguration consumerConfig = {
    bootstrapServers: KAFKA_BROKER,
    groupId: "ticketing-group",
    topics: ["ticket.requests", "payments.processed"]
};
listener kafka:Listener kafkaListener = new (consumerConfig);

service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string topic = rec.topic;
            json payload = check value:fromBytes(rec.value).fromJsonStringWithType();

            if topic == "ticket.requests" {
                string id = uuid:createType4AsString();
                map<json> doc = {
                    "_id": id,
                    "user_id": payload.user_id,
                    "trip_id": payload.trip_id,
                    "type": payload.type,
                    "status": "CREATED",
                    "created_at": time:utcToString(time:utcNow())
                };

                // Bonus: Seat reservation with concurrency
                lock {
                    map<json> tripFilter = { "_id": payload.trip_id };
                    json? trip = check mongoClient->findOne("trips", tripFilter);
                    if trip is json && <int>trip.seats_available > 0 {
                        int newSeats = <int>trip.seats_available - 1;
                        map<json> updateTrip = { "$set": { "seats_available": newSeats } };
                        check mongoClient->update("trips", tripFilter, updateTrip);
                        doc["seat"] = newSeats + 1;
                    } else {
                        log:printError("No seats");
                        continue;
                    }
                }

                check mongoClient->insert("tickets", doc);

                // Send to payment
                json paymentReq = { "ticket_id": id, "amount": 10.0 }; // Simulate amount
                byte[] serialized = paymentReq.toJsonString().toBytes();
                check kafkaProducer->send({ topic: "payments.process", value: serialized });
            } else if topic == "payments.processed" {
                string ticketId = payload.ticket_id;
                string status = payload.status == "SUCCESS" ? "PAID" : "FAILED";

                map<json> filter = { "_id": ticketId };
                map<json> updateDoc = { "$set": { "status": status, "paid_at": time:utcToString(time:utcNow()) } };
                check mongoClient->update("tickets", filter, updateDoc);
            }
        }
        check caller->commit();
    }
}

// HTTP for validation
service /ticketing on new http:Listener(9003) {
    resource function post validate(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string ticketId = check payload.ticket_id;

        map<json> filter = { "_id": ticketId, "status": "PAID" };
        json? ticket = check mongoClient->findOne("tickets", filter);
        if ticket is json {
            map<json> updateDoc = { "$set": { "status": "VALIDATED", "validated_at": time:utcToString(time:utcNow()) } };
            check mongoClient->update("tickets", filter, updateDoc);

            json validateEvent = { "ticket_id": ticketId, "user_id": ticket.user_id };
            byte[] serialized = validateEvent.toJsonString().toBytes();
            check kafkaProducer->send({ topic: "ticket.validated", value: serialized });
            return { "message": "Validated" };
        }
        return { "error": "Invalid ticket" };
    }
}

