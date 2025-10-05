import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;

// Config, models, clients same

service /admin on new http:Listener(9006) {
    // Reuse Transport endpoints or duplicate for separation

    resource function get reports/tickets() returns json|error {
        mongodb:Cursor cursor = check mongoClient->find("tickets");
        json[] tickets = [];
        check from json t in cursor
            do { tickets.push(t); };
        return { "total": tickets.length(), "data": tickets };
    }

    resource function post disruptions(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string tripId = check payload.trip_id;
        string status = check payload.status;

        json event = { "trip_id": tripId, "status": status };
        byte[] serialized = event.toJsonString().toBytes();
        check kafkaProducer->send({ topic: "schedule.updates", value: serialized });
        return { "message": "Disruption published" };
    }
}
