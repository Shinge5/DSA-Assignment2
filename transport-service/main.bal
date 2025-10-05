import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerina/uuid;
import ballerina/time;

// Config and models same as above

// Clients same

service /transport on new http:Listener(9002) {
    resource function post routes(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string name = check payload.name;
        string[] stops = check payload.stops.fromJsonWithType(string[]);

        map<json> doc = { "name": name, "stops": stops };
        check mongoClient->insert("routes", doc);
        return { "message": "Route created", "id": doc["_id"] };
    }

    resource function post trips(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string routeId = check payload.route_id;
        string scheduleTime = check payload.schedule_time;
        string vehicleType = check payload.vehicle_type;
        int seats = check payload.seats_available;

        map<json> doc = { "route_id": routeId, "schedule_time": scheduleTime, "vehicle_type": vehicleType, "status": "on_time", "seats_available": seats };
        check mongoClient->insert("trips", doc);
        return { "message": "Trip created", "id": doc["_id"] };
    }

    resource function post updates(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        string tripId = check payload.trip_id;
        string status = check payload.status;

        map<json> filter = { "_id": tripId };
        map<json> updateDoc = { "$set": { "status": status } };
        check mongoClient->update("trips", filter, updateDoc);

        json updateEvent = { "trip_id": tripId, "status": status };
        byte[] serialized = updateEvent.toJsonString().toBytes();
        check kafkaProducer->send({ topic: "schedule.updates", value: serialized });
        return { "message": "Update published" };
    }
}

