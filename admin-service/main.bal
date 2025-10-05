import ballerina/http;
import ballerina/mongodb;

configurable string mongodbHost = "localhost";
configurable int mongodbPort = 27017;

service /admin on new http:Listener(9095) {
    private final mongodb:Client dbClient;

    function init() returns error? {
        self.dbClient = check new ("mongodb://" + mongodbHost + ":" + mongodbPort + "/admin_db");
    }

    resource function get salesReport() returns json|error {
        json[] sales = check self.dbClient->find({}, "sales");
        return {report: sales};
    }
}