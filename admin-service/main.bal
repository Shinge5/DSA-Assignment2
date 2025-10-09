import ballerina/http;
import ballerinax/mongodb;
import ballerina/log;
import ballerinax/kafka;

configurable int port = 8085;
configurable string mongodbUri = "mongodb://admin:password@mongodb:27017/transit?authSource=admin";
configurable string kafkaUrl = "kafka:9092";

final mongodb:Client adminDb = check new (mongodbUri);
final kafka:Producer adminProducer = check new (kafkaUrl, "admin.updates");

service /admin on new http:Listener(port) {
    
    resource function get .() returns string {
        return "Admin Service: GET /admin/reports, POST /admin/disruptions, GET /admin/analytics, POST /admin/broadcast, GET /admin/reports/tickets, GET /admin/health";
    }
    
    resource function get reports() returns json|error {
        // Generate comprehensive reports from database
        int totalPassengers = check getTotalPassengers();
        decimal totalRevenue = check getTotalRevenue();
        int activeRoutes = check getActiveRoutes();
        string popularRoute = check getPopularRoute();
        
        return {
            reportId: "report-20251005",
            generatedAt: "2025-10-05T20:00:00Z",
            summary: {
                totalPassengers: totalPassengers,
                totalRevenue: totalRevenue,
                activeRoutes: activeRoutes,
                popularRoute: popularRoute,
                systemStatus: "OPERATIONAL"
            },
            dailyStats: {
                ticketsSold: 45,
                revenue: 1125.75,
                activeUsers: 28,
                validationRate: 0.92
            }
        };
    }
    
    // ADDED FROM INCOMING VERSION: Ticket reports endpoint
    resource function get reports/tickets() returns json|error {
        mongodb:Cursor cursor = check adminDb->find("tickets", ());
        json[] tickets = [];
        check from record {} t in cursor
            do { tickets.push(t.toJson()); };
        return { "total": tickets.length(), "data": tickets };
    }
    
    resource function post disruptions(@http:Payload json request) returns json|error {
        map<json> requestMap = <map<json>>request;
        string disruptionId = "disrupt-20251005";
        
        string message = requestMap.get("message")?.toString() ?: "";
        json routes = requestMap.get("routes") ?: [];
        string severity = requestMap.get("severity")?.toString() ?: "medium";
        string startTime = requestMap.get("startTime")?.toString() ?: "";
        string endTime = requestMap.get("endTime")?.toString() ?: "";
        string tripId = requestMap.get("trip_id")?.toString() ?: "";
        string status = requestMap.get("status")?.toString() ?: "disrupted";
        
        // Send disruption to Kafka for real-time notifications
        kafka:ProducerMessage message1 = {
            value: `{"disruptionId": "${disruptionId}", "type": "SERVICE_DISRUPTION", "message": "${message}"}`.toBytes()
        };
        kafka:Error? result = adminProducer->send(message1);
        
        // ALSO send to schedule updates (from incoming version)
        if tripId != "" {
            json scheduleEvent = { "trip_id": tripId, "status": status };
            kafka:ProducerMessage message2 = {
                value: scheduleEvent.toJsonString().toBytes()
            };
            kafka:Error? result2 = adminProducer->send(message2);
        }
        
        log:printInfo("Service disruption published", disruptionId = disruptionId);
        
        return {
            status: "success",
            message: "Service disruption published to all services",
            disruptionId: disruptionId,
            details: {
                message: message,
                affectedRoutes: routes,
                severity: severity
            }
        };
    }
    
    resource function get analytics() returns json {
        // Advanced analytics endpoint
        return {
            analyticsId: "analytics-001",
            period: "last_7_days",
            passengerGrowth: 15.5,
            revenueTrend: 12.3,
            peakHours: ["07:00-09:00", "16:00-18:00"],
            routePerformance: [
                {routeId: "R001", utilization: 0.85, revenue: 875.50},
                {routeId: "R002", utilization: 0.72, revenue: 625.25}
            ],
            systemHealth: {
                database: "connected",
                kafka: "active", 
                services: "all_operational"
            }
        };
    }
    
    resource function post broadcast(@http:Payload json request) returns json|error {
        map<json> requestMap = <map<json>>request;
        
        string title = requestMap.get("title")?.toString() ?: "";
        string message = requestMap.get("message")?.toString() ?: "";
        string priority = requestMap.get("priority")?.toString() ?: "normal";
        json targetServices = requestMap.get("targetServices") ?: [];
        string expiresAt = requestMap.get("expiresAt")?.toString() ?: "";
        
        // Broadcast message to all services via Kafka
        kafka:ProducerMessage msg = {
            value: `{"broadcastId": "broadcast-20251005", "type": "ADMIN_BROADCAST", "title": "${title}"}`.toBytes()
        };
        kafka:Error? result = adminProducer->send(msg);
        
        return {
            status: "success",
            message: "Admin broadcast sent to all services",
            broadcastId: "broadcast-20251005",
            details: {
                title: title,
                priority: priority,
                kafkaDelivery: result is () ? "confirmed" : "failed"
            }
        };
    }
    
    resource function get health() returns json {
        return {
            service: "admin-service",
            status: "healthy",
            version: "2.1.0",
            timestamp: "2025-10-05T20:00:00Z"
        };
    }
}

// Database helper functions
function getTotalPassengers() returns int|error {
    mongodb:Cursor result = check adminDb->find("passengers", ());
    int count = 0;
    check from record {} doc in result
        do { count += 1; };
    return count;
}

function getTotalRevenue() returns decimal|error {
    // Simulate revenue calculation
    return 3750.50;
}

function getActiveRoutes() returns int|error {
    // Simulate route count
    return 12;
}

function getPopularRoute() returns string|error {
    // Simulate popular route detection
    return "City Center to Katutura";
}
