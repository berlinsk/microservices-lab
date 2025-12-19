// л.р. 3-4.4
import Vapor
import Foundation

let eventStoreURL = Environment.get("EVENT_STORE_URL") ?? "http://event-store:8083"
let stateStoreURL = Environment.get("STATE_STORE_URL") ?? "http://state-store-1:8086"
let materializerId = Environment.get("MATERIALIZER_ID") ?? "materializer-1"

struct Event: Content, Codable {
    let id: String
    let streamId: String
    let type: String
    let data: [String: String]
    let timestamp: String
    let version: Int
}

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8085
    
    app.get("health") { req -> String in
        return "Materializer (\(materializerId)) is healthy"
    }
    
    // л.р. 3-4.4
    //*
    app.post("materialize", ":streamId") { req async throws -> Response in
        guard let streamId = req.parameters.get("streamId") else {
            return Response(status: .badRequest)
        }
        
        let eventsUri = URI(string: "\(eventStoreURL)/streams/\(streamId)/events")
        let eventsResponse = try await req.client.get(eventsUri)
        
        guard eventsResponse.status == .ok else {
            return Response(status: .notFound, body: .init(string: "{\"error\":\"not found\"}"))
        }
        
        let events = try eventsResponse.content.decode([Event].self)
        
        var state: [String: String] = [:]
        var status = "unknown"
        
        // л.р. 3-4.1
        for event in events.sorted(by: { $0.version < $1.version }) {
            switch event.type {
            case "TaskCreated":
                state["taskId"] = event.data["taskId"] ?? streamId
                state["operation"] = event.data["operation"] ?? ""
                state["value"] = event.data["value"] ?? "0"
                status = "pending"
            case "TaskStarted":
                status = "processing"
                state["providerId"] = event.data["providerId"] ?? ""
            case "TaskCompleted":
                status = "completed"
                state["result"] = event.data["result"] ?? ""
                state["computationTimeMs"] = event.data["computationTimeMs"] ?? "0"
            default:
                break
            }
        }
        state["status"] = status
        
        // л.р. 3-4.5
        struct SaveInput: Content {
            let entityId: String
            let entityType: String
            let state: [String: String]
            let version: Int
        }
        
        let saveUri = URI(string: "\(stateStoreURL)/entities")
        _ = try await req.client.post(saveUri) { r in
            try r.content.encode(SaveInput(entityId: streamId, entityType: "Task", state: state, version: events.count))
        }
        
        return Response(status: .ok, body: .init(string: "{\"streamId\":\"\(streamId)\",\"status\":\"\(status)\",\"events\":\(events.count)}"))
    }
    //*
    
    // л.р. 3-4.4
    //*
    app.post("materialize-all") { req async throws -> Response in
        let streamsUri = URI(string: "\(eventStoreURL)/streams")
        let streamsResponse = try await req.client.get(streamsUri)
        
        guard streamsResponse.status == .ok else {
            return Response(status: .ok, body: .init(string: "{\"count\":0}"))
        }
        
        let streamIds = try streamsResponse.content.decode([String].self)
        var count = 0
        
        for streamId in streamIds {
            let eventsUri = URI(string: "\(eventStoreURL)/streams/\(streamId)/events")
            let eventsResponse = try await req.client.get(eventsUri)
            
            guard eventsResponse.status == .ok else { continue }
            
            let events = try eventsResponse.content.decode([Event].self)
            guard !events.isEmpty else { continue }
            
            var state: [String: String] = [:]
            var status = "unknown"
            
            for event in events.sorted(by: { $0.version < $1.version }) {
                switch event.type {
                case "TaskCreated":
                    state["taskId"] = event.data["taskId"] ?? streamId
                    state["operation"] = event.data["operation"] ?? ""
                    state["value"] = event.data["value"] ?? "0"
                    status = "pending"
                case "TaskStarted":
                    status = "processing"
                    state["providerId"] = event.data["providerId"] ?? ""
                case "TaskCompleted":
                    status = "completed"
                    state["result"] = event.data["result"] ?? ""
                default:
                    break
                }
            }
            state["status"] = status
            
            struct SaveInput: Content {
                let entityId: String
                let entityType: String
                let state: [String: String]
                let version: Int
            }
            
            let saveUri = URI(string: "\(stateStoreURL)/entities")
            _ = try await req.client.post(saveUri) { r in
                try r.content.encode(SaveInput(entityId: streamId, entityType: "Task", state: state, version: events.count))
            }
            count += 1
        }
        
        return Response(status: .ok, body: .init(string: "{\"materialized\":\(count)}"))
    }
    //*
    
    app.get("stats") { req -> String in
        return "{\"materializerId\":\"\(materializerId)\"}"
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)
try configure(app)
print("Materializer (\(materializerId)) started on port 8085")
try await app.execute()
