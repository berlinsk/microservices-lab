// л.р. 3-4.5
import Vapor
import Foundation

struct EntityState: Content, Codable {
    let entityId: String
    let entityType: String
    let state: [String: String]
    let version: Int
    let lastUpdated: String
}

var entities: [String: EntityState] = [:]
let nodeId = Environment.get("NODE_ID") ?? "node-1"

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8086
    
    app.get("health") { req -> String in
        return "State Store (\(nodeId)) is healthy"
    }
    
    // л.р. 3-4.5
    app.post("entities") { req -> Response in
        struct SaveInput: Content {
            let entityId: String
            let entityType: String
            let state: [String: String]
            let version: Int
        }
        
        let input = try req.content.decode(SaveInput.self)
        let formatter = ISO8601DateFormatter()
        
        entities[input.entityId] = EntityState(
            entityId: input.entityId,
            entityType: input.entityType,
            state: input.state,
            version: input.version,
            lastUpdated: formatter.string(from: Date())
        )
        
        print("Saved: \(input.entityId)")
        return Response(status: .created)
    }
    
    app.get("entities", ":entityId") { req -> Response in
        guard let entityId = req.parameters.get("entityId") else {
            return Response(status: .badRequest)
        }
        
        if let entity = entities[entityId] {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(entity)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        }
        return Response(status: .notFound)
    }
    
    app.get("entities") { req -> [EntityState] in
        return Array(entities.values)
    }
    
    app.get("stats") { req -> String in
        return "{\"nodeId\":\"\(nodeId)\",\"count\":\(entities.count)}"
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)
try configure(app)
print("State Store (\(nodeId)) started on port 8086")
try await app.execute()
