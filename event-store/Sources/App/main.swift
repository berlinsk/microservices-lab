// л.р. 3-4.2
import Vapor
import Foundation

// л.р. 3-4.1
struct Event: Content, Codable {
    let id: String
    let streamId: String
    let type: String
    let data: [String: String]
    let timestamp: String
    let version: Int
}

var streams: [String: [Event]] = [:]
var versions: [String: Int] = [:]

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8083
    
    app.get("health") { req -> String in
        return "Event Store is healthy"
    }
    
    // л.р. 3-4.2
    app.post("streams", ":streamId", "events") { req -> Response in
        guard let streamId = req.parameters.get("streamId") else {
            return Response(status: .badRequest)
        }
        
        struct EventInput: Content {
            let type: String
            let data: [String: String]
        }
        
        let input = try req.content.decode(EventInput.self)
        let newVersion = (versions[streamId] ?? 0) + 1
        let formatter = ISO8601DateFormatter()
        
        let event = Event(
            id: UUID().uuidString,
            streamId: streamId,
            type: input.type,
            data: input.data,
            timestamp: formatter.string(from: Date()),
            version: newVersion
        )
        
        if streams[streamId] == nil {
            streams[streamId] = []
        }
        streams[streamId]?.append(event)
        versions[streamId] = newVersion
        
        print("Event: \(input.type) for \(streamId)")
        
        return Response(status: .created, body: .init(string: "{\"id\":\"\(event.id)\"}"))
    }
    
    // л.р. 3-4.1
    app.get("streams", ":streamId", "events") { req -> [Event] in
        guard let streamId = req.parameters.get("streamId") else {
            return []
        }
        return streams[streamId] ?? []
    }
    
    app.get("streams") { req -> [String] in
        return Array(streams.keys)
    }
    
    app.get("stats") { req -> Response in
        var total = 0
        for events in streams.values { total += events.count }
        return Response(status: .ok, body: .init(string: "{\"totalStreams\":\(streams.count),\"totalEvents\":\(total)}"))
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)
try configure(app)
print("Event Store started on port 8083")
try await app.execute()
