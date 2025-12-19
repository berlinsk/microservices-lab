// л.р. 2.1
import Vapor
import Foundation

struct BrokerTask: Content, Codable {
    let id: String
    let operation: String
    let value: Int
    let submittedAt: String
}

struct TaskResult: Content, Codable {
    let taskId: String
    let operation: String
    let input: Int
    let result: String
    let computationTimeMs: Double
    let providerId: String
    let completedAt: String
}

// л.р. 2.1, 2.4
//*
final class MessageQueue: @unchecked Sendable {
    static let shared = MessageQueue()
    
    private var pendingTasks: [BrokerTask] = []
    private var results: [String: TaskResult] = [:]
    private var processedCount = 0
    private let lock = NSLock()
    
    func submitTask(_ task: BrokerTask) {
        lock.lock()
        defer { lock.unlock() }
        pendingTasks.append(task)
        print("Task submitted: \(task.id) - \(task.operation)(\(task.value))")
    }
    
    func getNextTask() -> BrokerTask? {
        lock.lock()
        defer { lock.unlock() }
        guard !pendingTasks.isEmpty else { return nil }
        return pendingTasks.removeFirst()
    }
    
    func submitResult(_ result: TaskResult) {
        lock.lock()
        defer { lock.unlock() }
        results[result.taskId] = result
        processedCount += 1
        print("Result submitted: \(result.taskId) by \(result.providerId)")
    }
    
    func getResult(taskId: String) -> TaskResult? {
        lock.lock()
        defer { lock.unlock() }
        return results[taskId]
    }
    
    func getStats() -> [String: Any] {
        lock.lock()
        defer { lock.unlock() }
        return [
            "pendingTasks": pendingTasks.count,
            "completedTasks": results.count,
            "totalProcessed": processedCount
        ]
    }
}
//*

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8082
    
    app.get("health") { req -> String in
        return "Broker Service is healthy"
    }
    
    // л.р. 2.1
    app.post("tasks") { req -> Response in
        let task = try req.content.decode(BrokerTask.self)
        MessageQueue.shared.submitTask(task)
        return Response(status: .accepted)
    }
    
    // л.р. 2.4
    app.get("tasks", "next") { req -> Response in
        if let task = MessageQueue.shared.getNextTask() {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(task)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        }
        return Response(status: .noContent)
    }
    
    // л.р. 2.1
    app.post("results") { req -> Response in
        let result = try req.content.decode(TaskResult.self)
        MessageQueue.shared.submitResult(result)
        return Response(status: .accepted)
    }
    
    app.get("results", ":taskId") { req -> Response in
        guard let taskId = req.parameters.get("taskId") else {
            return Response(status: .badRequest)
        }
        if let result = MessageQueue.shared.getResult(taskId: taskId) {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(result)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        }
        return Response(status: .notFound)
    }
    
    app.get("stats") { req -> Response in
        let stats = MessageQueue.shared.getStats()
        let json = try! JSONSerialization.data(withJSONObject: stats, options: .prettyPrinted)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)

try configure(app)

print("Broker Service started on port 8082")

try await app.execute()
