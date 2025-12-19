import Vapor
import Foundation

struct Task: Content, Codable {
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

actor MessageQueue {
    static let shared = MessageQueue()
    
    private var pendingTasks: [Task] = []
    private var results: [String: TaskResult] = [:]
    private var stats = BrokerStats()
    
    struct BrokerStats {
        var totalTasksSubmitted: Int = 0
        var totalTasksCompleted: Int = 0
        var tasksByProvider: [String: Int] = [:]
    }
    
    func submitTask(_ task: Task) {
        pendingTasks.append(task)
        stats.totalTasksSubmitted += 1
        print("Task submitted: \(task.id) - \(task.operation)(\(task.value))")
    }
    
    func getNextTask() -> Task? {
        guard !pendingTasks.isEmpty else { return nil }
        let task = pendingTasks.removeFirst()
        print("Task dispatched: \(task.id)")
        return task
    }
    
    func submitResult(_ result: TaskResult) {
        results[result.taskId] = result
        stats.totalTasksCompleted += 1
        stats.tasksByProvider[result.providerId, default: 0] += 1
        print("Result received: \(result.taskId) from \(result.providerId)")
    }
    
    func getResult(taskId: String) -> TaskResult? {
        return results.removeValue(forKey: taskId)
    }
    
    func peekResult(taskId: String) -> TaskResult? {
        return results[taskId]
    }
    
    func getStats() -> [String: Any] {
        return [
            "totalTasksSubmitted": stats.totalTasksSubmitted,
            "totalTasksCompleted": stats.totalTasksCompleted,
            "pendingTasks": pendingTasks.count,
            "pendingResults": results.count,
            "tasksByProvider": stats.tasksByProvider
        ]
    }
    
    func getPendingCount() -> Int {
        return pendingTasks.count
    }
}

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8082
    
    try routes(app)
}

func routes(_ app: Application) throws {
    
    app.get("health") { req -> String in
        return "Broker Service is healthy"
    }
    
    app.post("tasks") { req async throws -> Response in
        let task = try req.content.decode(Task.self)
        await MessageQueue.shared.submitTask(task)
        return Response(status: .accepted, body: .init(string: "{\"status\":\"accepted\",\"taskId\":\"\(task.id)\"}"))
    }
    
    app.get("tasks", "next") { req async -> Response in
        if let task = await MessageQueue.shared.getNextTask() {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(task)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        } else {
            return Response(status: .noContent)
        }
    }
    
    app.post("results") { req async throws -> Response in
        let result = try req.content.decode(TaskResult.self)
        await MessageQueue.shared.submitResult(result)
        return Response(status: .accepted, body: .init(string: "{\"status\":\"accepted\"}"))
    }
    
    app.get("results", ":taskId") { req async -> Response in
        guard let taskId = req.parameters.get("taskId") else {
            return Response(status: .badRequest)
        }
        if let result = await MessageQueue.shared.getResult(taskId: taskId) {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(result)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        } else {
            return Response(status: .notFound)
        }
    }
    
    app.get("results", ":taskId", "peek") { req async -> Response in
        guard let taskId = req.parameters.get("taskId") else {
            return Response(status: .badRequest)
        }
        if let result = await MessageQueue.shared.peekResult(taskId: taskId) {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(result)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        } else {
            return Response(status: .notFound)
        }
    }
    
    app.get("stats") { req async -> Response in
        let stats = await MessageQueue.shared.getStats()
        let json = try! JSONSerialization.data(withJSONObject: stats, options: .prettyPrinted)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
    
    app.get("pending") { req async -> String in
        let count = await MessageQueue.shared.getPendingCount()
        return "\(count)"
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)

try configure(app)

print("Broker Service started on port 8082")

try await app.execute()

