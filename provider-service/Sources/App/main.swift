// л.р. 1.2
import Vapor
import Foundation

struct ComputationRequest: Content {
    let operation: String
    let value: Int
}

struct ComputationResponse: Content {
    let operation: String
    let input: Int
    let result: String
    let computationTimeMs: Double
    let timestamp: String
    let providerId: String
}

// л.р. 2.1
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

// л.р. 1.3
struct ComputationLog: Content {
    let requestId: String
    let operation: String
    let input: Int
    let computationTimeMs: Double
    let timestamp: String
    let mode: String
    let providerId: String
}

// л.р. 1.2
//*
struct ComputationService {
    
    static func factorial(_ n: Int) -> String {
        guard n >= 0 else { return "Error: negative number" }
        guard n <= 20 else { return "Error: number too large (max 20)" }
        var result: UInt64 = 1
        for i in 1...max(n, 1) { result *= UInt64(i) }
        return String(result)
    }
    
    static func fibonacci(_ n: Int) -> String {
        guard n >= 0 else { return "Error: negative number" }
        guard n <= 50 else { return "Error: number too large (max 50)" }
        if n <= 1 { return String(n) }
        var a: UInt64 = 0, b: UInt64 = 1
        for _ in 2...n { let temp = a + b; a = b; b = temp }
        return String(b)
    }
    
    static func isPrime(_ n: Int) -> String {
        guard n > 1 else { return "false" }
        if n <= 3 { return "true" }
        if n % 2 == 0 || n % 3 == 0 { return "false" }
        var i = 5
        while i * i <= n {
            if n % i == 0 || n % (i + 2) == 0 { return "false" }
            i += 6
        }
        return "true"
    }
    
    static func sum(_ n: Int) -> String {
        guard n >= 0 else { return "Error: negative number" }
        return String(n * (n + 1) / 2)
    }
    
    static func compute(operation: String, value: Int) -> (result: String, timeMs: Double) {
        let startTime = DispatchTime.now()
        let result: String
        switch operation.lowercased() {
        case "factorial": result = factorial(value)
        case "fibonacci": result = fibonacci(value)
        case "prime": result = isPrime(value)
        case "sum": result = sum(value)
        default: result = "Error: unknown operation"
        }
        let endTime = DispatchTime.now()
        let timeMs = Double(endTime.uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000
        return (result, timeMs)
    }
}
//*

// л.р. 1.3
//*
final class ComputationLogger: @unchecked Sendable {
    static let shared = ComputationLogger()
    private var logs: [ComputationLog] = []
    private let lock = NSLock()
    
    func log(_ entry: ComputationLog) {
        lock.lock()
        defer { lock.unlock() }
        logs.append(entry)
        print("Log [\(entry.mode)]: \(entry.operation)(\(entry.input)) in \(String(format: "%.3f", entry.computationTimeMs)) ms")
    }
    
    func getLogs() -> [ComputationLog] {
        lock.lock()
        defer { lock.unlock() }
        return logs
    }
    
    func clearLogs() {
        lock.lock()
        defer { lock.unlock() }
        logs.removeAll()
    }
}
//*

let providerId = Environment.get("PROVIDER_ID") ?? "provider-1"
let brokerURL = Environment.get("BROKER_URL") ?? "http://broker:8082"
let eventStoreURL = Environment.get("EVENT_STORE_URL") ?? "http://event-store:8083"

// л.р. 3-4.3
func publishEventAsync(client: Client, streamId: String, eventType: String, data: [String: String]) {
}

// л.р. 2.4
//*
final class BrokerWorker: @unchecked Sendable {
    private var isRunning = false
    private let client: Client
    private let providerId: String
    private let brokerURL: String
    private let lock = NSLock()
    
    init(client: Client, providerId: String, brokerURL: String) {
        self.client = client
        self.providerId = providerId
        self.brokerURL = brokerURL
    }
    
    func start() async {
        lock.lock()
        guard !isRunning else {
            lock.unlock()
            return
        }
        isRunning = true
        lock.unlock()
        
        print("Broker worker started, polling \(brokerURL)")
        
        while isRunning {
            await pollAndProcess()
            try? await Task.sleep(nanoseconds: 100_000_000)
        }
    }
    
    func stop() {
        lock.lock()
        isRunning = false
        lock.unlock()
    }
    
    private func pollAndProcess() async {
        do {
            let response = try await client.get(URI(string: "\(brokerURL)/tasks/next"))
            
            guard response.status == .ok else { return }
            
            let task = try response.content.decode(BrokerTask.self)
            print("Processing task: \(task.id) - \(task.operation)(\(task.value))")
            
            // л.р. 3-4.3
            publishEventAsync(client: client, streamId: task.id, eventType: "TaskStarted", data: [
                "taskId": task.id, "providerId": providerId
            ])
            
            // л.р. 1.3, 2.2
            let (result, timeMs) = ComputationService.compute(operation: task.operation, value: task.value)
            
            let formatter = ISO8601DateFormatter()
            let taskResult = TaskResult(
                taskId: task.id,
                operation: task.operation,
                input: task.value,
                result: result,
                computationTimeMs: timeMs,
                providerId: providerId,
                completedAt: formatter.string(from: Date())
            )
            
            let logEntry = ComputationLog(
                requestId: task.id,
                operation: task.operation,
                input: task.value,
                computationTimeMs: timeMs,
                timestamp: formatter.string(from: Date()),
                mode: "async",
                providerId: providerId
            )
            ComputationLogger.shared.log(logEntry)
            
            // л.р. 3-4.3
            publishEventAsync(client: client, streamId: task.id, eventType: "TaskCompleted", data: [
                "taskId": task.id,
                "result": result,
                "computationTimeMs": String(timeMs),
                "providerId": providerId
            ])
            
            _ = try await client.post(URI(string: "\(brokerURL)/results")) { req in
                try req.content.encode(taskResult)
            }
            
            print("Task completed: \(task.id)")
        } catch {
        }
    }
}
//*

var brokerWorker: BrokerWorker?

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8081
    
    try routes(app)
}

func routes(_ app: Application) throws {
    
    app.get("health") { req -> String in
        return "Provider Service (\(providerId)) is healthy"
    }
    
    // л.р. 1.1, 1.2
    app.post("compute") { req async throws -> ComputationResponse in
        let request = try req.content.decode(ComputationRequest.self)
        
        // л.р. 1.3
        let (result, timeMs) = ComputationService.compute(operation: request.operation, value: request.value)
        
        let formatter = ISO8601DateFormatter()
        let timestamp = formatter.string(from: Date())
        
        // л.р. 1.3
        let logEntry = ComputationLog(
            requestId: UUID().uuidString,
            operation: request.operation,
            input: request.value,
            computationTimeMs: timeMs,
            timestamp: timestamp,
            mode: "sync",
            providerId: providerId
        )
        ComputationLogger.shared.log(logEntry)
        
        return ComputationResponse(
            operation: request.operation,
            input: request.value,
            result: result,
            computationTimeMs: timeMs,
            timestamp: timestamp,
            providerId: providerId
        )
    }
    
    // л.р. 1.3
    app.get("logs") { req -> [ComputationLog] in
        return ComputationLogger.shared.getLogs()
    }
    
    app.delete("logs") { req -> String in
        ComputationLogger.shared.clearLogs()
        return "Logs cleared"
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)

try configure(app)

print("Provider Service (\(providerId)) started on port 8081")

// л.р. 2.4
Task {
    try? await Task.sleep(nanoseconds: 2_000_000_000)
    brokerWorker = BrokerWorker(client: app.client, providerId: providerId, brokerURL: brokerURL)
    await brokerWorker?.start()
}

try await app.execute()
