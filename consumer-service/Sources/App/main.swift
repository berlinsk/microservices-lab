// л.р. 1.1
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

struct TaskRequest: Content {
    let operation: String?
    let value: Int?
}

struct ConsumerResponse: Content {
    let operation: String
    let input: Int
    let result: String
    let computationTimeMs: Double
    let totalTimeMs: Double
    let networkOverheadMs: Double
    let mode: String
    let providerId: String
}

// л.р. 1.4
struct RequestLog: Content {
    let requestId: String
    let operation: String
    let input: Int
    let totalTimeMs: Double
    let computationTimeMs: Double
    let networkOverheadMs: Double
    let timestamp: String
    let mode: String
    let providerId: String
    let success: Bool
}

// л.р. 1.4, 2.3
//*
final class RequestLogger: @unchecked Sendable {
    static let shared = RequestLogger()
    private var logs: [RequestLog] = []
    private let lock = NSLock()
    
    func log(_ entry: RequestLog) {
        lock.lock()
        defer { lock.unlock() }
        logs.append(entry)
        print("Log [\(entry.mode)]: \(entry.operation)(\(entry.input)) - total: \(String(format: "%.3f", entry.totalTimeMs)) ms, compute: \(String(format: "%.3f", entry.computationTimeMs)) ms, by \(entry.providerId)")
    }
    
    func getLogs() -> [RequestLog] {
        lock.lock()
        defer { lock.unlock() }
        return logs
    }
    
    func clearLogs() {
        lock.lock()
        defer { lock.unlock() }
        logs.removeAll()
    }
    
    // л.р. 2.5
    func getStatistics() -> [String: Any] {
        lock.lock()
        defer { lock.unlock() }
        
        guard !logs.isEmpty else { return ["message": "No logs available"] }
        
        let syncLogs = logs.filter { $0.mode == "sync" && $0.success }
        let asyncLogs = logs.filter { $0.mode == "async" && $0.success }
        
        func avg(_ values: [Double]) -> Double {
            values.isEmpty ? 0 : values.reduce(0, +) / Double(values.count)
        }
        
        var providerStats: [String: Int] = [:]
        for log in logs.filter({ $0.success }) {
            providerStats[log.providerId, default: 0] += 1
        }
        
        return [
            "totalRequests": logs.count,
            "syncRequests": syncLogs.count,
            "asyncRequests": asyncLogs.count,
            "syncAvgTotalTimeMs": avg(syncLogs.map { $0.totalTimeMs }),
            "syncAvgComputeTimeMs": avg(syncLogs.map { $0.computationTimeMs }),
            "syncAvgNetworkOverheadMs": avg(syncLogs.map { $0.networkOverheadMs }),
            "asyncAvgTotalTimeMs": avg(asyncLogs.map { $0.totalTimeMs }),
            "asyncAvgComputeTimeMs": avg(asyncLogs.map { $0.computationTimeMs }),
            "asyncAvgNetworkOverheadMs": avg(asyncLogs.map { $0.networkOverheadMs }),
            "tasksByProvider": providerStats
        ]
    }
}
//*

let providerURL = Environment.get("PROVIDER_URL") ?? "http://localhost:8081"
let brokerURL = Environment.get("BROKER_URL") ?? "http://broker:8082"
let eventStoreURL = Environment.get("EVENT_STORE_URL") ?? "http://event-store:8083"

// л.р. 3-4.3
func publishEventAsync(client: Client, streamId: String, eventType: String, data: [String: String]) {
}

// л.р. 1.1
//*
struct ProviderClient {
    let client: Client
    
    func compute(operation: String, value: Int) async throws -> (response: ComputationResponse, requestTimeMs: Double) {
        let startTime = DispatchTime.now()
        let uri = URI(string: "\(providerURL)/compute")
        let request = ComputationRequest(operation: operation, value: value)
        
        let response = try await client.post(uri) { req in
            try req.content.encode(request)
        }
        
        let endTime = DispatchTime.now()
        let requestTimeMs = Double(endTime.uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000
        
        guard response.status == .ok else {
            throw Abort(.badRequest, reason: "Provider returned: \(response.status)")
        }
        
        let computationResponse = try response.content.decode(ComputationResponse.self)
        return (computationResponse, requestTimeMs)
    }
}
//*

// л.р. 2.1
//*
struct BrokerClient {
    let client: Client
    
    func submitTask(operation: String, value: Int) async throws -> String {
        let formatter = ISO8601DateFormatter()
        let taskId = UUID().uuidString
        let task = BrokerTask(
            id: taskId,
            operation: operation,
            value: value,
            submittedAt: formatter.string(from: Date())
        )
        
        let uri = URI(string: "\(brokerURL)/tasks")
        let response = try await client.post(uri) { req in
            try req.content.encode(task)
        }
        
        guard response.status == .accepted else {
            throw Abort(.badRequest, reason: "Broker returned: \(response.status)")
        }
        
        return taskId
    }
    
    func getResult(taskId: String, timeout: Double = 30.0) async throws -> (result: TaskResult, totalTimeMs: Double) {
        let startTime = DispatchTime.now()
        let timeoutNs = UInt64(timeout * 1_000_000_000)
        
        while true {
            let elapsed = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            if elapsed > timeoutNs {
                throw Abort(.requestTimeout, reason: "Task timeout")
            }
            
            let uri = URI(string: "\(brokerURL)/results/\(taskId)")
            let response = try await client.get(uri)
            
            if response.status == .ok {
                let result = try response.content.decode(TaskResult.self)
                let endTime = DispatchTime.now()
                let totalTimeMs = Double(endTime.uptimeNanoseconds - startTime.uptimeNanoseconds) / 1_000_000
                return (result, totalTimeMs)
            }
            
            try await Task.sleep(nanoseconds: 50_000_000)
        }
    }
}
//*

// л.р. 1.1
struct TaskGenerator {
    static let operations = ["factorial", "fibonacci", "prime", "sum"]
    
    static func generateTask(operation: String? = nil, value: Int? = nil) -> (operation: String, value: Int) {
        let op = operation ?? operations.randomElement()!
        let val: Int
        if let v = value { val = v }
        else {
            switch op {
            case "factorial": val = Int.random(in: 1...20)
            case "fibonacci": val = Int.random(in: 1...40)
            case "prime": val = Int.random(in: 2...10000)
            case "sum": val = Int.random(in: 1...1000000)
            default: val = Int.random(in: 1...100)
            }
        }
        return (op, val)
    }
}

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8080
    
    try routes(app)
}

func routes(_ app: Application) throws {
    
    app.get("health") { req -> String in
        return "Consumer Service is healthy"
    }
    
    app.get { req -> String in
        return "Consumer Service - POST /compute (sync), POST /compute-async (async), POST /batch, POST /compare, GET /statistics"
    }
    
    // л.р. 1.1
    app.post("compute") { req async throws -> ConsumerResponse in
        let taskRequest = try req.content.decode(TaskRequest.self)
        let operation = taskRequest.operation ?? "factorial"
        let value = taskRequest.value ?? 10
        
        let client = ProviderClient(client: req.client)
        // л.р. 1.4
        let (response, requestTimeMs) = try await client.compute(operation: operation, value: value)
        let networkOverhead = requestTimeMs - response.computationTimeMs
        
        let formatter = ISO8601DateFormatter()
        // л.р. 1.4
        let logEntry = RequestLog(
            requestId: UUID().uuidString,
            operation: operation,
            input: value,
            totalTimeMs: requestTimeMs,
            computationTimeMs: response.computationTimeMs,
            networkOverheadMs: networkOverhead,
            timestamp: formatter.string(from: Date()),
            mode: "sync",
            providerId: response.providerId,
            success: true
        )
        RequestLogger.shared.log(logEntry)
        
        return ConsumerResponse(
            operation: response.operation,
            input: response.input,
            result: response.result,
            computationTimeMs: response.computationTimeMs,
            totalTimeMs: requestTimeMs,
            networkOverheadMs: networkOverhead,
            mode: "sync",
            providerId: response.providerId
        )
    }
    
    // л.р. 2.1
    app.post("compute-async") { req async throws -> ConsumerResponse in
        let taskRequest = try req.content.decode(TaskRequest.self)
        let operation = taskRequest.operation ?? "factorial"
        let value = taskRequest.value ?? 10
        
        let client = BrokerClient(client: req.client)
        let startTime = DispatchTime.now()
        
        let taskId = try await client.submitTask(operation: operation, value: value)
        
        // л.р. 3-4.3
        publishEventAsync(client: req.client, streamId: taskId, eventType: "TaskCreated", data: [
            "taskId": taskId,
            "operation": operation,
            "value": String(value)
        ])
        
        // л.р. 2.3
        let (result, totalTimeMs) = try await client.getResult(taskId: taskId)
        
        let networkOverhead = totalTimeMs - result.computationTimeMs
        
        let formatter = ISO8601DateFormatter()
        // л.р. 2.3
        let logEntry = RequestLog(
            requestId: taskId,
            operation: operation,
            input: value,
            totalTimeMs: totalTimeMs,
            computationTimeMs: result.computationTimeMs,
            networkOverheadMs: networkOverhead,
            timestamp: formatter.string(from: Date()),
            mode: "async",
            providerId: result.providerId,
            success: true
        )
        RequestLogger.shared.log(logEntry)
        
        return ConsumerResponse(
            operation: result.operation,
            input: result.input,
            result: result.result,
            computationTimeMs: result.computationTimeMs,
            totalTimeMs: totalTimeMs,
            networkOverheadMs: networkOverhead,
            mode: "async",
            providerId: result.providerId
        )
    }
    
    // л.р. 1.1
    app.post("generate") { req async throws -> ConsumerResponse in
        let taskRequest = try? req.content.decode(TaskRequest.self)
        let (operation, value) = TaskGenerator.generateTask(operation: taskRequest?.operation, value: taskRequest?.value)
        
        let client = ProviderClient(client: req.client)
        let (response, requestTimeMs) = try await client.compute(operation: operation, value: value)
        let networkOverhead = requestTimeMs - response.computationTimeMs
        
        let formatter = ISO8601DateFormatter()
        let logEntry = RequestLog(
            requestId: UUID().uuidString,
            operation: operation,
            input: value,
            totalTimeMs: requestTimeMs,
            computationTimeMs: response.computationTimeMs,
            networkOverheadMs: networkOverhead,
            timestamp: formatter.string(from: Date()),
            mode: "sync",
            providerId: response.providerId,
            success: true
        )
        RequestLogger.shared.log(logEntry)
        
        return ConsumerResponse(
            operation: response.operation,
            input: response.input,
            result: response.result,
            computationTimeMs: response.computationTimeMs,
            totalTimeMs: requestTimeMs,
            networkOverheadMs: networkOverhead,
            mode: "sync",
            providerId: response.providerId
        )
    }
    
    // л.р. 2.4
    //*
    app.post("batch") { req async throws -> Response in
        struct BatchRequest: Content {
            let count: Int
            let operation: String?
            let mode: String?
        }
        
        let batchRequest = try req.content.decode(BatchRequest.self)
        let count = min(batchRequest.count, 100)
        let mode = batchRequest.mode ?? "sync"
        
        var results: [[String: Any]] = []
        let formatter = ISO8601DateFormatter()
        
        if mode == "async" {
            let brokerClient = BrokerClient(client: req.client)
            var taskIds: [(String, String, Int)] = []
            
            for _ in 1...count {
                let (operation, value) = TaskGenerator.generateTask(operation: batchRequest.operation)
                let taskId = try await brokerClient.submitTask(operation: operation, value: value)
                taskIds.append((taskId, operation, value))
                
                // л.р. 3-4.3
                publishEventAsync(client: req.client, streamId: taskId, eventType: "TaskCreated", data: [
                    "taskId": taskId, "operation": operation, "value": String(value)
                ])
            }
            
            for (index, (taskId, operation, value)) in taskIds.enumerated() {
                do {
                    let (result, totalTimeMs) = try await brokerClient.getResult(taskId: taskId)
                    let networkOverhead = totalTimeMs - result.computationTimeMs
                    
                    let logEntry = RequestLog(
                        requestId: taskId,
                        operation: operation,
                        input: value,
                        totalTimeMs: totalTimeMs,
                        computationTimeMs: result.computationTimeMs,
                        networkOverheadMs: networkOverhead,
                        timestamp: formatter.string(from: Date()),
                        mode: "async",
                        providerId: result.providerId,
                        success: true
                    )
                    RequestLogger.shared.log(logEntry)
                    
                    results.append([
                        "index": index + 1,
                        "operation": operation,
                        "value": value,
                        "result": result.result,
                        "totalTimeMs": totalTimeMs,
                        "computationTimeMs": result.computationTimeMs,
                        "networkOverheadMs": networkOverhead,
                        "providerId": result.providerId,
                        "mode": "async",
                        "success": true
                    ])
                } catch {
                    results.append([
                        "index": index + 1,
                        "operation": operation,
                        "value": value,
                        "mode": "async",
                        "success": false,
                        "error": error.localizedDescription
                    ])
                }
            }
        } else {
            let providerClient = ProviderClient(client: req.client)
            
            for i in 1...count {
                let (operation, value) = TaskGenerator.generateTask(operation: batchRequest.operation)
                
                do {
                    let (response, requestTimeMs) = try await providerClient.compute(operation: operation, value: value)
                    let networkOverhead = requestTimeMs - response.computationTimeMs
                    
                    let logEntry = RequestLog(
                        requestId: UUID().uuidString,
                        operation: operation,
                        input: value,
                        totalTimeMs: requestTimeMs,
                        computationTimeMs: response.computationTimeMs,
                        networkOverheadMs: networkOverhead,
                        timestamp: formatter.string(from: Date()),
                        mode: "sync",
                        providerId: response.providerId,
                        success: true
                    )
                    RequestLogger.shared.log(logEntry)
                    
                    results.append([
                        "index": i,
                        "operation": operation,
                        "value": value,
                        "result": response.result,
                        "totalTimeMs": requestTimeMs,
                        "computationTimeMs": response.computationTimeMs,
                        "networkOverheadMs": networkOverhead,
                        "providerId": response.providerId,
                        "mode": "sync",
                        "success": true
                    ])
                } catch {
                    results.append([
                        "index": i,
                        "operation": operation,
                        "value": value,
                        "mode": "sync",
                        "success": false,
                        "error": error.localizedDescription
                    ])
                }
            }
        }
        
        let json = try! JSONSerialization.data(withJSONObject: ["results": results, "mode": mode], options: .prettyPrinted)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
    //*
    
    // л.р. 2.5
    //*
    app.post("compare") { req async throws -> Response in
        struct CompareRequest: Content {
            let count: Int
            let operation: String?
        }
        
        let compareRequest = try req.content.decode(CompareRequest.self)
        let count = min(compareRequest.count, 50)
        
        var syncResults: [[String: Any]] = []
        var asyncResults: [[String: Any]] = []
        let providerClient = ProviderClient(client: req.client)
        let brokerClient = BrokerClient(client: req.client)
        let formatter = ISO8601DateFormatter()
        
        var syncTotalTime: Double = 0
        var asyncTotalTime: Double = 0
        
        for i in 1...count {
            let (operation, value) = TaskGenerator.generateTask(operation: compareRequest.operation)
            
            do {
                let (response, requestTimeMs) = try await providerClient.compute(operation: operation, value: value)
                syncTotalTime += requestTimeMs
                
                let logEntry = RequestLog(
                    requestId: UUID().uuidString,
                    operation: operation,
                    input: value,
                    totalTimeMs: requestTimeMs,
                    computationTimeMs: response.computationTimeMs,
                    networkOverheadMs: requestTimeMs - response.computationTimeMs,
                    timestamp: formatter.string(from: Date()),
                    mode: "sync",
                    providerId: response.providerId,
                    success: true
                )
                RequestLogger.shared.log(logEntry)
                
                syncResults.append([
                    "index": i,
                    "operation": operation,
                    "value": value,
                    "totalTimeMs": requestTimeMs,
                    "providerId": response.providerId
                ])
            } catch {
                syncResults.append(["index": i, "error": error.localizedDescription])
            }
        }
        
        for i in 1...count {
            let (operation, value) = TaskGenerator.generateTask(operation: compareRequest.operation)
            
            do {
                let taskId = try await brokerClient.submitTask(operation: operation, value: value)
                
                // л.р. 3-4.3
                publishEventAsync(client: req.client, streamId: taskId, eventType: "TaskCreated", data: [
                    "taskId": taskId, "operation": operation, "value": String(value)
                ])
                
                let (result, totalTimeMs) = try await brokerClient.getResult(taskId: taskId)
                asyncTotalTime += totalTimeMs
                
                let logEntry = RequestLog(
                    requestId: taskId,
                    operation: operation,
                    input: value,
                    totalTimeMs: totalTimeMs,
                    computationTimeMs: result.computationTimeMs,
                    networkOverheadMs: totalTimeMs - result.computationTimeMs,
                    timestamp: formatter.string(from: Date()),
                    mode: "async",
                    providerId: result.providerId,
                    success: true
                )
                RequestLogger.shared.log(logEntry)
                
                asyncResults.append([
                    "index": i,
                    "operation": operation,
                    "value": value,
                    "totalTimeMs": totalTimeMs,
                    "providerId": result.providerId
                ])
            } catch {
                asyncResults.append(["index": i, "error": error.localizedDescription])
            }
        }
        
        let comparison: [String: Any] = [
            "syncRequests": count,
            "asyncRequests": count,
            "syncTotalTimeMs": syncTotalTime,
            "asyncTotalTimeMs": asyncTotalTime,
            "syncAvgTimeMs": syncTotalTime / Double(count),
            "asyncAvgTimeMs": asyncTotalTime / Double(count),
            "syncResults": syncResults,
            "asyncResults": asyncResults
        ]
        
        let json = try! JSONSerialization.data(withJSONObject: comparison, options: .prettyPrinted)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
    //*
    
    // л.р. 1.4, 2.3
    app.get("logs") { req -> [RequestLog] in
        return RequestLogger.shared.getLogs()
    }
    
    app.delete("logs") { req -> String in
        RequestLogger.shared.clearLogs()
        return "Logs cleared"
    }
    
    // л.р. 2.5
    app.get("statistics") { req -> Response in
        let stats = RequestLogger.shared.getStatistics()
        let json = try! JSONSerialization.data(withJSONObject: stats, options: .prettyPrinted)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)

try configure(app)

print("Consumer Service started on port 8080")

try await app.execute()
