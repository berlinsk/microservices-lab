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
}

struct TaskGenerationRequest: Content {
    let operation: String?
    let value: Int?
}

struct ConsumerResponse: Content {
    let providerResponse: ComputationResponse
    let requestTimeMs: Double
    let consumerTimestamp: String
}

struct RequestLog: Content {
    let requestId: String
    let operation: String
    let input: Int
    let requestTimeMs: Double
    let computationTimeMs: Double
    let networkOverheadMs: Double
    let timestamp: String
    let success: Bool
    let errorMessage: String?
}

actor RequestLogger {
    static let shared = RequestLogger()
    
    private var logs: [RequestLog] = []
    
    func log(_ entry: RequestLog) {
        logs.append(entry)
        
        if entry.success {
            print("Log: \(entry.operation)(\(entry.input)) - request: \(String(format: "%.3f", entry.requestTimeMs)) ms, compute: \(String(format: "%.3f", entry.computationTimeMs)) ms")
        } else {
            print("Log: \(entry.operation)(\(entry.input)) - failed: \(entry.errorMessage ?? "unknown error")")
        }
    }
    
    func getLogs() -> [RequestLog] {
        return logs
    }
    
    func clearLogs() {
        logs.removeAll()
    }
    
    func getStatistics() -> [String: Any] {
        guard !logs.isEmpty else {
            return ["message": "No logs available"]
        }
        
        let successfulLogs = logs.filter { $0.success }
        let totalRequests = logs.count
        let successfulRequests = successfulLogs.count
        let failedRequests = totalRequests - successfulRequests
        
        let avgRequestTime = successfulLogs.isEmpty ? 0 :
            successfulLogs.map { $0.requestTimeMs }.reduce(0, +) / Double(successfulLogs.count)
        let avgComputationTime = successfulLogs.isEmpty ? 0 :
            successfulLogs.map { $0.computationTimeMs }.reduce(0, +) / Double(successfulLogs.count)
        let avgNetworkOverhead = successfulLogs.isEmpty ? 0 :
            successfulLogs.map { $0.networkOverheadMs }.reduce(0, +) / Double(successfulLogs.count)
        
        return [
            "totalRequests": totalRequests,
            "successfulRequests": successfulRequests,
            "failedRequests": failedRequests,
            "averageRequestTimeMs": avgRequestTime,
            "averageComputationTimeMs": avgComputationTime,
            "averageNetworkOverheadMs": avgNetworkOverhead
        ]
    }
}

struct ProviderClient {
    let client: Client
    let providerURL: String
    
    init(client: Client) {
        self.client = client
        self.providerURL = Environment.get("PROVIDER_URL") ?? "http://localhost:8081"
    }
    
    func compute(operation: String, value: Int) async throws -> (response: ComputationResponse, requestTimeMs: Double) {
        let startTime = DispatchTime.now()
        
        let uri = URI(string: "\(providerURL)/compute")
        let request = ComputationRequest(operation: operation, value: value)
        
        let response = try await client.post(uri) { clientRequest in
            try clientRequest.content.encode(request)
        }
        
        let endTime = DispatchTime.now()
        let nanoTime = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let requestTimeMs = Double(nanoTime) / 1_000_000
        
        guard response.status == .ok else {
            throw Abort(.badRequest, reason: "Provider service returned status: \(response.status)")
        }
        
        let computationResponse = try response.content.decode(ComputationResponse.self)
        
        return (computationResponse, requestTimeMs)
    }
}

struct TaskGenerator {
    static let operations = ["factorial", "fibonacci", "prime", "sum"]
    
    static func generateTask(operation: String? = nil, value: Int? = nil) -> (operation: String, value: Int) {
        let selectedOperation = operation ?? operations.randomElement()!
        
        let selectedValue: Int
        if let value = value {
            selectedValue = value
        } else {
            switch selectedOperation {
            case "factorial":
                selectedValue = Int.random(in: 1...20)
            case "fibonacci":
                selectedValue = Int.random(in: 1...40)
            case "prime":
                selectedValue = Int.random(in: 2...10000)
            case "sum":
                selectedValue = Int.random(in: 1...1000000)
            default:
                selectedValue = Int.random(in: 1...100)
            }
        }
        
        return (selectedOperation, selectedValue)
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
        return """
        Consumer Service
        
        Endpoints:
        POST /generate
        POST /compute
        POST /batch
        GET /logs
        GET /statistics
        GET /health
        """
    }
    
    app.post("generate") { req async throws -> ConsumerResponse in
        let requestId = UUID().uuidString
        let formatter = ISO8601DateFormatter()
        let timestamp = formatter.string(from: Date())
        
        let taskRequest = try? req.content.decode(TaskGenerationRequest.self)
        let (operation, value) = TaskGenerator.generateTask(
            operation: taskRequest?.operation,
            value: taskRequest?.value
        )
        
        let client = ProviderClient(client: req.client)
        
        do {
            let (providerResponse, requestTimeMs) = try await client.compute(
                operation: operation,
                value: value
            )
            
            let networkOverhead = requestTimeMs - providerResponse.computationTimeMs
            
            let logEntry = RequestLog(
                requestId: requestId,
                operation: operation,
                input: value,
                requestTimeMs: requestTimeMs,
                computationTimeMs: providerResponse.computationTimeMs,
                networkOverheadMs: networkOverhead,
                timestamp: timestamp,
                success: true,
                errorMessage: nil
            )
            await RequestLogger.shared.log(logEntry)
            
            return ConsumerResponse(
                providerResponse: providerResponse,
                requestTimeMs: requestTimeMs,
                consumerTimestamp: timestamp
            )
            
        } catch {
            let logEntry = RequestLog(
                requestId: requestId,
                operation: operation,
                input: value,
                requestTimeMs: 0,
                computationTimeMs: 0,
                networkOverheadMs: 0,
                timestamp: timestamp,
                success: false,
                errorMessage: error.localizedDescription
            )
            await RequestLogger.shared.log(logEntry)
            
            throw Abort(.serviceUnavailable, reason: "Failed to communicate with Provider Service: \(error.localizedDescription)")
        }
    }
    
    app.post("compute") { req async throws -> ConsumerResponse in
        let requestId = UUID().uuidString
        let formatter = ISO8601DateFormatter()
        let timestamp = formatter.string(from: Date())
        
        let computeRequest = try req.content.decode(ComputationRequest.self)
        
        let client = ProviderClient(client: req.client)
        
        do {
            let (providerResponse, requestTimeMs) = try await client.compute(
                operation: computeRequest.operation,
                value: computeRequest.value
            )
            
            let networkOverhead = requestTimeMs - providerResponse.computationTimeMs
            
            let logEntry = RequestLog(
                requestId: requestId,
                operation: computeRequest.operation,
                input: computeRequest.value,
                requestTimeMs: requestTimeMs,
                computationTimeMs: providerResponse.computationTimeMs,
                networkOverheadMs: networkOverhead,
                timestamp: timestamp,
                success: true,
                errorMessage: nil
            )
            await RequestLogger.shared.log(logEntry)
            
            return ConsumerResponse(
                providerResponse: providerResponse,
                requestTimeMs: requestTimeMs,
                consumerTimestamp: timestamp
            )
            
        } catch {
            let logEntry = RequestLog(
                requestId: requestId,
                operation: computeRequest.operation,
                input: computeRequest.value,
                requestTimeMs: 0,
                computationTimeMs: 0,
                networkOverheadMs: 0,
                timestamp: timestamp,
                success: false,
                errorMessage: error.localizedDescription
            )
            await RequestLogger.shared.log(logEntry)
            
            throw Abort(.serviceUnavailable, reason: "Failed to communicate with Provider Service: \(error.localizedDescription)")
        }
    }
    
    app.get("logs") { req async -> [RequestLog] in
        return await RequestLogger.shared.getLogs()
    }
    
    app.delete("logs") { req async -> String in
        await RequestLogger.shared.clearLogs()
        return "Logs cleared successfully"
    }
    
    app.get("statistics") { req async -> Response in
        let stats = await RequestLogger.shared.getStatistics()
        
        let json = try! JSONSerialization.data(withJSONObject: stats, options: .prettyPrinted)
        
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
    
    app.post("batch") { req async throws -> Response in
        struct BatchRequest: Content {
            let count: Int
            let operation: String?
        }
        
        let batchRequest = try req.content.decode(BatchRequest.self)
        let count = min(batchRequest.count, 100)
        
        var results: [[String: Any]] = []
        let client = ProviderClient(client: req.client)
        
        for i in 1...count {
            let (operation, value) = TaskGenerator.generateTask(operation: batchRequest.operation)
            
            do {
                let (response, requestTime) = try await client.compute(operation: operation, value: value)
                results.append([
                    "index": i,
                    "operation": operation,
                    "value": value,
                    "result": response.result,
                    "requestTimeMs": requestTime,
                    "computationTimeMs": response.computationTimeMs,
                    "success": true
                ])
            } catch {
                results.append([
                    "index": i,
                    "operation": operation,
                    "value": value,
                    "success": false,
                    "error": error.localizedDescription
                ])
            }
        }
        
        let json = try! JSONSerialization.data(withJSONObject: ["results": results], options: .prettyPrinted)
        
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
}

var env = try Environment.detect()
let app = Application(env)
defer { app.shutdown() }

try configure(app)

print("Consumer Service started on port 8080")

try app.run()
