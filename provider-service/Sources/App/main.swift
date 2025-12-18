import Vapor

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

struct ComputationLog: Content {
    let requestId: String
    let operation: String
    let input: Int
    let computationTimeMs: Double
    let timestamp: String
}

struct ComputationService {
    
    static func factorial(_ n: Int) -> String {
        guard n >= 0 else { return "Error: negative number" }
        guard n <= 20 else { return "Error: number too large (max 20)" }
        
        var result: UInt64 = 1
        for i in 1...max(n, 1) {
            result *= UInt64(i)
        }
        return String(result)
    }
    
    static func fibonacci(_ n: Int) -> String {
        guard n >= 0 else { return "Error: negative number" }
        guard n <= 50 else { return "Error: number too large (max 50)" }
        
        if n <= 1 { return String(n) }
        
        var a: UInt64 = 0
        var b: UInt64 = 1
        for _ in 2...n {
            let temp = a + b
            a = b
            b = temp
        }
        return String(b)
    }
    
    static func isPrime(_ n: Int) -> String {
        guard n > 1 else { return "false" }
        if n <= 3 { return "true" }
        if n % 2 == 0 || n % 3 == 0 { return "false" }
        
        var i = 5
        while i * i <= n {
            if n % i == 0 || n % (i + 2) == 0 {
                return "false"
            }
            i += 6
        }
        return "true"
    }
    
    static func sum(_ n: Int) -> String {
        guard n >= 0 else { return "Error: negative number" }
        let result = n * (n + 1) / 2
        return String(result)
    }
    
    static func compute(operation: String, value: Int) -> (result: String, timeMs: Double) {
        let startTime = DispatchTime.now()
        
        let result: String
        switch operation.lowercased() {
        case "factorial":
            result = factorial(value)
        case "fibonacci":
            result = fibonacci(value)
        case "prime":
            result = isPrime(value)
        case "sum":
            result = sum(value)
        default:
            result = "Error: unknown operation '\(operation)'. Available: factorial, fibonacci, prime, sum"
        }
        
        let endTime = DispatchTime.now()
        let nanoTime = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let timeMs = Double(nanoTime) / 1_000_000
        
        return (result, timeMs)
    }
}

actor ComputationLogger {
    static let shared = ComputationLogger()
    
    private var logs: [ComputationLog] = []
    
    func log(_ entry: ComputationLog) {
        logs.append(entry)
        print("Log: \(entry.operation)(\(entry.input)) = computed in \(String(format: "%.3f", entry.computationTimeMs)) ms")
    }
    
    func getLogs() -> [ComputationLog] {
        return logs
    }
    
    func clearLogs() {
        logs.removeAll()
    }
}

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8081
    
    try routes(app)
}

func routes(_ app: Application) throws {
    
    app.get("health") { req -> String in
        return "Provider Service is healthy"
    }
    
    app.get { req -> String in
        return """
        Provider Service
        
        Operations: factorial, fibonacci, prime, sum
        
        Endpoints:
        POST /compute
        GET /logs
        GET /health
        """
    }
    
    app.post("compute") { req async throws -> ComputationResponse in
        let requestId = UUID().uuidString
        
        let request = try req.content.decode(ComputationRequest.self)
        
        let (result, timeMs) = ComputationService.compute(
            operation: request.operation,
            value: request.value
        )
        
        let formatter = ISO8601DateFormatter()
        let timestamp = formatter.string(from: Date())
        
        let logEntry = ComputationLog(
            requestId: requestId,
            operation: request.operation,
            input: request.value,
            computationTimeMs: timeMs,
            timestamp: timestamp
        )
        await ComputationLogger.shared.log(logEntry)
        
        let response = ComputationResponse(
            operation: request.operation,
            input: request.value,
            result: result,
            computationTimeMs: timeMs,
            timestamp: timestamp
        )
        
        return response
    }
    
    app.get("logs") { req async -> [ComputationLog] in
        return await ComputationLogger.shared.getLogs()
    }
    
    app.delete("logs") { req async -> String in
        await ComputationLogger.shared.clearLogs()
        return "Logs cleared successfully"
    }
}

var env = try Environment.detect()
let app = Application(env)
defer { app.shutdown() }

try configure(app)

print("Provider Service started on port 8081")

try app.run()
