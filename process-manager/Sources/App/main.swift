// л.р. 5.1
import Vapor
import Foundation

let eventStoreURL = Environment.get("EVENT_STORE_URL") ?? "http://event-store:8083"
let brokerURL = Environment.get("BROKER_URL") ?? "http://broker:8082"
let processManagerId = Environment.get("PROCESS_MANAGER_ID") ?? "pm-1"

struct Event: Content, Codable {
    let id: String
    let streamId: String
    let type: String
    let data: [String: String]
    let timestamp: String
    let version: Int
}

// л.р. 5.2
struct Command: Content, Codable {
    let id: String
    let type: String
    let targetService: String
    let payload: [String: String]
    let timestamp: String
    let sourceEvent: String
}

// л.р. 5.1
struct ProcessState: Content, Codable {
    let processId: String
    let taskId: String
    let status: String
    let currentStep: String
    let commands: [Command]
    let startedAt: String
    let updatedAt: String
}

var processes: [String: ProcessState] = [:]
var commandLog: [Command] = []
var processedEvents: Set<String> = []

// л.р. 5.2
//*
func generateCommand(event: Event) -> Command? {
    let formatter = ISO8601DateFormatter()
    let timestamp = formatter.string(from: Date())
    
    switch event.type {
    case "TaskCreated":
        return Command(
            id: UUID().uuidString,
            type: "StartProcessing",
            targetService: "provider",
            payload: [
                "taskId": event.data["taskId"] ?? event.streamId,
                "operation": event.data["operation"] ?? "",
                "value": event.data["value"] ?? "0",
                "priority": "normal"
            ],
            timestamp: timestamp,
            sourceEvent: event.id
        )
    case "TaskStarted":
        return Command(
            id: UUID().uuidString,
            type: "MonitorProgress",
            targetService: "monitor",
            payload: [
                "taskId": event.data["taskId"] ?? event.streamId,
                "providerId": event.data["providerId"] ?? "",
                "timeout": "30000"
            ],
            timestamp: timestamp,
            sourceEvent: event.id
        )
    case "TaskCompleted":
        return Command(
            id: UUID().uuidString,
            type: "NotifyCompletion",
            targetService: "notification",
            payload: [
                "taskId": event.data["taskId"] ?? event.streamId,
                "result": event.data["result"] ?? "",
                "providerId": event.data["providerId"] ?? ""
            ],
            timestamp: timestamp,
            sourceEvent: event.id
        )
    default:
        return nil
    }
}
//*

// л.р. 5.1
//*
func updateProcessState(event: Event) {
    let formatter = ISO8601DateFormatter()
    let timestamp = formatter.string(from: Date())
    let taskId = event.data["taskId"] ?? event.streamId
    
    var process = processes[taskId] ?? ProcessState(
        processId: UUID().uuidString,
        taskId: taskId,
        status: "initialized",
        currentStep: "none",
        commands: [],
        startedAt: timestamp,
        updatedAt: timestamp
    )
    
    var status = process.status
    var currentStep = process.currentStep
    var commands = process.commands
    
    if let command = generateCommand(event: event) {
        commands.append(command)
        commandLog.append(command)
        print("Command generated: \(command.type) for task \(taskId)")
    }
    
    switch event.type {
    case "TaskCreated":
        status = "pending"
        currentStep = "awaiting_processing"
    case "TaskStarted":
        status = "processing"
        currentStep = "computation_in_progress"
    case "TaskCompleted":
        status = "completed"
        currentStep = "finished"
    default:
        break
    }
    
    processes[taskId] = ProcessState(
        processId: process.processId,
        taskId: taskId,
        status: status,
        currentStep: currentStep,
        commands: commands,
        startedAt: process.startedAt,
        updatedAt: timestamp
    )
}
//*

func configure(_ app: Application) throws {
    app.http.server.configuration.hostname = "0.0.0.0"
    app.http.server.configuration.port = 8087
    
    app.get("health") { req -> String in
        return "Process Manager (\(processManagerId)) is healthy"
    }
    
    // л.р. 5.1
    app.get("processes") { req -> [ProcessState] in
        return Array(processes.values)
    }
    
    app.get("processes", ":taskId") { req -> Response in
        guard let taskId = req.parameters.get("taskId") else {
            return Response(status: .badRequest)
        }
        if let process = processes[taskId] {
            let encoder = JSONEncoder()
            let data = try! encoder.encode(process)
            var headers = HTTPHeaders()
            headers.add(name: .contentType, value: "application/json")
            return Response(status: .ok, headers: headers, body: .init(data: data))
        }
        return Response(status: .notFound)
    }
    
    // л.р. 5.2
    app.get("commands") { req -> [Command] in
        return commandLog
    }
    
    app.get("commands", "pending") { req -> Response in
        let pending = commandLog.filter { cmd in
            if let process = processes.values.first(where: { $0.commands.contains(where: { $0.id == cmd.id }) }) {
                return process.status != "completed"
            }
            return false
        }
        let encoder = JSONEncoder()
        let data = try! encoder.encode(pending)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: data))
    }
    
    // л.р. 5.1
    app.post("process-event") { req -> Response in
        let event = try req.content.decode(Event.self)
        
        if processedEvents.contains(event.id) {
            return Response(status: .ok, body: .init(string: "{\"status\":\"already_processed\"}"))
        }
        
        processedEvents.insert(event.id)
        updateProcessState(event: event)
        
        return Response(status: .ok, body: .init(string: "{\"status\":\"processed\"}"))
    }
    
    // л.р. 5.2
    //*
    app.post("orchestrate") { req async throws -> Response in
        let streamsUri = URI(string: "\(eventStoreURL)/streams")
        let streamsResponse = try await req.client.get(streamsUri)
        
        guard streamsResponse.status == .ok else {
            return Response(status: .ok, body: .init(string: "{\"processed\":0,\"commands\":0}"))
        }
        
        let streamIds = try streamsResponse.content.decode([String].self)
        var processedCount = 0
        var commandsCount = 0
        
        for streamId in streamIds {
            let eventsUri = URI(string: "\(eventStoreURL)/streams/\(streamId)/events")
            let eventsResponse = try await req.client.get(eventsUri)
            
            guard eventsResponse.status == .ok else { continue }
            
            let events = try eventsResponse.content.decode([Event].self)
            
            for event in events.sorted(by: { $0.version < $1.version }) {
                if !processedEvents.contains(event.id) {
                    processedEvents.insert(event.id)
                    let commandsBefore = commandLog.count
                    updateProcessState(event: event)
                    commandsCount += commandLog.count - commandsBefore
                    processedCount += 1
                }
            }
        }
        
        return Response(status: .ok, body: .init(string: "{\"processed\":\(processedCount),\"commands\":\(commandsCount)}"))
    }
    //*
    
    // л.р. 5.1
    app.get("stats") { req -> Response in
        let stats: [String: Any] = [
            "processManagerId": processManagerId,
            "totalProcesses": processes.count,
            "totalCommands": commandLog.count,
            "processedEvents": processedEvents.count,
            "processesByStatus": [
                "pending": processes.values.filter { $0.status == "pending" }.count,
                "processing": processes.values.filter { $0.status == "processing" }.count,
                "completed": processes.values.filter { $0.status == "completed" }.count
            ]
        ]
        let json = try! JSONSerialization.data(withJSONObject: stats, options: .prettyPrinted)
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(data: json))
    }
}

var env = try Environment.detect()
let app = try await Application.make(env)
try configure(app)
print("Process Manager (\(processManagerId)) started on port 8087")
try await app.execute()
