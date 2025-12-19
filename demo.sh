#!/bin/bash

case "$1" in
    "sync")
        curl -X POST http://localhost:8080/compute -H "Content-Type: application/json" -d '{"operation": "factorial", "value": 15}'
        ;;
    "async")
        curl -X POST http://localhost:8080/compute-async -H "Content-Type: application/json" -d '{"operation": "fibonacci", "value": 30}'
        ;;
    "streams")
        curl http://localhost:8083/streams
        ;;
    "stats")
        curl http://localhost:8083/stats
        ;;
    "materialize")
        curl -X POST http://localhost:8085/materialize-all
        ;;
    "entities")
        curl http://localhost:8086/entities
        ;;
    "compare")
        curl -X POST http://localhost:8080/compare -H "Content-Type: application/json" -d '{"count": 5}'
        ;;
    "statistics")
        curl http://localhost:8080/statistics
        ;;
    "health")
        curl http://localhost:8080/health
        ;;
    "health-provider")
        curl http://localhost:8081/health
        ;;
    "health-broker")
        curl http://localhost:8082/health
        ;;
    "health-eventstore")
        curl http://localhost:8083/health
        ;;
    "health-materializer")
        curl http://localhost:8085/health
        ;;
    "health-statestore")
        curl http://localhost:8086/health
        ;;
    "health-pm")
        curl http://localhost:8087/health
        ;;
    "pm-processes")
        curl http://localhost:8087/processes
        ;;
    "pm-commands")
        curl http://localhost:8087/commands
        ;;
    "pm-orchestrate")
        curl -X POST http://localhost:8087/orchestrate
        ;;
    "pm-stats")
        curl http://localhost:8087/stats
        ;;
    *)
        echo "Usage: ./demо.sh [command]"
        echo ""
        echo "Lab 1 (Sync): sync, health, health-provider"
        echo "Lab 2 (Async): async, compare, statistics, health-broker"
        echo "Lab 3-4 (ES): streams, stats, materialize, entities"
        echo "Lab 5 (PM): pm-processes, pm-commands, pm-orchestrate, pm-stats"
        ;;
esac
