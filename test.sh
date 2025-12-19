#!/bin/bash

BASE_URL="http://localhost:8080"

pretty_json() {
    python3 -m json.tool 2>/dev/null || cat
}

echo "Health: $(curl -s $BASE_URL/health)"
echo ""

echo "Sync compute:"
curl -s -X POST "$BASE_URL/compute" -H "Content-Type: application/json" -d '{"operation": "factorial", "value": 10}' | pretty_json
echo ""

echo "Async compute:"
curl -s -X POST "$BASE_URL/compute-async" -H "Content-Type: application/json" -d '{"operation": "factorial", "value": 10}' | pretty_json
echo ""

echo "Batch sync (5):"
curl -s -X POST "$BASE_URL/batch" -H "Content-Type: application/json" -d '{"count": 5, "mode": "sync"}' | pretty_json
echo ""

echo "Batch async (5):"
curl -s -X POST "$BASE_URL/batch" -H "Content-Type: application/json" -d '{"count": 5, "mode": "async"}' | pretty_json
echo ""

echo "Statistics:"
curl -s "$BASE_URL/statistics" | pretty_json
