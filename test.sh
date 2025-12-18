#!/bin/bash

BASE_URL="http://localhost:8080"
PROVIDER_URL="http://localhost:8081"

pretty_json() {
    python3 -m json.tool 2>/dev/null || cat
}

echo "Consumer: $(curl -s $BASE_URL/health)"
echo "Provider: $(curl -s $PROVIDER_URL/health)"
echo ""

echo "Generate:"
curl -s -X POST "$BASE_URL/generate" | pretty_json
echo ""

echo "Factorial 10:"
curl -s -X POST "$BASE_URL/compute" -H "Content-Type: application/json" -d '{"operation": "factorial", "value": 10}' | pretty_json
echo ""

echo "Fibonacci 30:"
curl -s -X POST "$BASE_URL/compute" -H "Content-Type: application/json" -d '{"operation": "fibonacci", "value": 30}' | pretty_json
echo ""

echo "Prime 997:"
curl -s -X POST "$BASE_URL/compute" -H "Content-Type: application/json" -d '{"operation": "prime", "value": 997}' | pretty_json
echo ""

echo "Sum 1000:"
curl -s -X POST "$BASE_URL/compute" -H "Content-Type: application/json" -d '{"operation": "sum", "value": 1000}' | pretty_json
echo ""

echo "Statistics:"
curl -s "$BASE_URL/statistics" | pretty_json
