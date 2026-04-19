#!/bin/bash
echo "🛡️  Restoring Secure 8-Shard Index Baseline..."
curl -X DELETE -u elastic:MorpheusSOC2026! http://localhost:9200/morpheus-incidents-v2 || true
curl -X PUT -u elastic:MorpheusSOC2026! http://localhost:9200/morpheus-incidents-v2      -H 'Content-Type: application/json'      -d '{ "settings": { "index.number_of_shards": 8, "index.number_of_replicas": 0 } }'
echo "✅ Baseline Restored."
