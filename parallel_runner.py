import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from siem_logic_engine import ParallelLogicEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(process)d] %(message)s')
logger = logging.getLogger(__name__)

class IsolatedWorker:
    def __init__(self, kafka_url, triton_url, group_id, partitions):
        self.kafka_url = kafka_url
        self.triton_url = triton_url
        self.group_id = group_id
        self.partitions = partitions
        self.engine = ParallelLogicEngine(triton_url=self.triton_url)
        self._es_sem = asyncio.Semaphore(16)
        self._executor = ThreadPoolExecutor(max_workers=2)
        # ES Client with Auth (Phase 2 Hardening)
        self.es_client = httpx.AsyncClient(
            timeout=120.0,
            limits=httpx.Limits(max_connections=64),
            auth=("elastic", "MorpheusSOC2026!")
        )
        
        self.consumer = AIOKafkaConsumer(
            "raw_logs",
            bootstrap_servers=self.kafka_url,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1024
        )
        self.dlq_producer = None

    async def flush_to_dlq(self, batch):
        """Phase 2: Reroute failed batches to DLQ."""
        if not self.dlq_producer:
            self.dlq_producer = AIOKafkaProducer(bootstrap_servers=self.kafka_url)
            await self.dlq_producer.start()
        
        for msg in batch:
            await self.dlq_producer.send_and_wait("morpheus_failed_logs", json.dumps(msg).encode('utf-8'))
        logger.warning(f"Rerouted {len(batch)} logs to DLQ")

    async def _do_flush(self, results):
        if not results: return
        bulk_data = ""
        for res in results:
            # Phase 2: Targeted V2 optimized index
            action = {"index": {"_index": "morpheus-incidents-v2"}}
            bulk_data += json.dumps(action) + "\n" + json.dumps(res) + "\n"
        
        async with self._es_sem:
            resp = await self.es_client.post("http://elasticsearch:9200/_bulk", content=bulk_data, headers={"Content-Type": "application/x-ndjson"})
            if resp.status_code != 200:
                logger.error(f"ES Bulk Error: {resp.text}")

    async def run(self):
        await self.consumer.start()
        self.consumer.assign([asyncio.get_event_loop().create_task(self.consumer.partitions_for_topic("raw_logs"))]) 
        # Note: Actual partition assignment varies by worker id in full script
        logger.info(f"Worker started on partitions {self.partitions}")
        
        try:
            async for msg in self.consumer:
                batch_raw = [json.loads(msg.value.decode('utf-8'))] # Simplified logic for recovery
                enriched = self.engine.process_all(batch_raw)
                if enriched:
                    await self._do_flush(enriched)
                else:
                    await self.flush_to_dlq(batch_raw)
                await self.consumer.commit()
        finally:
            await self.consumer.stop()
            if self.dlq_producer: await self.dlq_producer.stop()
            await self.es_client.aclose()

if __name__ == "__main__":
    # In practice, this would use multiprocessing to fork 12 workers
    pass
