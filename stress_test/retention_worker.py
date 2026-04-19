import asyncio
import httpx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Retention")
ES_URL = "http://elasticsearch:9200"

async def prune():
    auth = httpx.BasicAuth("elastic", "MorpheusSOC2026!")
    async with httpx.AsyncClient(auth=auth) as client:
        # Retention logic with index discovery
        resp = await client.get(f"{ES_URL}/_cat/indices?h=index,pri.store.size&bytes=b")
        if resp.status_code == 200:
            for line in resp.text.splitlines():
                if "morpheus-" in line:
                    logger.info(f"Auditing: {line}")

if __name__ == "__main__":
    asyncio.run(prune())
