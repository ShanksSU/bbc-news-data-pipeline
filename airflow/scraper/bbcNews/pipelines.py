import pymongo
import logging
from scrapy.exceptions import DropItem


class DropIfEmptyFieldPipeline:
    def process_item(self, item, spider):
        if not item.get("title") and (not item.get("text") or item["text"] == "N/A"):
            raise DropItem("Missing necessary content (title/text).")
        return item


class MongoPipeline:
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient("mongo", 27017)
        self.db = self.client[self.mongo_db]
        self.db[spider.name].create_index("url", unique=True)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        try:
            self.db[spider.name].insert_one(dict(item))
            logging.info(f"Inserted: {item.get('url')}")
        except pymongo.errors.DuplicateKeyError:
            logging.info(f"Duplicate skipped: {item.get('url')}")

        return item
