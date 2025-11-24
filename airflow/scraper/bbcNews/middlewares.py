from scrapy.exceptions import IgnoreRequest
from pymongo import MongoClient
import random


class IgnoreDupReqMiddleware:
    # skip request if url already exists in mongodb
    _client = None

    @staticmethod
    def get_db():
        if IgnoreDupReqMiddleware._client is None:
            IgnoreDupReqMiddleware._client = MongoClient("mongo", 27017)
        return IgnoreDupReqMiddleware._client["bbcnews"]

    def process_request(self, request, spider):
        db = self.get_db()
        if db[spider.name].find_one({"url": request.url}):
            spider.logger.info(f"⚠️ duplicate skipped: {request.url}")
            raise IgnoreRequest()


class ProxyRotationMiddleware:
    # randomly rotate proxies for each request
    def process_request(self, request, spider):
        proxy_list = spider.settings.get("PROXY_LIST", [])
        if proxy_list:
            proxy = random.choice(proxy_list)
            request.meta["proxy"] = proxy
            spider.logger.info(f"[proxy] {proxy}")
