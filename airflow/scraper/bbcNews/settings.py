BOT_NAME = 'bbcNews'

SPIDER_MODULES = ['bbcNews.spiders']
NEWSPIDER_MODULE = 'bbcNews.spiders'

ROBOTSTXT_OBEY = False

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)

DOWNLOAD_DELAY = 2.0
RANDOMIZE_DOWNLOAD_DELAY = True

# max runtime for spider
CLOSESPIDER_TIMEOUT = 60

# download timeout
DOWNLOAD_TIMEOUT = 15

# max retry times
RETRY_TIMES = 2
DNS_TIMEOUT = 10

CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1
CONCURRENT_REQUESTS_PER_IP = 1

RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 400,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    
    'bbcNews.middlewares.IgnoreDupReqMiddleware': 300,
    # 'bbcNews.middlewares.ProxyRotationMiddleware': 410,  # enable if using proxy
}

MONGO_URI = "mongodb://mongo:27017"
MONGO_DATABASE = "bbcnews"

ITEM_PIPELINES = {
    'bbcNews.pipelines.DropIfEmptyFieldPipeline': 300,
    'bbcNews.pipelines.MongoPipeline': 400,
}
