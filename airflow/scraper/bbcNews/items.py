import scrapy

class BbcnewsItem(scrapy.Item):
    # fields for bbc news article data
    url = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    subtitle = scrapy.Field()
    authors = scrapy.Field()
    text = scrapy.Field()
    topic_name = scrapy.Field()
    topic_url = scrapy.Field()
    images = scrapy.Field()
