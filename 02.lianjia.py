import requests
from fake_useragent import UserAgent
from lxml import etree
import redis
import time
import re
import pymysql


class LianJia:

    def __init__(self):
        self.count = 1
        self.retry = 1   # 重试次数
        self.r = self.conn_redis()
        self.conn_mysql()
        # self.get_city_area_url()  # 获取城区 存入数据库
        # self.get_business_circle_url()  # 获取商圈 存入数据库
        # self.get_divide_page_url_list()  # 获取城区对应商圈的所有分页url 存入数据库
        self.get_list_page()
        # self.lrange("divide_page_url_list")
        # self.ltrim("divide_page_url_list")

    # 获取列表页数据 然后抓取列表页数据
    def get_list_page(self):
        divide_page_url_list = self.lrange("divide_page_url_list")
        for c, divide_page_url in enumerate(divide_page_url_list):
            print("========第{}个分页url下载==========".format(c + 1), divide_page_url)
            html_xml = self.get_html_xml(divide_page_url)

            # 缩小范围
            div_xml_list = html_xml.xpath("//div[@class='content__list']/div")
            for div_xml in div_xml_list:
                fang_dict = {}
                title = div_xml.xpath(".//p[@class='content__list--item--title twoline']/a/text()")  # 标题
                title = title[0].strip()
                city_area = div_xml.xpath(".//p[@class='content__list--item--des']/a[1]/text()")[0]  # 城区
                business_circle = div_xml.xpath(".//p[@class='content__list--item--des']/a[2]/text()")[0]  # 商圈
                price = div_xml.xpath(".//span[@class='content__list--item-price']/em/text()")[0]  # 价格
                detail_url = div_xml.xpath(".//p[@class='content__list--item--title twoline']/a/@href")[0]  # 详情url
                detail_url = "https://bj.lianjia.com" + detail_url

                fang_dict["title"] = title
                fang_dict["city_area"] = city_area
                fang_dict["business_circle"] = business_circle
                fang_dict["price"] = price
                fang_dict["detail_url"] = detail_url

                self.parse_detail_html(detail_url, fang_dict)

    # 解析详情页
    def parse_detail_html(self, detail_url, fang_dict):
        # 获取详情页的xml对象
        html_xml = self.get_html_xml(detail_url)
        shelf_time = html_xml.xpath("//div[@class='content__subtitle']/text()[2]")   # 上架时间
        # 如果抓空了 则重试三次
        if shelf_time:
            shelf_time = shelf_time[0].strip()[7:]
            self.retry = 1
        else:
            if self.retry == 3:
                shelf_time = ""
                self.retry = 1
            else:
                self.retry += 1
                self.parse_detail_html(detail_url, fang_dict)
        try:
            shelf_time = int(time.mktime(time.strptime(shelf_time,"%Y-%m-%d")))
        except Exception as e:
            print(e)
            shelf_time = ""

        # 获取房源编号
        house_codes = re.findall(r"zufang/(.*?).html", detail_url)[0]

        ucid = html_xml.xpath("//span[@class='agent__im']/@data-im_id | //span[@class='contact__im']/@data-im_id")
        # ucid = ucid[0] if ucid else ''
        # print(ucid)
        if ucid:
            ucid = ucid[0]
            # 拼接房产经纪人的接口
            agent_url = "https://bj.lianjia.com/zufang/aj/house/brokers?" \
                        "house_codes={}&position=bottom&ucid={}".format(house_codes, ucid)
            # 请求接口获取经纪人电话号码
            json_data = self.get_html_xml(agent_url, data="json")
            # print(detail_url)
            agent_phone = json_data.get("data", {}).get(house_codes, {}).get(house_codes, {}).get("tp_number", '')
        else:
            agent_phone = ''

        imgs = html_xml.xpath("//ul[@class='content__article__slide__wrapper']/div/img/@src")  # 图片

        fang_dict["shelf_time"] = shelf_time
        fang_dict["house_codes"] = house_codes
        fang_dict["agent_phone"] = agent_phone
        fang_dict["imgs"] = imgs

        self.insert_mysql(fang_dict)
        print("插入成功：", self.count, fang_dict)
        self.count += 1

    def insert_mysql(self, fang_dict):
        try:
            imgs = fang_dict["imgs"]
            imgs_str = "|".join(imgs)

            sql = "insert into lianjia (city_name, house_type, title, city_area, " \
                  "business_circle, price, detail_url, shelf_time, house_codes, " \
                  "agent_phone, imgs, refresh_time) values ('{}','{}','{}','{}'," \
                  "'{}','{}','{}', {},'{}','{}','{}', {})".format("bj", 'rent',
                                  fang_dict["title"], fang_dict["city_area"],
                                  fang_dict["business_circle"], fang_dict["price"],
                                  fang_dict["detail_url"], fang_dict["shelf_time"],
                                  fang_dict["house_codes"], fang_dict["agent_phone"],
                                  imgs_str, int(time.time()))
            print(sql)
            self.cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

    def get_divide_page_url_list(self):
        # 获取商圈url对应的页面
        business_circle_url_list = self.lrange("business_circle_url_list")
        for business_circle_url in business_circle_url_list:
            html_xml = self.get_html_xml(business_circle_url)

            # 获取最大页码，并拼接所有的分页url
            max_page = html_xml.xpath("//div[@class='content__pg']/@data-totalpage")
            # 如果没有抓到最大页 说明没有数据 跳过
            if max_page:
                max_page = int(max_page[0])
            else:
                continue

            for page in range(1, max_page + 1):
                divide_page_url = business_circle_url + "pg" + str(page)
                print(self.count, divide_page_url)
                self.lpush("divide_page_url_list", divide_page_url)

                self.count += 1

        self.lrange("divide_page_url_list")
        self.llen("divide_page_url_list")

    # 清空指定列表中的数据
    def ltrim(self, key):
        self.r.ltrim(key, 1, 0)
        print("delete success")

    def lrange(self, key):
        data_list = self.r.lrange(key, 0, -1)
        data_list1 = []
        for data in data_list:
            data = data.decode("utf-8")
            data_list1.append(data)
        print(key, ":", data_list1)
        print("长度：", len(data_list1))
        return data_list1

    def llen(self, key):
        data_length = self.r.llen(key)
        print(key, "的长度:", data_length)

    def conn_redis(self):
        return redis.Redis()

    # 连接mysql数据库
    def conn_mysql(self):
        self.conn = pymysql.connect(host="localhost", port=3306,
                                    database="lianjia", user="root", password="111111")
        self.cur = self.conn.cursor()

    # 当所有代码运行完 触发此函数
    def __del__(self):
        # self.conn.commit()
        self.cur.close()
        self.conn.close()

    def lpush(self, key, value):
        self.r.lpush(key, value)

    # 获取指定的url对应的页面
    def get_html_xml(self, base_url, data=None):

        headers = {"User-Agent": UserAgent().random}
        try:
            response = requests.get(base_url, headers=headers)
            if data:
                return response.json()
            else:
                html = response.text
                return etree.HTML(html)

        except Exception as e:
            print(e)
            return self.get_html_xml(base_url)

    # 获取城市对应的城区url
    def get_city_area_url(self):
        base_url = "https://bj.lianjia.com/zufang/"
        html_xml = self.get_html_xml(base_url)
        city_area_url_list = html_xml.xpath("//ul[@data-target='area']/li[position()>1]/a/@href")
        print(city_area_url_list)

        # 拼接完整的城区url
        for city_area_url in city_area_url_list:
            full_city_area_url = "https://bj.lianjia.com" + city_area_url
            print(full_city_area_url)

            # 存入redis数据库
            self.lpush("city_area_url_list", full_city_area_url)
        self.lrange("city_area_url_list")
        self.llen("city_area_url_list")

    # 获取商圈url 并且存入redis数据库
    def get_business_circle_url(self):
        city_area_url_list = self.lrange("city_area_url_list")
        for c, city_area_url in enumerate(city_area_url_list):
            print("========================", c + 1, city_area_url)
            html_xml = self.get_html_xml(city_area_url)
            business_circle_url_list = html_xml.xpath("//li[@data-type='bizcircle'][position()>1]/a/@href")
            # print(business_circle_url_list)
            for business_circle_url in business_circle_url_list:
                full_business_circle_url = "https://bj.lianjia.com" + business_circle_url
                print(self.count, full_business_circle_url)
                self.lpush("business_circle_url_list", full_business_circle_url)

                self.count += 1

        # self.lrange("business_circle_url_list")
        # self.llen("business_circle_url_list")


if __name__ == '__main__':
    LianJia()


'''
https://bj.lianjia.com/zufang/aj/house/brokers?house_codes=BJ2111475409773674496&position=bottom&ucid=1000000020119880
https://bj.lianjia.com/zufang/aj/house/brokers?house_codes=BJ2207906645831589888&position=bottom&ucid=1000000026010117
'''
