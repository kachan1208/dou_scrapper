import sys
import asyncio  
import aiohttp
import lxml.html
import aiomysql

class Dou:
    def __init__(self):
        print('CONSTRUCT')
        self._threads_count = 0
        self._max_threads_count = 5

        self._token_url = 'https://jobs.dou.ua/companies/'
        self._companies_url = 'https://jobs.dou.ua/companies/xhr-load/?'
        self._headers = self._get_headers()
        self._connector = aiohttp.TCPConnector(limit=self._max_threads_count)
        self._session = aiohttp.ClientSession(connector=self._connector, headers=self._headers)
        self._is_stop = False
        self._pool = None

        self.companies = []
        
    def _get_headers(self):
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:51.0) Gecko/20100101 Firefox/51.0'

        return {
            'User-Agent': user_agent,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://jobs.dou.ua/companies/'
        }

    async def _init_search_data(self):
        response = await self._session.get(self._token_url)
        print
        if response:
            html = lxml.html.fromstring(await response.text())
            self._token = html.xpath("//input[@name='csrfmiddlewaretoken']/@value")

    async def run(self):
        await self._create_pool()
        await self._init_search_data()

        for start_from in self._get_tasks():
            if self._is_stop:
                break

            future = asyncio.ensure_future(self.process(start_from))
            future.add_done_callback(self._done_task)
            self._threads_count += 1

            while self._threads_count >= self._max_threads_count:
                await asyncio.sleep(0.01)

    
    async def process(self, start_from):
        response = await self.load_companies(start_from)
        companies = await self.get_companies_info(response)
        await self.save_companies_data(companies)

        return {'start_from': start_from, 'companies': companies}

    def _done_task(self, task):
        result = task.result()
        if not result['companies']:
            self._is_stop = True
            print('Stop tasks creation')
            
        print('Done {}'.format(result['start_from']))
        self._threads_count -= 1

    async def get_companies_info(self, response):        
        companies = []

        if response.status != 200:
            return companies

        response = await response.json()
        response = response['html'].strip()

        if not response:
            return companies 
        
        content = lxml.html.fromstring(response)

        for html in content.xpath("//div[@class='company']"):
            company = {}
            name                   = html.xpath(".//a[@class='cn-a']/text()")
            company['name']        = self._get_value(name)
            image_url              = html.xpath(".//img[contains(@src, 's.dou.ua/CACHE/images/img/static/companies/')]/@src")
            company['image_url']   = self._get_value(image_url)
            cities                 = html.xpath(".//span[@class='city']/text()") 
            company['cities']      = self._get_list(cities)
            description            = html.xpath(".//div[@class='descr']/text()")
            company['description'] = self._get_value(description)
            company_url            = html.xpath(".//a[@class='cn-a']/@href")
            company['company_url'] = self._get_value(company_url)

            company['website_url']    = await self.get_company_site_url(company['company_url'])
            companies.append(company)

        return companies

    async def load_companies(self, start_from):
        post_data = {'csrfmiddlewaretoken': self._token, 'count': start_from}
        response = await self._session.post(self._companies_url, data=post_data)

        return response

    async def get_company_site_url(self, url):
        response = await self._session.get(url)
        content = lxml.html.fromstring(await response.text())
        site_url = content.xpath("//div[@class='site']//a/@href")

        return self._get_value(site_url)

    def _get_tasks(self):
        for i in range(0, 9999, 20):
            yield i

    def _get_value(self, val):
        return val[0].strip() if val else ''

    def _get_list(self, val):
        return val[0].strip().split(',') if val else ''

    async def save_companies_data(self, companies, table='companies'):
        async with self._pool.acquire() as connection:
            async with connection.cursor() as cursor:
                keys = ['name', 'image_url', 'description', 'website_url']
                placeholders = ",".join(['%s'] * len(keys))

                query = 'INSERT INTO {}({}) VALUES ({})'.format(table, ','.join(keys), placeholders)
                for company in companies:
                    await cursor.execute('SELECT id FROM {} WHERE name = %s'.format(table), company['name'])
                    if not await cursor.fetchone():
                        data = [company[key] for key in keys]
                        await cursor.execute(query, data)

    async def _create_pool(self):
        if not self._pool:
            self._pool = await aiomysql.create_pool(host='127.0.0.1', port=3306, user='root', password='', db='dou', charset='utf8')

if __name__ == '__main__':
    bot = Dou()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bot.run())
