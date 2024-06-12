import asyncio
import aiohttp
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup # pip install aiohttp aiodns


async def get_page(
    session: aiohttp.ClientSession,
    page: int,
    **kwargs
) -> dict:
    url = f"https://acm.timus.ru/ranklist.aspx"
    if page > 1:
        url = url + f'?from={(page - 1) * 25 + 1}'
    resp = await session.request('GET', url=url, **kwargs)
    # Note that this may raise an exception for non-2xx responses
    # You can either handle that here, or pass the exception through
    data = await resp.text()
    soup = BeautifulSoup(data, "html.parser")
    table = soup.find('table', class_="ranklist")
    nav = table.findAll('tr', class_="navigation")
    for n in nav:
        n.decompose()
    flags = table.findAll('div', class_="flags-img")
    for country in flags:
        country.replaceWith(country.attrs.get('title'))
    res = pd.read_html(table.prettify(), index_col=0)[0]#, index_col=["Rank"])[0].drop(columns=[0])#
    print(res)
    return res


async def main():
    async with aiohttp.ClientSession() as session:

        tasks = [get_page(session, page) for page in range(1, 6000)]

        d = await asyncio.gather(*tasks)
    res = pd.concat(d)
    res.to_csv("timus_ranked.csv")
    print(res)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(main())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
