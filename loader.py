import asyncio
import aiohttp

from datetime import datetime
import pandas as pd

import re
from bs4 import BeautifulSoup

from time import sleep
from tqdm import tqdm

async def load_problems():
    async def get_problem(session, id):
        url = 'https://acm.timus.ru/problem.aspx'
        resp = await session.get(url, params={'num': id})
        html = await resp.text()

        soup = BeautifulSoup(html, features='html.parser')
        try:
            problem_links = soup.find('div', class_='problem_links')
            name = soup.find('h2', class_='problem_title').text
            tag = list(problem_links.previous_sibling.children)[1]
            difficulty = list(problem_links.children)[0]

            tag = tag.text.strip()
            difficulty = int(''.join(filter(lambda c: c.isdigit(), difficulty.text)))
        except AttributeError as e:
            return None
        
        return pd.Series({
            'id': id,
            'name': name,
            'difficulty': difficulty,
            'tag': tag
        })

    print('Loading problems...')
    async with aiohttp.ClientSession() as session:
        tasks = []
        for id in range(1000, 2174):
            tasks.append(get_problem(session, id))
        problems = await asyncio.gather(*tasks)
    problems = filter(lambda x: x is not None, problems)
    problems = pd.DataFrame(problems)
    problems.to_csv('problems.csv')
    print('Loaded %d problems successfully' % len(problems))


async def load_submissions():
    async def get_submissions(session, from_):
        url = 'https://acm.timus.ru/status.aspx'
        resp = await session.get(url, params={'from': from_, 'count': 1000})
        html = await resp.text()

        soup = BeautifulSoup(html, features='html.parser')
        table = soup.find('table', class_='status')

        submissions = []
        for submission in list(table.children)[1:-1]:
            id = int(submission.find('td', class_='id').text)

            user_id = submission.find('td', class_='coder').find('a')['href']
            user_id = int(re.search(r"author.aspx\?id=(\d+)", user_id).group(1))

            problem_id = submission.find('td', class_='problem').find('a')['href']
            problem_id = int(re.search(r"problem.aspx\?space=1&num=(\d+)", problem_id).group(1))
            
            language = submission.find('td', class_='language').text
            verdict  = submission.find('td', class_='language').next_sibling['class'][0].split('_')[1]

            time = submission.find('td', class_='date').findAll('nobr')[0].text
            date = submission.find('td', class_='date').findAll('nobr')[1].text
            dt = ' '.join((time, date)) # 01:59:53 10 Jun 2024
            dt = datetime.strptime(dt, '%H:%M:%S %d %b %Y')
            ts = int(dt.timestamp())

            submission = pd.Series({
                'id': id,
                'user_id': user_id,
                'problem_id': problem_id,
                'language': language,
                'verdict': verdict,
                'ts': ts
            })
            submissions.append(submission)
        return pd.DataFrame(submissions)

    print('Loading submissions...')
    async with aiohttp.ClientSession() as session:
        submissions = []
        try:
            for i in tqdm(range(1, 10677)):
                submissions.append(await get_submissions(session, i * 1000))
                sleep(0.05)
        except:
            pass
    submissions = pd.concat(submissions, ignore_index=True)
    submissions.drop_duplicates(inplace=True)
    submissions.sort_values('id', inplace=True)
    submissions['ts'] = pd.to_datetime(submissions['ts'], unit='s')

    submissions.to_csv('submissions.csv')
    print('Loaded %d submissions successfully' % len(submissions))


async def load_users():
    async def get_users(session, from_):
        url = 'https://acm.timus.ru/ranklist.aspx'
        resp = await session.get(url, params={'from': from_, 'count': 1000})
        html = await resp.text()

        soup = BeautifulSoup(html, features='html.parser')
        table = soup.find('table', class_='ranklist')

        users = []
        for user in list(table.findAll('tr', class_='content'))[1:-1]:
            fields = user.findAll('td')
            id = fields[2].find('a')['href']
            id = int(re.search(r"author.aspx\?id=(\d+)", id).group(1))
            rank = int(fields[0].text)
            country = list(fields[1].children)[0].attrs.get('title')
            name = fields[2].text
            rating = int(fields[3].text)
            solved = int(fields[4].text)
            last_ac = fields[5].text
            last_ac = datetime.strptime(last_ac, '%d %b %Y %H:%M')
            last_ac = int(last_ac.timestamp())

            user = pd.Series({
                'id': id, 
                'rank': rank,
                'country': country,
                'name': name,
                'solved': solved,
                'last_ac': last_ac
            })
            users.append(user)

        return pd.DataFrame(users)

    print('Loading users...')
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(1, 150000, 1000):
            tasks.append(get_users(session, i))
        users = await asyncio.gather(*tasks)

    users = pd.concat(users, ignore_index=True)
    users.drop_duplicates(inplace=True)
    users.sort_values('id', inplace=True)
    users['last_ac'] = pd.to_datetime(users['last_ac'], unit='s')

    users.to_csv('users.csv')
    print('Loaded %d users successfully' % len(users))


async def main():
    # await load_users()
    # await load_problems()
    # await load_submissions() # takes ~2 hours
    pass

if __name__ == '__main__':
    asyncio.run(main())