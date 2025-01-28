import asyncio
from dotenv import load_dotenv
import httpx
import os
import pandas as pd
import time
import math 

load_dotenv()
token_api = os.getenv('API_TOKEN')

async def extract_tasks(dados):
    results = []
    for x in dados['tasks']:
        try:
            results.append({
                'id': x.get('id'),
                'subject': x.get('subject'),
                'type': x.get('type'),
                'hour': x.get('hour'),
                'markup': x.get('markup'),
                'done': x.get('done'),
                'user_ids': x.get('user'),
                'notes': x.get('notes'),
                'done_date': x.get('done_date'),
                'created_at': x.get('created_at'),
                'date': x.get('date'),
                'deal': x.get('deal'),
                'users': x.get('users'),
            })  
        except Exception as e:
            print(f"Erro ao processar tarefa ID: {x.get('id')} - {e}")
    return results 

async def get_tasks():
    all_tasks = []
    async with httpx.AsyncClient() as client:
        page = 1
        date_start = '2024-01-01'
        date_end = '2024-03-01'

        while True:
            url = f"https://crm.rdstation.com/api/v1/tasks?token={token_api}&page={page}&limit=200&date_start={date_start}&date_end={date_end}"
            headers = {'accept': 'application/json'}
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                dados = response.json()

            #Erros 
            except httpx.HTTPStatusError as e:
                print(f"Erro ao buscar tarefas: {e}")
                break
            except httpx.RequestError as e:
                print(f"Erro na requisição: {e}")
                break

            if dados['total'] == 0:
                print(f"Nenhuma tarefa encontrada para o intervalo {date_start} a {date_end}")
                break

            print(f"Processando página {page} de tarefas para o intervalo {date_start} a {date_end}")
            tasks = await extract_tasks(dados)
            all_tasks.extend(tasks)

            # Verificando se existem mais páginas
            if dados.get('has_more') is True:
                page += 1
            else:
                page = 1
                date_start = date_end
                date_end = pd.to_datetime(date_end) + pd.DateOffset(months=2)
                date_end = date_end.strftime('%Y-%m-%d')

        if all_tasks:
            if not os.path.exists('Data'):
                os.makedirs('Data')
            df = pd.DataFrame(all_tasks)
            df.to_excel('Data/tasks.xlsx', index=False)
            print(f"Arquivo exportado com sucesso!")
        else:
            print("Nenhuma tarefa encontrada no total.")

async def main():
    await get_tasks()

if __name__ == '__main__':
    asyncio.run(main())