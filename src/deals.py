import asyncio
from dotenv import load_dotenv
import httpx
import os
import pandas as pd
import time

load_dotenv()
token_api = os.getenv('API_TOKEN')
list_pipeline_id = {'Vendas': '65a529bba45153001041db69', 'Comparecimento': '65a19c4aeaa5d40011c85939', 'Pre': '6583259feec615001f1fd36f', 'Pos': '65a52caa08f3d90017ecf0c5'}

async def extract_deals(dados):
    results = []
    for x in dados['deals']:
        try:
            print(f"Processando negociação ID: {x.get('id')} - Nome: {x.get('name')}")
            
            deal_info = {
                'id_negociação': x.get('id'),
                'name_negociação': x.get('name'),
                'amount_montly': x.get('amount_montly', 0),
                'amount_unique': x.get('amount_unique', 0),
                'amount_total': x.get('amount_total', 0),
                'prediction_date': x.get('prediction_date'),
                'markup': x.get('markup'),
                'last_activity_at': x.get('last_activity_at'),
                'interactions': x.get('interactions', 0),
                'markup_last_activities': x.get('markup_last_activities'),
                'created_at': x.get('created_at'),
                'updated_at': x.get('updated_at'),
                'rating': x.get('rating'),
                'markup_created': x.get('markup_created'),
                'last_activity_content': x.get('last_activity_content'),
                'user_changed': x.get('user_changed'),
                'hold': x.get('hold'),
                'win': x.get('win'),
                'closed_at': x.get('closed_at')
            }

            # Atualizando o status da negociação
            win = x.get('win')
            if win is True:
                win = 'Vendida'
            elif win is False:
                win = 'Perdida'
            else:
                win = 'Em andamento'
            deal_info.update({'win': win})

            stop_time_limit = x.get('stop_time_limit', {})
            deal_info.update({
                'expiration_date_time': stop_time_limit.get('stop_time_limit'),
                'expired': stop_time_limit.get('expired'),
                'expired_days': stop_time_limit.get('expired_days')
            })

            user = x.get('user', {})
            deal_info.update({
                'user_id': user.get('id'),
                'user_name': user.get('name'),
                'user_nickname': user.get('nickname'),
                'user_email': user.get('email')
            })

            deal_stage = x.get('deal_stage', {})
            deal_info.update({
                'deal_stage_id': deal_stage.get('id'),
                'deal_stage_name': deal_stage.get('name'),
                'deal_stage_nickname': deal_stage.get('nickname'),
            })

            deal_source = x.get('deal_source', {})
            deal_info.update({
                'deal_source_id': deal_source.get('id'),
                'deal_source_name': deal_source.get('name'),
            })

            campaign = x.get('campaign', {})
            deal_info.update({
                'campaing_id': campaign.get('id'),
                'campaing_name': campaign.get('name')
            })

            next_task = x.get('next_task', {})
            deal_info.update({
                'next_task_id': next_task.get('id'),
                'next_task_name': next_task.get('subject'),
                'next_task_date': next_task.get('date'),
                'next_task_hour': next_task.get('hour')
            })

            deal_lost_reason = x.get('deal_lost_reason', {})
            if deal_lost_reason:
                deal_info.update({
                    'deal_lost_reason_id': deal_lost_reason.get('id'),
                    'deal_lost_reason_name': deal_lost_reason.get('name'),
                })

            contacts = x.get('contacts', [])
            for contact in contacts:
                contact_name = contact.get('name')
                contact_email = None
                contact_phone = None

                if contact.get('emails'):
                    contact_email = contact['emails'][0].get('email')

                if contact.get('phones'):
                    contact_phone = contact['phones'][0].get('phone')

                deal_info.update({
                    'contact_name': contact_name,
                    'contact_email': contact_email,
                    'contact_phone': contact_phone
                })

            deal_custom_fields = x.get('deal_custom_fields', [])
            if deal_custom_fields:
                for field in deal_custom_fields:
                    value = field.get('value')
                    label = field.get('custom_field', {}).get('label')

                    if value and label:
                        deal_info.update({label: value})

            deal_products = x.get('deal_products', [])
            if deal_products:
                for product in deal_products:
                    product_info = {
                        'id': product.get('id'),
                        'product_id': product.get('product_id'),
                        'product_name': product.get('name'),
                        'product_description': product.get('description'),
                        'product_base_price': product.get('base_price'),
                        'product_created_at': product.get('created_at'),
                        'product_updated_at': product.get('updated_at'),
                        'product_price': product.get('price'),
                        'product_amount': product.get('amount'),
                        'product_recurrence': product.get('recurrence'),
                        'product_discount': product.get('discount'),
                        'product_discount_type': product.get('discount_type'),
                        'product_total': product.get('total')
                    }
                    deal_info.update(product_info)

            results.append(deal_info)
        except Exception as e:
            print(f"Erro processando negociação: {e}")

    return results

# Fazer a requisição para a API
async def fazer_requisicao(pipeline_name, deal_pipeline_id):
    all_deals = []
    async with httpx.AsyncClient() as client:
        url = f"https://crm.rdstation.com/api/v1/deals?token={token_api}&page=1&limit=200&deal_pipeline_id={deal_pipeline_id}"
        headers = {"accept": "application/json"}

        while url:
            try:
                print(f'Processando o pipeline {pipeline_name}')
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                dados = response.json()

                deals = await extract_deals(dados)
                all_deals.extend(deals)

                # Verifica se existe próxima página
                if dados.get('next_page'):
                    url = f"https://crm.rdstation.com/api/v1/deals?token={token_api}&next_page={dados['next_page']}&limit=200&deal_pipeline_id={deal_pipeline_id}"
                else:
                    url = None

            except httpx.RequestError as e:
                print(f"Erro na requisição: {url}")
                continue

    if all_deals:
        df = pd.DataFrame(all_deals)
        df.to_excel(f'relatorio_deals_{pipeline_name}.xlsx', index=False)
        print(f"Arquivos exportados com sucesso para o pipeline {pipeline_name}.")
    else:
        print(f"Nenhuma negociação encontrada para o pipeline {pipeline_name}.")

    print("Aguardando 5 segundos para próxima requisição...")

async def main():
    tasks = [fazer_requisicao(pipeline_name, deal_pipeline_id) for pipeline_name, deal_pipeline_id in list_pipeline_id.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
