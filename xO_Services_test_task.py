# импортирование библиотек
import pandas as pd
import numpy as np
import re
from datetime import datetime

COSTS = 'm_costs'
CLICKS = 'm_clicks'
DATE = 'created_at'
KEYS = ['created_at',
        'd_utm_source',
        'd_utm_medium',
        'd_utm_campaign',
        'd_utm_content']

# загрузка данных
path = '' # укажите путь к файлу данных

ads = pd.read_csv(path + 'ads.csv')
leads = pd.read_csv(path + 'leads.csv')
purchases = pd.read_csv(path + 'purchases.csv')

# обработка логических несоответствий
leads = leads[leads.lead_created_at >= ads.created_at.min()]

# обработка неинформативного столбца 'd_utm_term'
ads = ads.drop('d_utm_term', axis=1).reset_index(drop=True)
leads = leads[leads.d_lead_utm_term.isna()].drop('d_lead_utm_term', axis=1).reset_index(drop=True)

# приведение данных к одному типу
ads[['d_utm_campaign','d_utm_content']] = \
ads[['d_utm_campaign','d_utm_content']].astype(str)

# обработка пропущенных значений
leads = leads.fillna('unknown')
purchases.client_id = purchases.client_id.fillna('unknonw_client')

# унификация названий столбцов для объединения таблиц
for old_col in leads.columns:
    new_col = re.sub('_lead', '', old_col)
    leads = leads.rename(columns={old_col: new_col})
leads = leads.rename(columns={'lead_created_at': DATE})
    
# объединение таблиц
merge_ad_lead = ads.merge(leads,
                          how='left',
                          on=KEYS)

merge_ad_lead_client = merge_ad_lead.merge(purchases,
                                           how='left',
                                           on='client_id')

# сделаем сводную таблицу с агрегирующими функциями
final = (
    pd.pivot_table(merge_ad_lead_client,
                   index=KEYS,
                   values=['m_clicks',
                           'm_cost',
                           'lead_id',
                           'purchase_id',
                           'm_purchase_amount'],
                   aggfunc={'m_clicks': 'sum',
                            'm_cost': 'sum',
                            'lead_id': 'count',
                            'purchase_id': 'count',
                            'm_purchase_amount': 'sum'})
    .reset_index()
)

# посчитаем нужные метрики
final['cpl_per_day'] = final.m_cost / final.lead_id
final['roas_per_day_perc'] = final.m_purchase_amount / final.m_cost * 100
final.cpl_per_day = final.cpl_per_day.replace(np.inf, 0)
final = final.fillna(0)

# финальная подгтовка таблицы к выгрузке
final = final.drop('d_utm_content', axis=1)
final = final.rename(columns={
    'lead_id': 'leads_total',
    'm_clicks': 'clicks_total',
    'm_cost': 'cost_total',
    'm_purchase_amount': 'total_sales_revenue',
    'purchase_id': 'purchases_total'})

# раскомментируйте для загрузки в папку
# final.to_excel(f'final-{datetime.now().date()}.xlsx')