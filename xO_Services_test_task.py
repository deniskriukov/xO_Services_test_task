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

# перевод дат в тип 'datetime'
def change_type(data, column):
    data[column] = pd.to_datetime(data[column], format='%Y-%m-%d')
    
change_type(ads, 'created_at')
change_type(leads, 'lead_created_at')
change_type(purchases, 'purchase_created_at')

# унификация названий столбцов для объединения таблиц
for old_col in leads.columns:
    new_col = re.sub('_lead', '', old_col)
    leads = leads.rename(columns={old_col: new_col})
leads = leads.rename(columns={'lead_created_at': DATE})

# объединение таблиц
merge_ad_lead = ads.merge(leads,
                          how='left',
                          on=KEYS)
merge_full = merge_ad_lead.merge(purchases,
                                 how='left',
                                 on='client_id')

# выделим клики, для которых нет лидов
ads_without_leads = merge_full[merge_full.lead_id.isnull()]
merge_full = merge_full.drop(ads_without_leads.index).reset_index(drop=True)
ads_without_leads = ads_without_leads.reset_index(drop=True)

# выделим лиды, для которых нет покупок
leads_without_purchases = merge_full[merge_full.purchase_id.isnull()]
merge_full = merge_full.drop(leads_without_purchases.index).reset_index(drop=True)
leads_without_purchases = leads_without_purchases.reset_index(drop=True)

# найдем такие строки, где покупка была совершена
# в течение 15 дней с момента создания заявки;
# остальные запишем в отдельную переменную
leads_with_relevant_purchases = (
    merge_full[
        ((merge_full.purchase_created_at - merge_full.created_at) / np.timedelta64(1, 'D') >= 0) & \
        ((merge_full.purchase_created_at - merge_full.created_at) / np.timedelta64(1, 'D') <= 15)
    ]
)
leads_without_relevant_purchases = merge_full.drop(leads_with_relevant_purchases.index).reset_index(drop=True)
leads_with_relevant_purchases = leads_with_relevant_purchases.reset_index(drop=True)

# сгруппируем лиды по покупкам
# и оставим только ближайший к покупке
leads_purchases_nearest = pd.DataFrame()
leads_purchases_other = pd.DataFrame()

for row in leads_with_relevant_purchases.index:
    
    purchase_lines = pd.DataFrame()
    diff_list = []
    true_matrix = []
    false_matrix = []
    
    # сгруппируем строки по id покупки и найдем её дату
    purchase_id = leads_with_relevant_purchases.loc[row, 'purchase_id']
    purchase_id_group = leads_with_relevant_purchases[leads_with_relevant_purchases.purchase_id == purchase_id]
    purchase_date = leads_with_relevant_purchases.loc[row, 'purchase_created_at']
    
    # найдем разницу даты создания лида и покупки;
    # запишем разницы для 'purchase_id_group' в список 'diff_list'
    for i in purchase_id_group.index:
        lead_date = purchase_id_group.loc[i, 'created_at']
        days_diff = (purchase_date - lead_date) / np.timedelta64(1, 'D')
        diff_list.append(days_diff)
    
    # создадим булеву маску, где ближайшему к покупке лиду будет присовено 'True'
    for j in range(len(diff_list)):
        true_matrix.append(diff_list[j] == min(diff_list))
        false_matrix.append(not (diff_list[j] == min(diff_list)))
        
    # распределим заявки в группе по двум таблицам
    leads_purchases_nearest = pd.concat([leads_purchases_nearest, purchase_id_group[true_matrix]])
    leads_purchases_other = pd.concat([leads_purchases_other, purchase_id_group[false_matrix]])

leads_purchases_nearest = leads_purchases_nearest.drop_duplicates().reset_index(drop=True)
leads_purchases_other = leads_purchases_other.drop_duplicates().reset_index(drop=True)

# таблица с кликами и лидами, но без покупок
# (в течение 15 дней или ближайших к покупке)
ads_leads = (
    pd.concat([
        leads_without_purchases,
        leads_without_relevant_purchases,
        leads_purchases_other])
    .reset_index(drop=True)
)

# уберём информацию о покупках, так как они нерелевантны для данных лидов
# и не будут учитываться при подсчете покупок;
# однако лиды будут считаться как лиды без покупок
ads_leads[['purchase_created_at',
           'purchase_id', 
           'm_purchase_amount']] = None
ads_leads = ads_leads.drop_duplicates().reset_index(drop=True)

merge_full = pd.concat([ads_without_leads, ads_leads, leads_purchases_nearest]).reset_index(drop=True)

# сделаем сводную таблицу с агрегирующими функциями
final_data = (
    pd.pivot_table(merge_full,
                   index=['created_at',
                          'd_utm_source',
                          'd_utm_medium',
                          'd_utm_campaign'],
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

# изменим названия столбцов в соответствии со значениями
final_data = final_data.rename(columns={
    'm_clicks': 'clicks_total',
    'm_cost': 'cost_total',
    'lead_id': 'lead_count',
    'purchase_id': 'purchase_count',
    'm_purchase_amount': 'total_sales_revenue'
})

# посчитаем метрики CPL и ROAS
for row in final_data.index:
    
    if final_data.loc[row, 'lead_count'] == 0:
        final_data.loc[row, 'cpl_per_day'] = 0
    else:
        final_data.loc[row, 'cpl_per_day'] = \
        final_data.loc[row, 'cost_total'] / final_data.loc[row, 'lead_count']
        
    if final_data.loc[row, 'cost_total'] == 0:
        final_data.loc[row, 'roas_per_day_perc'] = 0
    else:
        final_data.loc[row, 'roas_per_day_perc'] = \
        (final_data.loc[row, 'total_sales_revenue'] / final_data.loc[row, 'cost_total']) * 100

# загрузка таблицы в папку
final_data.to_excel(f'final_data-{datetime.now().date()}.xlsx')