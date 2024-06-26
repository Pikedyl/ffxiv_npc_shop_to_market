import requests, json, pandas as pd, pytz
from datetime import datetime

api_url = 'https://universalis.app/api/v2/history'
worldDcRegion = '/Brynhildr'
timeRange = '?entriesWithin=1209600'
timeZone = 'America/Chicago'
tradable_items = GCScripShop_df[GCScripShop_df['Untradable_Ind'] != 1]['Item_ID'].values
csv_item_id = ",".join(str(x) for x in tradable_items)
item_ids = "/"+csv_item_id

item_hist = requests.get(api_url+worldDcRegion+item_ids+timeRange).json()['items']

items_list = tradable_items.tolist()

hist_list = []
#for i in items_list:
    #try:
        #hist_list = hist_list + pd.json_normalize(flatten(item_hist[str(i)]))[["itemID","regularSaleVelocity","nqSaleVelocity","hqSaleVelocity","worldName","entries_0_pricePerUnit","entries_0_quantity","entries_0_hq","entries_0_buyerName","entries_0_timestamp"]].values.tolist()
    #except KeyError:
        #continue

for i in items_list:
    try:
        hist_df = pd.json_normalize(item_hist[str(i)]).explode('entries').reset_index(drop=True)
        hist_df = hist_df.merge(pd.json_normalize(hist_df['entries']), left_index=True, right_index=True).drop('entries', axis=1)
        hist_list = hist_list +  hist_df[["itemID","regularSaleVelocity","nqSaleVelocity","hqSaleVelocity","worldName","pricePerUnit","quantity","hq","buyerName","timestamp"]].values.tolist()   
    except KeyError:
        continue

hist_columns = ["Item_ID","Reg_Sale_Vel","NQ_Sale_Vel","HQ_Sale_Vel","World","Price_Per_Unit","Qty","HQ_Ind","Buyer","Timestamp"]
hist_table = pd.DataFrame(hist_list, columns = hist_columns).sort_values(["Item_ID","Timestamp"], ascending=[True,False]).reset_index(drop=True)
hist_table['RN'] = hist_table.sort_values(["Item_ID","Timestamp"], ascending=[True,False]) \
    .groupby("Item_ID") \
    .cumcount() + 1
hist_table['Timestamp'] = pd.to_datetime(hist_table['Timestamp'],unit='s',utc=True).dt.tz_convert(timeZone).dt.strftime('%Y/%m/%d %H:%M:%S')

hist_agg_tbl = hist_table.groupby(["Item_ID","Reg_Sale_Vel","NQ_Sale_Vel","HQ_Sale_Vel","World"], as_index=False) \
    .agg(Avg_Price_Per_Unit=pd.NamedAgg(column='Price_Per_Unit', aggfunc='mean'),
        Avg_Qty=pd.NamedAgg(column='Qty', aggfunc='mean'),
         Last_Buyer=pd.NamedAgg(column='Buyer', aggfunc='first'),
         Last_Purchase_Tms=pd.NamedAgg(column='Timestamp', aggfunc='max'))

gc_mrkt_join = pd.merge(GCScripShop_df[GCScripShop_df['Untradable_Ind'] != 1],hist_agg_tbl,how='left',on="Item_ID")

display(gc_mrkt_join)
