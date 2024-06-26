import pandas as pd, requests, json, time


# Variables for api call
private_key = '29309ca885bb4c9096e08ef47213962d19137c17218b4f0a826024807837d435'
api_url = 'https://xivapi.com'
game_data = '/GCScripShopItem'
pg_url = api_url + game_data + "?private_key=" + private_key
pg_ttl = requests.get(pg_url).json()["Pagination"]["PageTotal"]
call_url = api_url + game_data + "?pages={}" + "?private_key=" + private_key

resp_list = requests.get(api_url + game_data + "?pages=2" + "?private_key=" + private_key).json()["Results"]

response_df = pd.DataFrame(resp_list)
response_df['Url'] = api_url + response_df['Url']

urls_list = response_df['Url'].to_list()


row_processed = 0
item_list = []
for url in urls_list:
    try:
        item_api = pd.json_normalize(requests.get(url).json())
        item_response = item_api[["ID","RequiredGrandCompanyRank.Tier","Item.ID","Item.ItemUICategory.IconHD","Item.Name","Item.ItemUICategory.Name_en","Item.IsUntradable","CostGCSeals","Item.StackSize"]] \
            .values.tolist()
        item_list = item_list + item_response
        row_processed += 1
        if row_processed == 20:
            row_processed = 0
            time.sleep(2)  
    except:
        continue
    
GC_Col = ["GC_ID","GC_Tier","Item_ID","HD_Icon","Item_Name","Item_Category","Untradable_Ind","GC_Seals","Stack_Size"]
GCScripShop_df = pd.DataFrame(item_list, columns = GC_Col).sort_values(by=['GC_ID'])


display(GCScripShop_df)
