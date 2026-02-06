
import pandas as pd
import numpy as np
import json
import ast
import re


file_path = r"C:/Users/mallampati.saivenkat/Downloads/feedback-generalization-jan26.jsonl.out"

records = [] 
errors = []

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        try:
            entry = json.loads(line)
            respondent_id = entry.get("recordId")
            
            # Extract Model_Output text
            model_output = (
                entry.get("modelOutput", {})
                     .get("output", {})
                     .get("message", {})
                     .get("content", [{}])[0]
                     .get("text")
            )
            
            if respondent_id and model_output:
                records.append({"Unique_ID": respondent_id, "Model_Output": model_output})
            else:
                errors.append({"Unique_ID": respondent_id, "Model_Output": model_output})
        
        except Exception:
            errors.append({"Unique_ID": None, "Model_Output": None}) 


extract_df = pd.DataFrame(records)
error_df = pd.DataFrame(errors)


extract_df.to_excel("Generalization_Output.xlsx", index=False)
# After creating the excel, clean the Model_Output column, it may/ may not contains excess text like ('''json, ''') like that

# After cleaning
df = pd.read_excel("Generalization_Output.xlsx")

def merge_sentiments(json_text):
    try:
        data = json.loads(json_text)
        for category, sentiments in data.items():
            merged = {}
            for sentiment, issues in sentiments.items():
                merged.setdefault(sentiment, []).extend(issues)
            data[category] = merged
        return data
    except:
        return None

df["Parsed_Output"] = df["Model_Output"].apply(merge_sentiments)

df.to_excel("Merged_Generalization_Output.xlsx", index=False)

# After creating of "Merged" excel, try to check whether there are any left out or I mean "NULL" values in Parsed_Output column, 
# if there are NULL that means the format in the "Model_Ouput" is wrong and rectify it manually

# Remove the whole "Model_Output" column and rename the "Parsed_Output" column with "Model_Output" name
df = pd.read_excel("Merged_Generalization.xlsx")

import pandas as pd
import ast


# Convert stringified dict to actual dict
df['Model_Output'] = df['Model_Output'].apply(ast.literal_eval)

rows = []
for _, row in df.iterrows():
    unique_id = row['Unique_ID']
    model_output = row['Model_Output']
    
    # Build category map
    category_map = {}
    for category_id, sentiment_dict in model_output.items():
        positive_issues = []
        negative_issues = []
        neutral_issues = [] 
        for sentiment, issues in sentiment_dict.items():
            if sentiment == '1':
                positive_issues.extend(issues)
            elif sentiment == '-1':
                negative_issues.extend(issues)
            elif sentiment == '0':
                neutral_issues.extend(issues)
        category_map[int(category_id)] = {
            'Positive_Issues': positive_issues,
            'Negative_Issues': negative_issues,
            'Neutral_Issues': neutral_issues
        }
    
    # Ensure all 14 categories
    for cat_id in range(1, 15):
        pos_list = category_map.get(cat_id, {}).get('Positive_Issues', [])
        neg_list = category_map.get(cat_id, {}).get('Negative_Issues', [])
        neu_list = category_map.get(cat_id, {}).get('Neutral_Issues', [])

        max_len = max(len(pos_list), len(neg_list), len(neu_list), 1)

        for i in range(max_len):
            rows.append({
                'Unique_ID': unique_id,
                'Category_ID': cat_id,
                'Positive_Issues': pos_list[i] if i < len(pos_list) else None,
                'Negative_Issues': neg_list[i] if i < len(neg_list) else None,
                'Neutral_Issues': neu_list[i] if i < len(neu_list) else None
            })

# Create final DataFrame
final_df = pd.DataFrame(rows)

# This is the final dataframe which will be uploaded to MySQL
final_df.sort_values(by=['Unique_ID', 'Category_ID'], inplace=True)


 
