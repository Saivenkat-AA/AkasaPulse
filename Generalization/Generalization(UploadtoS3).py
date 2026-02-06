import pandas as pd
import numpy as np
import logging
import boto3
import re
import json
from tqdm import tqdm
from botocore.exceptions import ClientError
import uuid
import time
from datetime import datetime
import os
import tempfile

logger = logging.getLogger(__name__)    
logging.basicConfig(level=logging.INFO)

df = pd.read_excel("df.xlsx")
# Build {Category_ID: Sentiments} per (Unique_ID, Feedback)
series_map = (
    df.groupby(["Unique_ID", "Feedback"])
      .apply(lambda g: dict(zip(g["Category_ID"], g["Sentiments"])))
)

# Convert Series to DataFrame with a proper column name
agg = series_map.to_frame("category_sentiment_map").reset_index()

# Compose Feedback_Output as `feedback:{...}`
agg["Feedback_Output"] = agg["Feedback"] + ":" + agg["category_sentiment_map"].astype(str)

# Final result
result = agg[["Unique_ID", "Feedback_Output"]]
result.to_excel("df_agg.xlsx", index=False)


DF = pd.read_excel("df_agg.xlsx")


def sanitize_key(raw_key: str) -> str:
    """Clean feedback key text (paragraphs, bullet points, etc.)."""
    key = raw_key.strip()
    key = re.sub(r'\s+', ' ', key)   # collapse whitespace
    key = key.replace('€', 'EUR')    # replace currency symbol
    return key

def extract_int_pairs(val: str) -> dict:
    """Extract integer:value pairs like 1:1, 2:-1 from the string."""
    pairs = {}
    for left, right in re.findall(r'([+-]?\d+)\s*:\s*([+-]?\d+)', val):
        try:
            k = int(left.lstrip("0") or "0")   # normalize leading zeros
            v = int(right)
            pairs[k] = v
        except:
            continue
    return pairs

parse_errors = []

def safe_parse_row(s: str, idx: int):
    """Parse one Feedback_Output row into {feedback_key: {int:int}}."""
    try:
        if ":" not in s:
            parse_errors.append({"row_index": idx, "error_type": "ValueError", "error": "Missing colon delimiter"})
            return {}

        # Split on the first colon only
        key_raw, val_raw = s.split(":", 1)
        key = sanitize_key(key_raw)
        val = val_raw.strip("() ")   # remove stray parentheses

        kv_dict = extract_int_pairs(val)
        if not kv_dict:
            parse_errors.append({"row_index": idx, "error_type": "ParseError", "error": "No int:int pairs found"})
            return {}

        return {key: kv_dict}

    except Exception as e:
        parse_errors.append({"row_index": idx, "error_type": type(e).__name__, "error": str(e)})
        return {}


DF["feedback_dict"] = DF.apply(lambda row: safe_parse_row(row["Feedback_Output"], row.name), axis=1)


if parse_errors:
    print("\n--- Parse Errors ---")
    print(pd.DataFrame(parse_errors))
else:
    print("\nNo parse errors. All rows parsed successfully!")


df = (
    DF.groupby("Unique_ID")
      .apply(lambda g: {list(d.keys())[0]: list(d.values())[0] for d in g["feedback_dict"] if d})
      .reset_index(name="Feedback_Output")
)

df["Feedback_Count"] = df["Feedback_Output"].apply(lambda x: len(x))

def create_local_batch_input_file(df, feedback_column, respondent_id_column, output_dir="./batch_temp"):
    """
    Create batch input file locally with Respondent_ID included and dynamic maxTokens based on Feedback_Count.
    """
    import os, json, uuid
    from datetime import datetime

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    job_id = str(uuid.uuid4())[:8]
    input_file = os.path.join(output_dir, f"feedback-input-{timestamp}-{job_id}.jsonl")

    sentiment_id = {'Positive': 1, 'Neutral': 0, 'Negative': -1}

    category_keywords = {
        'Compensation': ['Salary', 'Allowance'], 
        'Employee Policies & Benefits': ['ELT', 'Insurance', 'Transport', 'Night shift Allowance', 'Leaves', 'Medi Claims', 'HR Polices'], 
        'Career & Growth': ['Promotion', 'Progression', 'Internal Job Post', 'Transfer'], 
        'Reward and Recognition': ['Getting recognised for work', 'Appreciation at work place'], 
        'Learning & Development': ['Learning needs', 'Training needs'], 
        'Work Place Amenities': ['Drinking Water', 'Space', 'Washroom'], 
        'Work Environment & Culture': ['Respect at work place', 'Fairness at work place', 'Transparency', 'Inclusivity', 'Positivity at work', 'Empowerment'], 
        'Operational Effectiveness': ['Rostering', 'Scheduling', 'Resource Shortage', 'Lack of Information', 'Communication Gaps'], 
        'Leadership': ['HOD', 'EXCOM', 'Vison of organisation', 'Org decisions'], 
        'Line Manager': ['Immediate Line Manager'], 
        'Team': ['Immediate team members'], 
        'Cross functional Collaboration': ['Other departments and teams'], 
        'Pride and Brand association': ['General Positive or Negative feedback about Akasa Air']
    }
    
    category_examples = {
        'Compensation': 'Salary should be revised.Kindly increase hours or salary.',
        'Employee Policies & Benefits': 'Night allowances Two way cab felicity or transport allowance.',
        'Career & Growth': 'Currently I am looking for growth in terms of designation. I have been working with Akasa from last 3.5 years and have managed different profiles in all these years. Grateful to Akasa for providing me with an opportunity to work on such diverse profiles but I am currently looking at taking a step ahead with promotion as well.',
        'Reward and Recognition': 'More staff centric appreciation to be there,best employee of month ,best performers Tobe awarded',
        'Learning & Development': 'Training should be expedited',
        'Work Place Amenities': 'Just need some extra facilities for employees',
        'Work Environment & Culture': 'To much politics & favoritism in the team.  Need to maintain transparency & fairness in the team.',
        'Operational Effectiveness': 'No work life balance with long patterns and rostering changes',
        'Leadership': 'I wanted to take a moment to express my appreciation for the outstanding leadership demonstrated by Ananya Narula (SR GM AALA) Ananya has consistently gone above and beyond to foster a positive work environment, motivate individuals, promote transparency, and lead by example. Her efforts have undoubtedly contributed to the overall success and morale of our team.',
        'Line Manager': 'The duty manager send mails for things which I havenâ€™t done and I have to explain every time.',
        'Team': 'Seniors should treat all subordinates with equal respect and fairness.',
        'Cross functional Collaboration': 'Improved inter-departmental communication would enhance overall efficiency.Streamlining processes for approvals and addressing requirements could help reduce delays and prevent unnecessary issues with regulatory authorities.',
        'Pride and Brand association': 'I feel extremely good to work with Akasa Air.'
    }

    category_id = {
        'Compensation': 1, 
        'Employee Policies & Benefits': 2, 
        'Career & Growth': 3, 
        'Reward and Recognition': 4, 
        'Learning & Development': 5, 
        'Work Place Amenities': 6, 
        'Work Environment & Culture': 7, 
        'Operational Effectiveness': 8, 
        'Leadership': 9, 
        'Line Manager': 10, 
        'Team': 11, 
        'Cross functional Collaboration': 12, 
        'Pride and Brand association': 13,
        'Others': 14
    }

    batch_requests = []

    for _, row in df.iterrows():
        feedback_text = str(row[feedback_column])
        respondent_id = str(row[respondent_id_column])
        feedback_count = int(row.get('Feedback_Count', 0))  # Default to 0 if missing

        # Dynamic maxTokens logic
        max_tokens = 1500 if feedback_count <= 150 else feedback_count * 10
        user_message = f"""You are an expert text analyst specializing in employee feedback.

Task:
Process a batch of employee feedbacks and produce a summary of issues grouped by categories and sentiments.

Input:
- Feedbacks are provided in a dict format: {{feedback_1: {{"category_id_1": "sentiment_id_1", "category_id_2": "sentiment_id_2", ...}}, feedback_2: {{...}}, ...}}.
- Category and sentiment mappings are given in {category_id} and {sentiment_id}.
- Unique_Identifier: {respondent_id}
- Input Feedbacks: {feedback_text}

Instructions:
1. Write the top 5 frequent / top most occurring positive, negative, and neutral issues for each category in short sentences which can summarize. Here the category is identified by category_id and sentiment is identified by sentiment_id.
2. For understanding the categories, refer to the category keywords and examples provided in {category_keywords} and {category_examples}.
3. Include issues for all sentiments: Positive (1), Negative (-1), and Neutral (0).
4. Do not include any issues for sentiments that are not present in the input feedbacks.
5. While writing the issues for each category, write them as short sentences summarizing the main concern or praise not full sentences and the issue should be repeatable across multiple feedbacks.
6. In mentioning the issues, do not repeat the same issue again for all the categories. For example, if "Salary is low" is mentioned as an issue for category_id 1 (Compensation), do not mention the same issue for any other category_id.
7. If "Salary is low" is mentioned as an issue for category_id 1 (Compensation), do not mention it again in the same category_id again and again in the same sentiment_id (positive, negative, or neutral).
8. Different issues should be mentioned for the same category under different sentiments that means if the issue is present in positive sentiment that issue should not be mentioned in negative or neutral sentiment for the same category.
9. While writing the issues/phrases for positive, negative, and neutral under each category, make sure that you write the things only related to that category.
10. While writing the issues for neutral sentiment, ensure that they should be suggestions or observations that are neither positive nor negative ( this is important ).
11. If there are no issues present for a particular sentiment in a category, then for that category only the issues for the present sentiments should be written.
12. Rank issues by frequency of occurrence not by sentiment score.
13. The issues must be maximum of 5 per category and sentiment and should be short phrases summarizing the main concern or praise.
14. No additional text or explanations should be included in the output.
15. In the Output JSON, don't mention the words "category_id" or "sentiment_id", only use the respective IDs as keys.
16. While writing the issues/phrases for positive or neutral or negative under each category, make sure that you write the things only related to that category.
17. If there are no positive sentiment_id is present for a particular category, then for that category mention only the sentiment issues should be written and vice versa.

18. Output must be valid JSON in the following format:

{{
  "category_id_1": {{
    "{{positive_sentiment_id}}": ["issue1", "issue2", ...],
    "{{negative_sentiment_id}}": ["issue1", "issue2", ...],
    "{{neutral_sentiment_id}}": ["issue1", "issue2", ...]
  }},
  "category_id_2": {{
    "{{positive_sentiment_id}}": ["issue1", "issue2", ...],
    "{{negative_sentiment_id}}": ["issue1", "issue2", ...],
    "{{neutral_sentiment_id}}": ["issue1", "issue2", ...]
  }}
}}
"""

        batch_request = {
            "recordId": respondent_id,
            "modelInput": {
                "messages": [
                    {"role": "user", "content": [{"text": user_message}]}
                ],
                "inferenceConfig": {
                    "maxTokens": max_tokens,
                    "temperature": 0,
                    "topP": 0.1
                }
            }
        }
        batch_requests.append(batch_request)

    with open(input_file, 'w', encoding='utf-8') as f:
        for request in batch_requests:
            f.write(json.dumps(request) + '\n')

    print(f"Batch input file created: {input_file}")
    print(f"Total requests: {len(batch_requests)}")
    return input_file




def upload_to_s3_temp(file_path, s3_client, bucket_name):
    """
    Upload file to S3 temporarily for batch processing
    
    Args:
        file_path (str): Local file path
        s3_client: S3 client
        bucket_name (str): S3 bucket name
    
    Returns:
        str: S3 URI
    """
    filename = os.path.basename(file_path)
    s3_key = f"temp-batch-inference-generalization/{filename}"
    
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        print(f"File uploaded to: {s3_uri}")
        return s3_uri
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return None


def cleanup_temp_files(*file_paths):
    """
    Clean up temporary files
    
    Args:
        *file_paths: Variable number of file paths to delete
    """
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"Cleaned up: {file_path}")
            except Exception as e:
                print(f"Error cleaning up {file_path}: {e}")



def process_feedback_batch_local(df, feedback_column, s3_bucket, role_arn, aws_credentials, cleanup_files=True): #1
    """
    Process feedback using batch inference with local file handling
    
    Args:
        df (pd.DataFrame): DataFrame with feedback data
        feedback_column (str): Column name containing feedback text
        s3_bucket (str): S3 bucket for temporary files (still needed for batch processing)
        role_arn (str): IAM role ARN for batch processing
        aws_credentials (dict): AWS credentials dictionary
        cleanup_files (bool): Whether to clean up temporary files
    
    Returns:
        pd.DataFrame: DataFrame with categories and sentiments added
    """
    
    # Initialize AWS clients
    bedrock_client = boto3.client(
        "bedrock",
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        aws_session_token=aws_credentials['aws_session_token'],
        region_name=aws_credentials['region_name']
    )
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        aws_session_token=aws_credentials['aws_session_token'],
        region_name=aws_credentials['region_name']
    )
    
    model_id = "apac.amazon.nova-pro-v1:0"
    
    # Generate unique identifiers
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    job_id = str(uuid.uuid4())[:8]
    
    local_input_file = None
    local_output_file = None
    
    try:
        # Step 1: Create local input file
        print("Step 1: Creating local input file...")
        local_input_file = create_local_batch_input_file(df,respondent_id_column='Unique_ID', feedback_column = feedback_column)
        
        # Step 2: Upload to S3 temporarily
        print("Step 2: Uploading to S3 temporarily...")
        input_s3_uri = upload_to_s3_temp(local_input_file, s3_client, s3_bucket)
        if not input_s3_uri:
            return df
        
    
    finally:
        # Cleanup temporary files
        if cleanup_files:
            cleanup_temp_files(local_input_file, local_output_file)
    

def process_feedbacks(df, feedback_column, s3_bucket, role_arn):
    """
    Simple function to process feedbacks with your existing credentials
    
    Args:
        df (pd.DataFrame): DataFrame with feedback data
        feedback_column (str): Column name containing feedback text
        s3_bucket (str): S3 bucket name
        role_arn (str): IAM role ARN
    
    Returns:
        pd.DataFrame: DataFrame with results
    """
    aws_credentials = {
        'aws_access_key_id': 'A',
        'aws_secret_access_key': 'v',
        'aws_session_token': 'IQo',
        'region_name': 'ap-south-1'
    }
    # AWS Credentials changes everyday and varies with the each user
    return process_feedback_batch_local(df, feedback_column, s3_bucket, role_arn, aws_credentials)

result_df = process_feedbacks(df=df,feedback_column='Feedback_Output',s3_bucket='akasa-bedrock',role_arn='arn:aws:iam::891377165721:role/Amazon-Bedrock-Batchinference-Role')



