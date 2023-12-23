from unidecode import unidecode
import re
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from transformers import AutoTokenizer, AutoModel
import torch
import pandas as pd


def clean_text(text):
    if isinstance(text, str):
        text = unidecode(text)
        cleaned_text = re.sub(r'[^\w\s]', '', text)

        # remove link
        cleaned_text = re.sub(r'http\S+', '', cleaned_text)

        return cleaned_text
    else:
        return str(text)
# use BERT to encode text

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
model = AutoModel.from_pretrained("bert-base-uncased").to(device)

def encode_text(text):
    tokens = tokenizer(text, return_tensors="pt", truncation=True, padding=True).to(device)
    with torch.no_grad():
        output = model(**tokens)
    return output.last_hidden_state.mean(dim=1).squeeze().cpu().numpy()
    
def processing_data(data):
    data['user_rawDescription'] = data['user_rawDescription'].apply(clean_text)
    data['rawContent'] = data['rawContent'].apply(clean_text)
    
    # One-hot encoding for  language columns
    data = pd.get_dummies(data, columns=['language'], drop_first=True)
    # standard for numeric columns
    numeric_features = ['likeCount', 'replyCount', 'retweetCount', 'user_favouritesCount', 'user_followersCount', 'user_friendsCount']
    scaler = StandardScaler()
    data[numeric_features] = scaler.fit_transform(data[numeric_features])

    data['user_blue'] = data['user_blue'].astype(int)
    data['user_verified'] = data['user_verified'].astype(int)

    data['user_rawDescription_encoding'] = data['user_rawDescription'].apply(encode_text)
    data['rawContent_encoding'] = data['rawContent'].apply(encode_text)

    data_final = pd.concat([data.drop(['user_rawDescription', 'rawContent'], axis=1), data['user_rawDescription_encoding'].apply(pd.Series), data['rawContent_encoding'].apply(pd.Series)], axis=1)
    return data_final

if __name__ == "__main__":
    data = pd.read_csv('hashtags.csv',lineterminator = '\n')
    data_processed = processing_data(data_test)
    data_processed.to_csv("processed_hashtags.csv")