from unidecode import unidecode
import re
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from transformers import AutoTokenizer, AutoModel
import torch
import pandas as pd
import json
import numpy as np
from sklearn.cluster import KMeans
from numpy.linalg import norm
from pyspark.ml.feature import VectorAssembler 
from pyspark.ml.feature import StandardScaler 
from pyspark.ml import clustering  
from pyspark.ml.evaluation import ClusteringEvaluator

# Load BERT model
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
model = AutoModel.from_pretrained("bert-base-uncased").to(device)

# Preprocess
def clean_text(text):
    if isinstance(text, str):
        text = unidecode(text)
        cleaned_text = re.sub(r'[^\w\s]', '', text)
        # remove link
        cleaned_text = re.sub(r'http\S+', '', cleaned_text)
        return cleaned_text
    else:
        return str(text)
    
# User BERT to encode text
def encode_text(text):
    tokens = tokenizer(text, return_tensors="pt", truncation=True, padding=True).to(device)
    with torch.no_grad():
        output = model(**tokens)
    return output.last_hidden_state.mean(dim=1).squeeze().cpu().numpy()

# Process hashtags
def process_hashtags(hashtags):
    if isinstance(hashtags, list) and len(hashtags) > 0:
        return " ".join([json.loads(h.replace("'", "\""))['text'] for h in hashtags])
    else:
        return ""
    
# Process projects data file and convert to dataframe
def process_projects_data(projects_data):
    projects_data = [projects_data[k] for k in projects_data]
    socialAccounts = [x.pop('socialAccounts') for x in projects_data]
    df = pd.DataFrame(projects_data)
    return df, socialAccounts

# Process tweets data file and convert to dataframe
def process_tweets_data(tweets_data):
    tweets_data = [tweets_data[k] for k in tweets_data]
    df = pd.DataFrame(tweets_data)
    df['hashtags'] = df['hashtags'].apply(process_hashtags)
    df['text'] = df['text'].apply(clean_text)
    return df

# Recommendation
def get_recommendation(projects_df, tweets_df, socialAccounts, project_number = 12):
    # Preprocess
    X = projects_df['name']
    tweets_df['relate'] = tweets_df['text'] + tweets_df['hashtags']
    projects_df['category'] = projects_df['category'].fillna('Unknown')
    projects_df['relate'] = projects_df['name'] + " " + projects_df['category']
    projects_df['name'] = projects_df['name'].apply(clean_text)
    projects_df['category'] = projects_df['category'].apply(clean_text)
    projects_df['name'] = projects_df['name'].apply(encode_text)
    projects_df['category'] = projects_df['category'].apply(encode_text)
    projects_df = pd.concat([projects_df.drop(['name', 'category'], axis=1), projects_df['category'].apply(pd.Series), projects_df['name'].apply(pd.Series)], axis=1)
    X_categorize = projects_df.iloc[:, 0:789]
    projects_df_relate = projects_df[['relate']]
    projects_df.drop('_id', axis=1, inplace=True)
    projects_df.drop('source', axis=1, inplace=True)
    A = projects_df_relate['relate'].apply(encode_text).apply(pd.Series)
    B = tweets_df['relate'].apply(encode_text).apply(pd.Series)
    A = np.array(A)
    B = np.array(B)
    cosine = [np.sum(a.reshape(1, -1)*B, axis=1)/(norm(a.reshape(1, -1), axis=1)*norm(B, axis=1)) for a in A]

    # Recommendation
    if socialAccounts:
        print(f"For latest news about your projects, please follow these accounts: {socialAccounts}")
        print(print('Information for project: ', X[project_number]))
        print(f'Post most related to project: {tweets_df.iloc[np.argmax(cosine[project_number]), 1]} with cosine similarity: {np.max(cosine[project_number])}')
        print(f"Post trending related to project: ")
        for tweet in tweets_df.sort_values(by = ['likes', 'retweet_counts', 'views'], ascending = False).head(10)['text'].values:
            print(tweet)
    return X_categorize.drop(['relate', '_id', 'source'], axis = 1), cosine

# Categorize projects
def categorize_projects(X_categorize):
    model = KMeans(n_clusters=10, random_state=0)
    X_categorize.columns = X_categorize.columns.astype(str)
    model.fit(X_categorize)
    pd.DataFrame(model.cluster_centers_[:, :18], columns=X_categorize.columns[:18])
    print('Based on the approximate value of projects in each values, we can categorize the projects into 10 categories: ')
    return pd.DataFrame(model.cluster_centers_[:, :18], columns=X_categorize.columns[:18])

def process_spark(spark, X_categorize):
    X_categorize_spark = spark.createDataFrame(X_categorize)
    vec_assembler = VectorAssembler(inputCols = X_categorize_spark.columns, 
                                outputCol = 'features') 
    X_categorize_spark = vec_assembler.transform(X_categorize_spark)
    scaler = StandardScaler(inputCol="features",  
                            outputCol="scaledFeatures",  
                            withStd=True,  
                            withMean=False) 
    
    # Compute summary statistics by fitting the StandardScaler 
    scalerModel = scaler.fit(X_categorize_spark) 
    
    # Normalize each feature to have unit standard deviation. 
    X_categorize_spark = scalerModel.transform(X_categorize_spark) 
    # Trains a k-means model.
    kmeans = clustering.KMeans().setK(20).setMaxIter(1).setSeed(1)
    model = kmeans.fit(X_categorize_spark)

    # Make predictions
    predictions = model.transform(X_categorize_spark)

    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)