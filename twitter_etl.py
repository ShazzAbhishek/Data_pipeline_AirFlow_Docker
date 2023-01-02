import pandas as pd
import tweepy
import numpy as np
import s3fs


def run_twitter_etl():
    ## new access token:
    consumer_key = "SWGetggvKRR7zrdkysHFY47fc"
    consumer_secret = "GopgUL8HgVaSixhda4bfagaLdY33pY1JHEcxi9LrW0R1sLIvp3"
    bearer_token = "AAAAAAAAAAAAAAAAAAAAABjekwEAAAAAmhnYy2PbwXB77kbeZTRLn2c9eyU%3DcBYusPFwHc4L4ejA4o0DlNvMisRVfR0AdneAJuUlW9uQchPyPL"
    access_token = "1609262606426034177-KbgK9616M0qkw0aLnNTlwowAy3Cvc5"
    access_token_secret = "fHrgbd5TqDPcAPW8WUu3hBSd4xrWXnIPsm6W4yFBpSADW"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@SpaceX',
                               # 200 is the maximum allowed count
                               count=25,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)
    # print(list)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')
