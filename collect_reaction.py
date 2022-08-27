from datetime import datetime, timezone, timedelta

import pandas as pd
import pytz
import tweepy

API_KEY             = "APIキー"
API_SECRET          = "APIシークレット"
ACCESS_TOKEN        = "アクセストークン"
ACCESS_TOKEN_SECRET = "アクセストークンのシークレット"

#関数:　UTCをJSTに変換する
def change_time_JST(u_time):
    #イギリスのtimezoneを設定するために再定義する
    utc_time = datetime(u_time.year, u_time.month,u_time.day, \
    u_time.hour,u_time.minute,u_time.second, tzinfo=timezone.utc)
    #タイムゾーンを日本時刻に変換
    jst_time_since = utc_time.astimezone(pytz.timezone("Asia/Tokyo"))
    # 文字列で返す
    str_time_since = jst_time_since.strftime("%Y-%m-%d_%H:%M:%S")
    return str_time_since

auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)

# CSVに書き込む用のマスターDataFrame
df = pd.DataFrame({}, columns=['name', 'screen_name', 'text', 'created_at'])

# RTを全件取得し，RTしたユーザーのIDとRT時刻を取得
# そのユーザーのRT時刻直後のツイートを取得(exclude:retweets)
#   RTから10分以内で検索（終点を指定しないと最新のものから取得するため）
# APIリクエスト数の上限を超えたら15分待機
# 結果をCSV出力（name, screen_name, text, created_at）
max_id = -1

while True:
    # 初回以外は，前回取得した最後のTweetのID - 1をmax_idとする
    if max_id != -1:
        max_id -= 1

    # 戻り値はヒットしたTweetのリストと考えて良い（たぶん正確には違うけど）
    ret = api.search_tweets(q="「Shinobi-Mas」を公開 filter:nativeretweets", count=100, max_id=max_id)

    # 検索結果が0件になったら終了
    if len(ret) == 0:
        break

    for tweet in ret:
        # RTのTweet ID
        id = tweet.id
        # RTしたユーザーの情報
        user = tweet.user
        # 検索範囲（開始，終了）
        since = tweet.created_at.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        until = (tweet.created_at + timedelta(minutes=10)).strftime("%Y-%m-%d_%H:%M:%S_UTC")
        # 検索クエリ
        query = f"from:{user.screen_name} since:{since} until:{until} exclude:retweets"
        print(f"query: {query}")
        ret2 = api.search_tweets(q=query)
        # RT後10分以内に呟きがなければスキップ
        if len(ret2) > 0:
            # Tweet本文（改行文字を除去）
            text = ret2[-1]._json['text'].replace("\n", "")
            # Tweet日時(JST)
            ctime = change_time_JST(ret2[-1].created_at)
            # 1行のDataFrameを作成してマスターに結合
            df2 = pd.DataFrame({'name': [user.name], 'screen_name': [user.screen_name], 'text': [text], 'created_at': [ctime]})
            df = pd.concat([df, df2])
    # max_idの更新
    max_id = ret[-1].id

# DataFrameをCSVに出力
df.to_csv('out.csv', index=False)
