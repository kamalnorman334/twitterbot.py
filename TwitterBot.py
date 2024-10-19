import snscrape.modules.twitter as sntwitter
import pandas as pd
from tqdm import tqdm
import tweepy
import time
 

search = str(input("Enter keyword or hashtag : "))
maxTweets = int(input("Enter number of tweets you want to scrape : "))
keyword = str(input("Enter keyword to search in profile descriptions : "))
text = str(input("Enter message to DM : "))

temp=[]
temp.append(['id','username','date','tweet'])

print("\n\nScraping tweets with--"+search+"--keyword\n\n") 

for i,tweet in tqdm(enumerate(sntwitter.TwitterSearchScraper(search+" -filter:replies").get_items())):
    if i > maxTweets :
        break
    temp.append([tweet.id, tweet.username, tweet.date, tweet.content])
df = pd.DataFrame(temp)
df.to_csv(search+" results.csv",encoding="UTF-8-sig",index=False)

print("\n\nData Scraped and Saved to CSV.\n\n\nRemoving Duplicate Usernames...")

df1 = df.drop_duplicates(subset=1, keep='first', inplace=False)

print("\n\nDuplicates Removed...\n\n")

print("Scraping User Profiles :")

consumer_key =  ""
consumer_secret = ""
access_key = ""
access_secret = ""
 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
 
api = tweepy.API(auth)

temp=1
f= []
for i in tqdm(df1[1][1:]):
    user = api.get_user(i) 
    userdata = {
        'user' : i,
        'desription' : user.description
    }
    f.append(userdata)
    temp+=1
    if(temp%900==0):
        print("limit exceeded, Pausing for 15 minutes")
        time.sleep(60*16)
		
ndf = pd.DataFrame(f)
ndf.to_csv(search+" results with profiles.csv",encoding="UTF-8-sig",index=False)

print("\n\nProfiles Scraped and Saved in CSV.\n\n")

print("Looking for keyword--"+keyword+"--in all profiles.\n\n")
user_stock=[]
for i in tqdm(range(0,len(ndf))):
    un = str(ndf['user'].iloc[i]).lower()
    ds = str(ndf['desription'].iloc[i]).lower()
    if keyword in un+ds:
        user_stock.append(ndf['user'].iloc[i])
        
print("\nNumber of Usernames found : ",len(user_stock))

print("\n\nSending DMs\n\n")

last=[]
temp=1
for i in tqdm(user_stock):
    try:
        user = api.get_user(i) 
        recipient_id = user.id_str
        text = text 
        direct_message = api.send_direct_message(recipient_id, text) 
        direct_message.message_create['message_data']['text'] 
        data = {
            'user' : i,
            'status' : 'Sent'
        }
        direct_message.message_create['message_data']['text'] 
        last.append(data)
    except:
        data = {
            'user' : i,
            'status' : 'Not sent'
        }
        last.append(data)
        continue
    if data['status']=='Sent':
        temp+=1
        if(temp%1000==0):
            print("limit exceeded, Pausing for 24 hours")
            time.sleep(60*60*60)
final = pd.DataFrame(last)        
print(dict(final.status.value_counts()))

