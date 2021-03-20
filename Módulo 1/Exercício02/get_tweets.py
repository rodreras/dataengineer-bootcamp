import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

#Cadastrando as chaves de acesso
consumer_key = 'SECRET'
consumer_secret = 'SECRET'

access_token = 'SECRET'
access_token_secret = 'SECRET'

#Definir um arquivo de saída para armazenar os tweets coletados
agora = datetime.now().strftime("%Y-%m-%d-%H-%M-%m")
out = open(f"collected_tweets{agora}.txt",'w')

# Implementar uma classe para conexão com o Twitter
class MyListener(StreamListener):
    
    def on_data(self, data):
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        return True
    
    def on_error(self, status):
        print(status)
        
#Implementar a função MAIN

if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    stream = Stream(auth, l)
    stream.filter(track = ["Lula"])
