import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

#Cadastrando as chaves de acesso
consumer_key = 'Qx8NrwYeylu0xeuiPiizblKsv'
consumer_secret = 'APpja0FL7hjyWQjti0DchBs4x40QGsHDMBEqETnOoihb7UVPfe'

access_token = '745374787569061892-cqpkRowuhOEzRXjA3gwmpAXx2qQatWY'
access_token_secret = 'Q0oS9vAP0AHX7kCnR7iq9wWejq42WLRl5QZle3RlT8dsu'

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