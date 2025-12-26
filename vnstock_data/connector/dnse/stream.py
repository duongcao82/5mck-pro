_D='application/json'
_C='Content-Type'
_B='matchQtty'
_A='matchPrice'
import requests,json,logging,random,time,os,csv
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import MQTTv5
from paho.mqtt.subscribeoptions import SubscribeOptions
import yaml
def append_tick_to_csv(tick_data,filename='tick_data.csv'):
	A=filename;C=['symbol',_A,_B,'time','side','session','low','open','lastUpdated','volume','close','type','high'];D=os.path.isfile(A)
	with open(A,'a',newline='')as E:
		B=csv.DictWriter(E,fieldnames=C)
		if not D:B.writeheader()
		B.writerow(tick_data)
with open('/content/drive/MyDrive/Colab Notebooks/config/dnse_creds.yaml')as f:creds=yaml.safe_load(f);username=creds['usr'];password=creds['pwd']
def dnse_auth(username,password):
	B='https://services.entrade.com.vn/dnse-user-service/api/auth';C=json.dumps({'username':username,'password':password});D={_C:_D};A=requests.request('POST',B,headers=D,data=C)
	if A.status_code==200:E=A.json()['token'];return E
def account_info(jwt_token):
	B='https://services.entrade.com.vn/dnse-user-service/api/me';C={_C:_D,'authorization':f"Bearer {jwt_token}"};A=requests.request('GET',B,headers=C)
	if A.status_code==200:return A.json()
jwt_token=dnse_auth(username,password)
investor_id=account_info(jwt_token)['investorId']
class Config:BROKER='datafeed-lts.dnse.com.vn';PORT=443;TOPICS='plaintext/quotes/derivative/OHLC/1/VN30F1M','plaintext/quotes/stock/tick/+';CLIENT_ID=f"python-json-mqtt-{random.randint(0,1000)}";USERNAME=investor_id;PASSWORD=jwt_token
class MQTTClient:
	def __init__(A):A.client=mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1,Config.CLIENT_ID,protocol=MQTTv5,transport='websockets');A.client.username_pw_set(Config.USERNAME,Config.PASSWORD);A.client.tls_set_context();A.client.ws_set_options(path='/wss');A.client.on_connect=A.on_connect;A.client.on_message=A.on_message;A.client.on_disconnect=A.on_disconnect
	def connect_mqtt(A):A.client.connect(Config.BROKER,Config.PORT,keepalive=120);return A.client
	def on_connect(A,client,userdata,flags,rc,properties=None):
		if rc==0:logging.info('Connected to MQTT Broker!');B=[(A,SubscribeOptions(qos=2))for A in Config.TOPICS];A.client.subscribe(B)
		else:logging.error(f"Failed to connect, return code {rc}")
	def on_disconnect(A,client,userdata,rc,properties=None):logging.info('Disconnected with result code: %s',rc)
	def on_message(B,client,userdata,msg):
		A=json.loads(msg.payload.decode())
		if _A in A and _B in A:
			try:A[_A]=float(A[_A]);A[_B]=float(A[_B]);append_tick_to_csv(A);logging.debug(f"Received tick data: {A}")
			except ValueError:logging.error('Invalid data format, skipping tick.')
def run():logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',level=logging.DEBUG);A=MQTTClient();B=A.connect_mqtt();B.loop_forever()
if __name__=='__main__':run()