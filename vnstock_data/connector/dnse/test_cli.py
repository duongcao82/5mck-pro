_D='application/json'
_C='Content-Type'
_B='matchQtty'
_A='matchPrice'
import os,argparse,requests,json,csv,logging,random,time,yaml,ast
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import MQTTv5
from paho.mqtt.subscribeoptions import SubscribeOptions
def append_tick_to_csv(tick_data,filename='tick_data.csv'):
	A=filename;C=['symbol',_A,_B,'time','side','session','low','open','lastUpdated','volume','close','type','high'];D=os.path.isfile(A)
	with open(A,'a',newline='')as E:
		B=csv.DictWriter(E,fieldnames=C)
		if not D:B.writeheader()
		B.writerow(tick_data)
def yaml_creds(path:str):
	try:
		with open(path,'r')as C:
			A=yaml.safe_load(C)
			if not isinstance(A,dict):raise ValueError('The credentials file format is incorrect. Expected a dictionary.')
			D=A['usr'];E=A['pwd']
		return D,E
	except FileNotFoundError:raise FileNotFoundError(f"The file {path} does not exist.")
	except yaml.YAMLError as B:raise ValueError(f"Error parsing the YAML file: {B}")
	except KeyError as B:raise KeyError(f"Missing required key in the credentials file: {B}")
class Auth:
	def __init__(A,username:str,password:str):A.username=username;A.password=password;A.token=A._get_token();A.investor_id=A._get_investorid()
	def _get_token(B):
		C='https://services.entrade.com.vn/dnse-user-service/api/auth';D=json.dumps({'username':B.username,'password':B.password});E={_C:_D};A=requests.request('POST',C,headers=E,data=D)
		if A.status_code==200:return A.json()['token']
		else:A.raise_for_status()
	def _get_investorid(B):
		C='https://services.entrade.com.vn/dnse-user-service/api/me';D={_C:_D,'authorization':f"Bearer {B.token}"};A=requests.request('GET',C,headers=D)
		if A.status_code==200:E=A.json();return E['investorId']
		else:A.raise_for_status()
class Config:
	def __init__(A,creds_path:str,topics:tuple):A.user_name,A.password=yaml_creds(creds_path);A.auth=Auth(A.user_name,A.password);A.BROKER='datafeed-lts.dnse.com.vn';A.PORT=443;A.TOPICS=topics;A.CLIENT_ID=f"python-json-mqtt-ws-sub-{random.randint(0,1000)}";A.USERNAME=A.auth.investor_id;A.PASSWORD=A.auth.token;A.FIRST_RECONNECT_DELAY=1;A.RECONNECT_RATE=2;A.MAX_RECONNECT_COUNT=12;A.MAX_RECONNECT_DELAY=60
class MQTTClient:
	def __init__(A,config:Config):A.config=config;A.client=mqtt_client.Client(client_id=A.config.CLIENT_ID,protocol=MQTTv5,transport='websockets');A.client.username_pw_set(A.config.USERNAME,A.config.PASSWORD);A.client.tls_set_context();A.client.ws_set_options(path='/wss');A.client.on_connect=A.on_connect;A.client.on_message=A.on_message;A.client.on_disconnect=A.on_disconnect;A.FLAG_EXIT=False
	def connect_mqtt(A):A.client.connect(A.config.BROKER,A.config.PORT,keepalive=120);return A.client
	def on_connect(A,client,userdata,flags,rc,properties=None):
		if rc==0 and client.is_connected():logging.info('Connected to MQTT Broker!');B=[(A,SubscribeOptions(qos=2))for A in A.config.TOPICS];A.client.subscribe(B)
		else:logging.error(f"Failed to connect, return code {rc}")
	def on_disconnect(A,client,userdata,rc,properties=None):
		logging.info('Disconnected with result code: %s',rc);C,B=0,A.config.FIRST_RECONNECT_DELAY
		while C<A.config.MAX_RECONNECT_COUNT:
			logging.info('Reconnecting in %d seconds...',B);time.sleep(B)
			try:client.reconnect();logging.info('Reconnected successfully!');return
			except Exception as D:logging.error('%s. Reconnect failed. Retrying...',D)
			B*=A.config.RECONNECT_RATE;B=min(B,A.config.MAX_RECONNECT_DELAY);C+=1
		logging.info('Reconnect failed after %s attempts. Exiting...',C);A.FLAG_EXIT=True
	def on_message(B,client,userdata,msg):
		A=json.loads(msg.payload.decode())
		if _A in A and _B in A:
			try:A[_A]=float(A[_A]);A[_B]=float(A[_B]);append_tick_to_csv(A);logging.debug(f"Received tick data: {A}")
			except ValueError:logging.error('Invalid data format, skipping tick.')
def run(creds_path,topics):
	A=topics;logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',level=logging.DEBUG)
	if isinstance(A,str):
		try:
			A=ast.literal_eval(A)
			if not isinstance(A,list):raise ValueError('Topics should be a list of strings.')
		except(ValueError,SyntaxError)as C:raise ValueError(f"Failed to parse topics: {C}")
	if not all(isinstance(A,str)for A in A):raise ValueError('All topics must be strings.')
	B=tuple(A);logging.debug(f"Parsed topics: {B}");D=Config(creds_path,B);E=MQTTClient(D);F=E.connect_mqtt();F.loop_forever()
if __name__=='__main__':parser=argparse.ArgumentParser(description='MQTT Client CLI App');parser.add_argument('creds_path',type=str,help='Path to the creds.yaml file');parser.add_argument('--topics',type=str,nargs='+',default=['plaintext/quotes/derivative/OHLC/1/VN30F1M','plaintext/quotes/stock/tick/+'],help='List of topics to subscribe to');args=parser.parse_args();run(args.creds_path,args.topics)