_C='direct'
_B='GET'
_A=None
import requests,json,random
from typing import Dict,Any,Optional,Union,List
from enum import Enum
from pydantic import BaseModel
from vnstock.core.utils.logger import get_logger
logger=get_logger(__name__)
class ProxyConfig(BaseModel):proxy_list:Optional[List[str]]=_A;proxy_mode:'ProxyMode'='try';request_mode:'RequestMode'=_C;hf_proxy_url:Optional[str]=_A
logger=get_logger(__name__)
class ProxyMode(Enum):TRY='try';ROTATE='rotate';RANDOM='random';SINGLE='single'
class RequestMode(Enum):DIRECT=_C;PROXY='proxy';HF_PROXY='hf_proxy'
_current_proxy_index=0
HF_PROXY_URLS=['https://autonomous-it-proxy-vci.hf.space/proxy']
def build_proxy_dict(proxy_url:str)->Dict[str,str]:A=proxy_url;return{'http':A,'https':A}
def get_proxy_by_mode(proxy_list:List[str],mode:ProxyMode)->str:
	B=mode;A=proxy_list;global _current_proxy_index
	if not A:raise ValueError('Proxy list is empty')
	if B==ProxyMode.SINGLE:return A[0]
	elif B==ProxyMode.RANDOM:return random.choice(A)
	elif B==ProxyMode.ROTATE:C=A[_current_proxy_index%len(A)];_current_proxy_index+=1;return C
	else:return A[0]
def create_hf_proxy_payload(url:str,headers:dict,method:str=_B,payload=_A)->dict:return{'url':url,'headers':headers,'method':method,'payload':payload}
def send_request(url:str,headers:Dict[str,str],method:str=_B,params:Optional[Dict]=_A,payload:Optional[Union[Dict,str]]=_A,show_log:bool=False,timeout:int=30,proxy_list:Optional[List[str]]=_A,proxy_mode:Union[ProxyMode,str]=ProxyMode.TRY,request_mode:Union[RequestMode,str]=RequestMode.DIRECT,hf_proxy_url:Optional[str]=_A)->Dict[str,Any]:
	M=hf_proxy_url;J=proxy_list;I=timeout;H=headers;G=request_mode;F=method;E=url;D=payload;C=params;B=proxy_mode;A=show_log
	if isinstance(B,str):
		try:B=ProxyMode(B)
		except ValueError:raise ValueError(f"Invalid proxy_mode: {B}")
	if isinstance(G,str):
		try:G=RequestMode(G)
		except ValueError:raise ValueError(f"Invalid request_mode: {G}")
	if A:
		logger.info(f"{F.upper()} request to {E} (mode: {G.value})")
		if C:logger.info(f"Params: {C}")
		if D:logger.info(f"Payload: {D}")
	if G==RequestMode.HF_PROXY:
		if B==ProxyMode.TRY and len(HF_PROXY_URLS)>1:
			K=_A
			for N in HF_PROXY_URLS:
				try:
					if A:logger.info(f"Trying HF proxy: {N}")
					return send_request_hf_proxy(E,H,F,C,D,I,N)
				except ConnectionError as L:
					K=L
					if A:logger.warning(f"HF proxy {N} failed: {L}")
					continue
			raise ConnectionError(f"All HF proxies failed. Last error: {K}")
		else:
			if len(HF_PROXY_URLS)>1:O=get_proxy_by_mode(HF_PROXY_URLS,B)
			else:O=M or HF_PROXY_URLS[0]
			if A:logger.info(f"Using HF proxy: {O}")
			return send_request_hf_proxy(E,H,F,C,D,I,O)
	elif G==RequestMode.PROXY:
		if not J:raise ValueError('proxy_list is required for PROXY mode')
		def S(url):A=url;return isinstance(A,str)and A.startswith('http')and'hf.space'in A
		if all(S(A)for A in J):
			M=J[0]
			if A:logger.warning(f"Detected Hugging Face proxy in proxy_list, switching to HF_PROXY mode with url: {M}")
			return send_request_hf_proxy(E,H,F,C,D,I,M)
		if B==ProxyMode.TRY:
			K=_A
			for P in J:
				try:
					if A:logger.info(f"Trying proxy: {P}")
					Q=build_proxy_dict(P);return send_request_direct(E,H,F,C,D,I,Q)
				except ConnectionError as L:
					K=L
					if A:logger.warning(f"Proxy {P} failed: {L}")
					continue
			raise ConnectionError(f"All proxies failed. Last error: {K}")
		else:
			R=get_proxy_by_mode(J,B);Q=build_proxy_dict(R)
			if A:logger.info(f"Using proxy ({B.value} mode): {R}")
			return send_request_direct(E,H,F,C,D,I,Q)
	else:
		if A:logger.info('Sending direct request (no proxy)')
		return send_request_direct(E,H,F,C,D,I,proxies=_A)
def send_request_hf_proxy(url:str,headers:Dict[str,str],method:str=_B,params:Optional[Dict]=_A,payload:Optional[Union[Dict,str]]=_A,timeout:int=30,hf_proxy_url:str=_A)->Dict[str,Any]:
	E='application/json';C=params;B=method;A=hf_proxy_url
	if not A:A=HF_PROXY_URLS[0]
	D=url
	if C and B.upper()==_B:F='&'.join([f"{A}={B}"for(A,B)in C.items()]);D=f"{url}?{F}"
	G=create_hf_proxy_payload(url=D,headers=headers,method=B,payload=payload);H={'Content-Type':E,'Accept':E};return send_request_direct(url=A,headers=H,method='POST',payload=G,timeout=timeout)
def send_request_direct(url:str,headers:Dict[str,str],method:str=_B,params:Optional[Dict]=_A,payload:Optional[Union[Dict,str]]=_A,timeout:int=30,proxies:Optional[Dict[str,str]]=_A)->Dict[str,Any]:
	F=proxies;E=timeout;D=headers;A=payload
	try:
		if method.upper()==_B:B=requests.get(url,headers=D,params=params,timeout=E,proxies=F)
		else:
			if A is not _A:
				if isinstance(A,dict):C=json.dumps(A)
				elif isinstance(A,str):C=A
				else:raise ValueError('Payload must be either a dictionary or a raw string.')
			else:C=_A
			B=requests.post(url,headers=D,data=C,timeout=E,proxies=F)
		if B.status_code!=200:raise ConnectionError(f"Failed to fetch data: {B.status_code} - {B.reason}")
		return B.json()
	except requests.exceptions.RequestException as H:G=f"API request failed: {str(H)}";logger.error(G);raise ConnectionError(G)
def reset_proxy_rotation():global _current_proxy_index;_current_proxy_index=0
def send_direct_request(url:str,headers:Dict[str,str],**A):return send_request(url,headers,request_mode=RequestMode.DIRECT,**A)
def send_proxy_request(url:str,headers:Dict[str,str],proxy_list:List[str],**A):return send_request(url,headers,proxy_list=proxy_list,request_mode=RequestMode.PROXY,**A)
def send_hf_proxy_request(url:str,headers:Dict[str,str],**A):return send_request(url,headers,request_mode=RequestMode.HF_PROXY,**A)