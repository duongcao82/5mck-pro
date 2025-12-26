_J='event_counts'
_I='total_processed'
_H='quantity'
_G='change'
_F='status'
_E='time'
_D='event_type'
_C='timestamp'
_B='data'
_A=None
import json
from typing import Dict,Any
from copy import deepcopy
FIELD_MAPPING={_D:'event',_C:_E,_B:_B,'id':'id','mc':'market','vol':'volume','value':'value',_E:_E,_F:_F,'accVol':'acc_volume','ot':'other','cIndex':'index','oIndex':'open_index','ptVol':'pt_volume','ptValue':'pt_value','fBVol':'foreign_buy_vol','fSVol':'foreign_sell_vol','fBVal':'foreign_buy_val','fSVal':'foreign_sell_val','up':'up','down':'down','ref':'unchanged','hsx_ce':'hsx_ceil','hsx_fl':'hsx_floor','hnx_ce':'hnx_ceil','hnx_fl':'hnx_floor','upcom_ce':'upcom_ceil','upcom_fl':'upcom_floor','vn30_ce':'vn30_ceil','vn30_fl':'vn30_floor','vn100_ce':'vn100_ceil','vn100_fl':'vn100_floor','sym':'symbol','lastPrice':'last_price','lastVol':'last_volume',_G:_G,'changePc':'change_pct','totalVol':'total_volume','hp':'high','lp':'low','ch':'change_flag','lc':'low_flag','ap':'avg_price','ca':'ceiling','sID':'session_id','side':'side','lot':'lot_size','bid':'bid_price','ask':'ask_price','oi':'open_interest','fBVol1':'foreign_buy_vol1','fSVol1':'foreign_sell_vol1','boardId':'board_id','marketId':'market_id','sequence':'seq','hashValue':'hash','transId':'trans_id','vol4':'volume_4','timeServer':'server_time','g1':'level_1','g2':'level_2','g3':'level_3','g4':'level_4','g5':'level_5','g6':'level_6','g7':'level_7','g8':'level_8','g9':'level_9','fSVolume':'foreign_sell_volume','fBValue':'foreign_buy_value','fSValue':'foreign_sell_value','group':'group','buyerID':'buyer_id','sellerID':'seller_id','price':'price',_H:_H,'cl':'color'}
def flatten_raw_data(raw_data:Dict[str,Any],field_mapping:Dict[str,Any]|_A=_A)->Dict[str,Any]:
	D=field_mapping;B=raw_data
	if D is _A:D=FIELD_MAPPING
	C={};G=B.get(_D,'unknown');C[_D]=G;A=B.get(_B,B)
	if isinstance(A,dict)and _B in A:A=A[_B]
	if G in['index','stock','stockps','board','boardps']:
		if _B in A:A=A[_B]
	for(E,F)in A.items():
		H=D.get(E,E)
		if E.startswith('g')and isinstance(F,str):_parse_price_level(F,E,C,D)
		else:C[H]=F
	if _C in B:C[_C]=B[_C]
	return C
def _parse_price_level(level_str:str,level_key:str,result:Dict[str,Any],field_mapping:Dict[str,str])->_A:
	E=level_str;C=level_key;A=result
	try:
		B=E.split('|')
		if len(B)>=3:D=C[1:];A[f"level_{D}_price"]=float(B[0]);A[f"level_{D}_volume"]=int(B[1]);A[f"level_{D}_side"]=B[2]
	except(ValueError,IndexError):F=field_mapping.get(C,C);A[F]=E
def flatten_raw_json_file(input_file:str,output_file:str,field_mapping:Dict[str,str]|_A=_A,limit:int|_A=_A)->Dict[str,Any]:
	K=limit;J=output_file;I='message';E=field_mapping;import csv
	if E is _A:E=FIELD_MAPPING
	with open(input_file,'r')as F:M=json.load(F)
	A=[];G={}
	for(N,B)in enumerate(M):
		if K and N>=K:break
		if isinstance(B.get(I),str)and B[I].startswith('42'):
			try:
				O=B[I][2:];C=json.loads(O)
				if isinstance(C,list)and len(C)>=2:H=C[0];P=C[1];Q={_D:H,_B:P,_C:B.get(_C)};R=flatten_raw_data(Q,E);A.append(R);G[H]=G.get(H,0)+1
			except json.JSONDecodeError:continue
	if A:
		D=set()
		for S in A:D.update(S.keys())
		D=sorted(list(D))
		with open(J,'w',newline='')as F:L=csv.DictWriter(F,fieldnames=D);L.writeheader();L.writerows(A)
	return{_I:len(A),_J:G,'output_file':J}
def get_field_mapping()->Dict[str,str]:return deepcopy(FIELD_MAPPING)
def update_field_mapping(custom_mapping:Dict[str,str])->_A:global FIELD_MAPPING;FIELD_MAPPING.update(custom_mapping)
def export_field_mapping_to_json(output_file:str)->_A:
	with open(output_file,'w')as A:json.dump(FIELD_MAPPING,A,indent=2)
def print_field_mapping_summary()->_A:
	A='=';print('\n'+A*80);print('FIELD NAME MAPPING CONFIGURATION');print(A*80)
	for(B,C)in sorted(FIELD_MAPPING.items()):
		if B!=C:print(f"  {B:20} → {C}")
	print(A*80+'\n')
if __name__=='__main__':
	import sys
	if len(sys.argv)>1:input_file=sys.argv[1];output_file=sys.argv[2]if len(sys.argv)>2 else'flattened_data.csv';print(f"Flattening {input_file} to {output_file}...");stats=flatten_raw_json_file(input_file,output_file);print(f"✓ Processed: {stats[_I]} messages");print(f"  Event types: {stats[_J]}")
	else:print_field_mapping_summary()