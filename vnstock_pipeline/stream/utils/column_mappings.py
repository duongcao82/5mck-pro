_x='net_value'
_w='net_volume'
_v='sell_value'
_u='buy_value'
_t='buy_volume'
_s='foreign_net_volume'
_r='foreign_sell_volume'
_q='foreign_buy_volume'
_p='volume_4'
_o='volume_3'
_n='price_3'
_m='volume_2'
_l='price_2'
_k='volume_1'
_j='price_1'
_i='time_server'
_h='last_volume'
_g='last_price'
_f='trading_session'
_e='ask_volume_1'
_d='ask_price_1'
_c='bid_volume_1'
_b='bid_price_1'
_a='low_price'
_Z='high_price'
_Y='open_price'
_X='status'
_W='acc_vol'
_V='percent_change'
_U='sell_volume'
_T='total_value'
_S='time'
_R='value'
_Q='volume'
_P='change'
_O='side'
_N='total_volume'
_M='board_id'
_L='unchanged'
_K='declines'
_J='advances'
_I='hash_value'
_H='market_id'
_G='stock_id'
_F='trans_id'
_E='sequence'
_D='market_code'
_C='symbol'
_B='timestamp'
_A='event_type'
COLUMN_ORDER={'index':[_B,_A,'index_id',_D,_E,_I,_F,'current_index','open_index',_P,_V,'abs_change','percent_change_text',_Q,_R,_W,'value_ot',_J,_K,_L,_X,_S,'high_index','low_index','close_index','foreign_buy','foreign_sell'],'stock':[_B,_A,_C,_G,'current_price',_Y,'close_price',_Z,_a,_P,_V,_Q,_R,_W,'acc_value',_b,_c,_d,_e,_X,'flag'],'stockps':[_B,_A,_G,_C,_M,_H,_E,_I,_F,_f,'command','date','index','calculate','ff',_P,_g,_Y,_Z,_a,'average_price','change_percent','change_flag','low_flag','ceiling_actual',_h,_N,_S,_i,'color','level',_O,'session_id'],'soddlot':[_B,_A,_C,_G,_H,_D,_E,_I,_F,'ceiling_price','floor_price','reference_price',_g,'lot',_h,_b,_c,'bid_price_2','bid_volume_2','bid_price_3','bid_volume_3',_d,_e,'ask_price_2','ask_volume_2','ask_price_3','ask_volume_3'],'board':[_B,_A,_C,_G,_M,_H,_E,_I,_F,_O,_j,_k,'flag_1',_l,_m,'flag_2',_n,_o,'flag_3',_p,_i],'boardps':[_B,_A,_C,_G,_M,_H,_O,_E,_I,_F,_f,_j,_k,_l,_m,_n,_o,'price_4',_p,'price_5','volume_5','price_6','volume_6','price_7','volume_7','price_8','volume_8','price_9','volume_9','price_10','volume_10','volume_4_extra'],'aggregatemarket':[_B,_A,_D,_N,_T,'put_through_volume','put_through_value',_q,_r,'foreign_buy_value','foreign_sell_value',_s,'foreign_net_value',_J,_K,_L,'hsx_ceiling','hsx_floor','hnx_ceiling','hnx_floor','upcom_ceiling','upcom_floor','vn30_ceiling','vn30_floor','vn100_ceiling','vn100_floor'],'aggregateps':[_B,_A,'lot',_q,_r,_s,'bid_volume','ask_volume','open_interest'],'aggregatecw':[_B,_A,_D,_N,_T,_J,_K,_L],'aggregateetf':[_B,_A,_D,_N,_T,_J,_K,_L],'aggregateforeigngroup':[_B,_A,'group',_t,_U,_u,_v,_w,_x],'aggregateforeignmarket':[_B,_A,_D,_t,_U,_u,_v,_w,_x,'room','current_room'],'spt':[_B,_A,_C,'transaction_id',_M,_H,_E,_F,'transaction_type','color','price',_Q,_R,_S,_H,_O,'buyer','seller','foreign_buyer','foreign_seller','block_trade','order_id','firm_no'],'psfsell':[_B,_A,_C,_G,_U,'percent_sell','sell_price'],'regs':[_B,_A,'action','list']}
def get_column_order(event_type:str)->list:return COLUMN_ORDER.get(event_type,[])
def reorder_columns(data:dict,event_type:str)->dict:
	A=data;D=get_column_order(event_type);B={}
	for C in D:
		if C in A:B[C]=A[C]
	F=set(A.keys())-set(D)
	for E in sorted(F):B[E]=A[E]
	return B
def format_csv_columns(csv_path:str,output_path:str=None)->bool:
	G=False;D=True;B=csv_path;import os,pandas as J,logging as K;C=K.getLogger(__name__)
	try:
		if not os.path.exists(B):C.error(f"CSV file not found: {B}");return G
		A=J.read_csv(B)
		if A.empty:C.warning(f"CSV is empty: {B}");return D
		if _A in A.columns:E=A[_A].iloc[0]
		else:
			L=os.path.basename(B);H=L.replace('market_data_','').split('_')
			if len(H)>=2:E=H[0]
			else:C.warning(f"Cannot determine event type for {B}");return D
		F=get_column_order(E)
		if F:M=[B for B in F if B in A.columns];N=[A for A in A.columns if A not in F];O=M+N;A=A[O]
		I=output_path or B;os.makedirs(os.path.dirname(I)or'.',exist_ok=D);A.to_csv(I,index=G);C.info(f"Formatted CSV: {B} ({len(A.columns)} columns, {len(A)} rows, event_type={E})");return D
	except Exception as P:C.error(f"Error formatting CSV {B}: {P}");import traceback as Q;C.error(Q.format_exc());return G
def format_all_csvs_in_directory(directory:str,pattern:str='market_data_*.csv')->int:
	A=directory;import os,glob,logging as E;C=E.getLogger(__name__)
	try:
		D=glob.glob(os.path.join(A,pattern));B=0
		for F in D:
			if format_csv_columns(F):B+=1
		C.info(f"Formatted {B}/{len(D)} CSV files in {A}");return B
	except Exception as G:C.error(f"Error formatting CSVs in {A}: {G}");return 0