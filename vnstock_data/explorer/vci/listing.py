_L='__typename'
_K="Tham số lang phải là 'vi' hoặc 'en'."
_J='Không tìm thấy dữ liệu. Vui lòng kiểm tra lại.'
_I='organ_name'
_H='en_'
_G='symbol'
_F='records'
_E='VCI'
_D='vi'
_C='VCI.ext'
_B=True
_A=False
from typing import Dict,Optional
from datetime import datetime
from vnstock.explorer.vci.const import _GROUP_CODE,_TRADING_URL,_GRAPHQL_URL
import json,pandas as pd
from vnstock.core.utils.parser import camel_to_snake
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.transform import drop_cols_by_pattern,reorder_cols
from vnstock_data.core.utils.client import send_request
from vnstock_data.core.utils.user_agent import get_headers
from vnai import agg_execution
logger=get_logger(__name__)
class Listing:
	def __init__(A,random_agent:Optional[bool]=_A,show_log:Optional[bool]=_A):
		B=show_log;A.data_source=_E;A.base_url=_TRADING_URL;A.headers=get_headers(data_source=A.data_source,random_agent=random_agent);A.show_log=B
		if not B:logger.setLevel('CRITICAL')
	@agg_execution(_C)
	def all_symbols(self,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B)->Dict:
		A=self.symbols_by_exchange(show_log=show_log,to_df=_B);A=A.query('type == "STOCK"').reset_index(drop=_B);A=A[[_G,_I]]
		if to_df:return A
		else:B=A.to_json(orient=_F);return B
	@agg_execution(_C)
	def symbols_by_industries(self,lang:str=_D,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):
		D=show_log
		if lang not in[_D,'en']:raise ValueError(_K)
		C='{"query":"{\\n  CompaniesListingInfo {\\n    ticker\\n    organName\\n    enOrganName\\n    icbName3\\n    enIcbName3\\n    icbName2\\n    enIcbName2\\n    icbName4\\n    enIcbName4\\n    comTypeCode\\n    icbCode1\\n    icbCode2\\n    icbCode3\\n    icbCode4\\n    __typename\\n  }\\n}\\n","variables":{}}';C=json.loads(C);B=send_request(url=_GRAPHQL_URL,headers=self.headers,method='POST',payload=C,show_log=D)
		if not B:raise ValueError(_J)
		if D:logger.info(f"Truy xuất thành công dữ liệu danh sách phân ngành icb.")
		A=pd.DataFrame(B['data']['CompaniesListingInfo']);A.columns=[camel_to_snake(A)for A in A.columns];A=A.drop(columns=[_L]);A=A.rename(columns={'ticker':_G});A.source=_E
		if lang==_D:A=drop_cols_by_pattern(A,[_H])
		else:A=A.drop(columns=[_I,'icb_name2','icb_name3','icb_name4']);A.columns=[A.replace(_H,'')for A in A.columns]
		if to_df:return A
		else:B=A.to_json(orient=_F);return B
	@agg_execution(_C)
	def symbols_by_exchange(self,lang:str=_D,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):
		D='exchange';C=show_log
		if lang not in[_D,'en']:raise ValueError(_K)
		E=self.base_url+'/price/symbols/getAll';B=send_request(url=E,headers=self.headers,method='GET',payload=None,show_log=C)
		if not B:raise ValueError(_J)
		if C:logger.info(f"Truy xuất dữ liệu thành công cho {len(B)} mã.")
		A=pd.DataFrame(B);A.columns=[camel_to_snake(A)for A in A.columns];A=A.rename(columns={'board':D});A=reorder_cols(A,[_G,D,'type'],position='first');A=A.drop(columns=['id'])
		if lang==_D:A=drop_cols_by_pattern(A,[_H])
		else:A=A.drop(columns=[_I,'organ_short_name']);A.columns=[A.replace(_H,'')for A in A.columns]
		if to_df:A.source=_E;return A
		else:B=A.to_json(orient=_F);return B
	@agg_execution(_C)
	def industries_icb(self,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):
		D=show_log;C='{"query":"query Query {\\n  ListIcbCode {\\n    icbCode\\n    level\\n    icbName\\n    enIcbName\\n    __typename\\n  }\\n  CompaniesListingInfo {\\n    ticker\\n    icbCode1\\n    icbCode2\\n    icbCode3\\n    icbCode4\\n    __typename\\n  }\\n}","variables":{}}';C=json.loads(C);B=send_request(url=_GRAPHQL_URL,headers=self.headers,method='POST',payload=C,show_log=D)
		if not B:raise ValueError(_J)
		if D:logger.info(f"Truy xuất thành công dữ liệu danh sách phân ngành icb.")
		A=pd.DataFrame(B['data']['ListIcbCode']);A.columns=[camel_to_snake(A)for A in A.columns];A=A.drop(columns=[_L]);A=A[['icb_name','en_icb_name','icb_code','level']];A.source=_E
		if to_df:return A
		else:B=A.to_json(orient=_F);return B
	@agg_execution(_C)
	def symbols_by_group(self,group:str='VN30',show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):
		D=show_log;C=group
		if C not in _GROUP_CODE:raise ValueError(f"Invalid group. Group must be in {_GROUP_CODE}")
		E=self.base_url+f"/price/symbols/getByGroup?group={C}";A=send_request(url=E,headers=self.headers,method='GET',payload=None,show_log=D)
		if D:logger.info(f"Truy xuất thành công dữ liệu danh sách mã CP theo nhóm.")
		B=pd.DataFrame(A)
		if to_df:
			if not A:raise ValueError('JSON data is empty or not provided.')
			B.source=_E;return B[_G]
		else:A=B.to_json(orient=_F);return A
	@agg_execution(_C)
	def all_future_indices(self,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):return self.symbols_by_group(group='FU_INDEX',show_log=show_log,to_df=to_df)
	@agg_execution(_C)
	def all_government_bonds(self,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):return self.symbols_by_group(group='FU_BOND',show_log=show_log,to_df=to_df)
	@agg_execution(_C)
	def all_covered_warrant(self,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):return self.symbols_by_group(group='CW',show_log=show_log,to_df=to_df)
	@agg_execution(_C)
	def all_bonds(self,show_log:Optional[bool]=_A,to_df:Optional[bool]=_B):return self.symbols_by_group(group='BOND',show_log=show_log,to_df=to_df)