_J='windows'
_I='https://ifin.tvsi.com.vn/'
_H='https://data.maybanktrade.com.vn'
_G='https://masboard.masvn.com'
_F='https://s.cafef.vn/lich-su-giao-dich-vnindex-3.chn'
_E='https://mkw.vndirect.com.vn'
_D='chrome'
_C='User-Agent'
_B='Origin'
_A='Referer'
import random
from vnstock.core.utils.user_agent import HEADERS_MAPPING_SOURCE,DEFAULT_HEADERS
from vnstock_data.core.utils.browser_profiles import USER_AGENTS
VDS_HEADERS={'Accept':'application/json, text/javascript, */*; q=0.01','Accept-Language':'en-US,en;q=0.9','Connection':'keep-alive','Content-Type':'application/x-www-form-urlencoded; charset=UTF-8','Sec-Fetch-Dest':'empty','Sec-Fetch-Mode':'cors','Sec-Fetch-Site':'same-origin','X-Requested-With':'XMLHttpRequest','sec-ch-ua-mobile':'?0','sec-ch-ua-platform':'"Windows"'}
HEADERS_MAPPING_SOURCE['VND']={_A:_E,_B:_E}
HEADERS_MAPPING_SOURCE['VDS']={_A:'https://livedragon.vdsc.com.vn/general/intradayBoard.rv',_B:'https://livedragon.vdsc.com.vn'}
HEADERS_MAPPING_SOURCE['FIALDA']={_A:'https://fwt.fialda.com/',_B:'https://fwt.fialda.com'}
HEADERS_MAPPING_SOURCE['FIINTRADE']={_A:'https://fiintrade.vn',_B:'https://fiintrade.vn/'}
HEADERS_MAPPING_SOURCE['FIDT']={_A:'https://portal.fidt.vn',_B:'https://portal.fidt.vn/'}
HEADERS_MAPPING_SOURCE['CAFEF']={_A:_F,_B:_F}
HEADERS_MAPPING_SOURCE['MAS']={_A:_G,_B:_G}
HEADERS_MAPPING_SOURCE['MBK']={_A:_H,_B:_H}
HEADERS_MAPPING_SOURCE['TVS']={_A:_I,_B:_I}
BROWSER_PROFILES={_D:{_C:'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'},'safari':{_C:'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/605.1.15 Version/16.3 Safari/605.1.15'},'coccoc':{_C:'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0 CocCocBrowser/123.0'},'firefox':{_C:'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'},'brave':{_C:'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Brave/120.0.0.0 Safari/537.36'},'vivaldi':{_C:'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Vivaldi/6.2.3105.58 Safari/537.36'}}
def get_headers(data_source:str='SSI',random_agent:bool=True,browser:str=_D,platform:str=_J)->dict:
	D=platform;C=browser;E=HEADERS_MAPPING_SOURCE.get(data_source.upper(),{});F=E.get(_A,'');G=E.get(_B,'')
	if random_agent:C=random.choice(list(USER_AGENTS.keys()));D=random.choice(list(USER_AGENTS[C].keys()))
	A=USER_AGENTS.get(C.lower(),{}).get(D.lower())
	if not A:
		A=USER_AGENTS.get(_D,{}).get(_J)
		if not A:
			for H in USER_AGENTS.values():
				if isinstance(H,dict):A=next(iter(H.values()));break
	B=DEFAULT_HEADERS.copy();B[_C]=A
	if F:B[_A]=F
	if G:B[_B]=G
	return B