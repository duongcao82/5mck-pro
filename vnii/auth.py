'\nAPI Key Authentication for vnii\n\nSimple, focused authentication using Vnstock API keys only.\nGitHub authentication has been removed.\n\nDesign:\n  - Single authentication method: API Key\n  - Device registration via vnstock API\n  - Unified device tracking with vnai and installer\n'
_C='api_key.json'
_B='https://vnstocks.com'
_A='api_key'
import json,logging,os,requests
from pathlib import Path
from typing import Dict,Optional
log=logging.getLogger(__name__)
class APIKeyAuthStrategy:
	'\n    API Key-based authentication (only supported method)\n\n    Uses Vnstock system API key for device registration, license verification,\n    and package installation.\n    '
	def __init__(A,project_dir,api_base_url=_B):B=project_dir;A.project_dir=B;A.api_base_url=api_base_url.rstrip('/');A.api_key_path=B/_C;A.session=requests.Session();A.name='API Key'
	def get_credentials(A):
		'Get API key from file or environment';C=os.getenv('VNSTOCK_API_KEY')
		if C:B=f"[{A.name}] Using API key from environment variable";log.debug(B);return C
		if A.api_key_path.exists():
			try:
				with open(A.api_key_path,'r')as D:
					E=json.load(D);C=E.get(_A)
					if C and C.strip():B=f"[{A.name}] Using API key from api_key.json";log.debug(B);return C.strip()
			except(json.JSONDecodeError,ValueError)as F:B=f"[{A.name}] api_key.json is corrupted: {F}";log.warning(B)
		else:B=f"[{A.name}] api_key.json not found at: {A.api_key_path}";log.debug(B)
	def verify_credentials(B,credential):
		'Verify API key by registering device and checking license status';U='deviceLimit';T='unknown';S='devicesUsed';R='machine_info';Q='os_version';P='os_type';O='device_name';N=credential;M='tier';I='device_id'
		try:
			A=f"[{B.name}] Getting device information...";log.debug(A)
			try:from.device import get_device_info_for_api as V;D=V();A=f"[{B.name}] Using unified device module";log.debug(A)
			except ImportError as J:A=f"[{B.name}] device module not available: {J}\nPlease ensure vnii is properly installed.";log.error(A);raise RuntimeError(A)
			A=f"[{B.name}] Registering device...";log.debug(A);W={_A:N,I:D.get(I),O:D.get(O),P:D.get(P),Q:D.get(Q),R:D.get(R)};X='{}/api/vnstock/auth/device-register';C=B.session.post(X.format(B.api_base_url),json=W,timeout=30)
			if C.status_code!=200:
				E=C.json();K=E.get('error',f"HTTP {C.status_code}")
				if C.status_code==429:H=E.get(S,T);F=E.get(U,T);G=E.get(M,'your');Y=E.get('canReset',False);log.debug(f"[{B.name}] 429 Response: {E}");log.debug(f"[{B.name}] devicesUsed={H}, deviceLimit={F}, tier={G}");Z='\n💡 You can remove devices at: https://vnstocks.com/account'if Y else'';A=f"""❌ License Error: Device limit exceeded
   Your {G} plan allows {F} device(s) per OS.
   Currently registered: {H}/{F}
   {K}{Z}

Please remove unused devices or upgrade your subscription.""";raise SystemExit(A)
				elif C.status_code==403:A=f"❌ License Error: Subscription required\n   {K}\n\nPlease visit https://vnstocks.com/store to purchase a membership.";raise SystemExit(A)
				else:A=f"❌ License Error: Device registration failed\n   Status: {C.status_code}\n   Message: {K}\n\nPlease check your API key and try again.";raise SystemExit(A)
			L=C.json();G=L.get(M,'free');H=L.get(S,0);F=L.get(U,'unlimited');A=f"[{B.name}] Device registered successfully";log.debug(A);A=f"[{B.name}] Tier: {G}, Devices: {H}/{F}";log.debug(A);a=B._get_username(N);b=f"License recognized and verified. Tier: {G}";return{'status':b,'user':a,M:G,'devices_used':H,'device_limit':F,I:D.get(I),'auth_method':_A}
		except requests.exceptions.RequestException as J:A='Network error during license verification: {}\nPlease check your connection and try again.'.format(J);raise SystemExit(A)
	def _get_username(A,api_key):
		'Get username from API';C='Unknown'
		try:
			D={'Authorization':f"Bearer {api_key}"};E='{}/api/vnstock/user/profile';B=A.session.get(E.format(A.api_base_url),headers=D,timeout=10)
			if B.status_code==200:F=B.json();return F.get('username',C)
		except Exception as G:H=f"[{A.name}] Could not fetch username: {G}";log.debug(H)
		return C
def authenticate(project_dir,api_base_url=_B):
	'\n    Authenticate using API key.\n    \n    Args:\n        project_dir: Directory containing api_key.json\n        api_base_url: Base URL for vnstock API\n        \n    Returns:\n        License info dict on success\n        \n    Raises:\n        SystemExit on authentication failure\n    ';A=project_dir;B=APIKeyAuthStrategy(A,api_base_url);C=B.get_credentials()
	if not C:
		from.utils import is_google_colab as F;D=A/_C;E=f"""❌ License Error: No API key found.
   Expected location: {D}

Please either:
1. Set VNSTOCK_API_KEY environment variable, or
2. Create api_key.json file at: {D}
"""
		if F():E+='\n💡 For Google Colab:\n   Your api_key.json should be in Google Drive:\n   /content/drive/MyDrive/.vnstock/api_key.json\n   \n   Make sure Google Drive is mounted first!'
		raise SystemExit(E)
	return B.verify_credentials(C)