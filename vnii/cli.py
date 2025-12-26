'\nCommand-line interface for vnii\n\nSimple CLI to initialize vnstock license using API Key authentication.\n'
import sys,logging
from.core import VnstockInitializer
log=logging.getLogger(__name__)
def lc_init(debug=False):
	"\n    Initialize and verify vnstock license using API Key.\n\n    Args:\n        debug: Enable detailed logging (default: False)\n\n    Returns:\n        License info dict on success\n        \n    Raises:\n        SystemExit on authentication failure\n        \n    Example:\n        >>> license_info = lc_init(debug=True)\n        >>> print(license_info['tier'])\n        'premium'\n    ";C=debug
	if not log.handlers:D=logging.StreamHandler(sys.stdout);E=logging.Formatter('%(message)s');D.setFormatter(E);log.addHandler(D)
	if C:log.setLevel(logging.DEBUG);log.debug('vnii: Debug mode enabled')
	else:log.setLevel(logging.INFO)
	F=VnstockInitializer(target='vnstock')
	try:B=F.authenticate();G=B.get('user','Unknown');log.info(f"✅ Authentication successful: {G}");log.debug(f"License info: {B}");return B
	except SystemExit as A:log.error(f"❌ Authentication failed: {A}");raise
	except Exception as A:
		log.error(f"❌ Unexpected error: {A}")
		if C:import traceback as H;H.print_exc()
		raise SystemExit(str(A))