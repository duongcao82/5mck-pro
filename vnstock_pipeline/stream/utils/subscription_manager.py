from typing import List
VN30_STOCKS=['ACB','BCM','BID','CTG','DGC','FPT','GAS','GVR','HDB','HPG','LPB','MBB','MSN','MWG','PLX','SAB','SHB','SSB','SSI','STB','TCB','TPB','VCB','VHM','VIB','VIC','VJC','VNM','VPB','VRE']
STO_CONTRACTS=['41I1FB000','VN30F2512','41I1G3000','41I1G6000','41I2FB000','41I2FC000','41I2G3000','41I2G6000','41B5G3000','41B5G6000','GB05F2512','41BAG3000','41BAG6000','GB10F2512']
def get_hose_stocks()->List[str]:return VN30_STOCKS.copy()
def get_sto_contracts()->List[str]:return STO_CONTRACTS.copy()
def get_all_symbols()->tuple[List[str],List[str]]:return VN30_STOCKS.copy(),STO_CONTRACTS.copy()
if __name__=='__main__':print(f"\nVN30 Stocks ({len(VN30_STOCKS)}): {', '.join(VN30_STOCKS)}");sto_str=', '.join(STO_CONTRACTS);print(f"\nSTO Contracts ({len(STO_CONTRACTS)}): {sto_str}");hose_list=','.join(VN30_STOCKS);sto_list=','.join(STO_CONTRACTS);print('\n'+'='*70);print('REGS Messages (for WebSocket subscription):');print('='*70);print(f'\nHOSE: 42["regs","{{"action":"join","list":"{hose_list}"}}"]');print(f'\nSTO:  42["regs","{{"action":"join","list":"{sto_list}"}}"]');print('='*70)