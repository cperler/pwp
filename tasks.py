from utils import get_exchanges, get_eod_quotes, send_mail, get_symbols_by_exchange, get_isins_by_exchange,\
	get_fundamentals_by_symbol, xignite_token
from models import Pwp_Pwp_Stocks, Pwp_Pwp_Xignite_Stocks, ERROR, SUCCESS, SKIPPED
import datetime
import urllib
from utils import get_fundamental_file, get_price_file
import zlib
from urls import request, get_eod_quote_for_date

def missing_isins(as_of_date):
	symbols = ['TWIGA.DAR','RAD.XNYS','FUM.XAIM','ORCL.XNAS','GLW.XNYS','GS.XNYS','TARO.F.PINX','KITD.PINX','CEC.LUSE','CMVT.XNAS','PSS.XNYS','CROX.XNYS','NGC.XTSE','GM.XNYS','COST.XNYS','PTGI.XNYS','CRF.XASX','HGSI.XNAS','TCAP.XNYS','CJES.XNYS','BOX.XNYS','DEJ.XTSE','MMYT.XNYS','PZG.XASE','PETR4.XBSP',' DB.XNYS','GT.XNYS','KIRK.XNYS','APKT.XNAS','GEF.XNYS','MRK.XFRA','ULTA.XNYS','KSWS.XNAS','MMR.XNYS','MRVC.PINX',' HSBA.XLON','YMI.XTSE','POE.XTSE','ORC.B.XTSE','TAT.XASE','EXPR.XNYS','GR.XNYS','SINA.XNYS','EC.XNYS','VIV.XNYS','SCMR.XNAS','VRUS.XNAS','XTA.XLON','IEM.XASX','GMODELO C.XBER','NSU.XFRA','VIVT3.XBSP','HMSP.XJKT','VTS.XASX','BAACEZ.SEP','PP.XPAR','HNZ.XNYS','BEI.XFRA','007974.OSE','6146252.BDL','TELMEX L.XBER','NXY.XTSE','MAN.XFRA','513377.XBOM','FI.XMIL','SMB.PSE','ASA.JSE','CBE.XNYS','GRUPOAVAL.BVC','RDCD3.XBSP','A46.XSES','EBK.XFRA','1SKF401E.BSSE','006981.OSE','PRGO.XNAS','OGXP3.XBSP','MEO.XFRA','CNH.XNYS','QTEL.DSM','OCIC.CASE','MDY.ARCX','MIIC.F.PINX','QRN.XASX','BANE.RTS','BOGOTA.BVC','IRAO.XMIC','MMBM.XMIC','CREDITC1.BVL','ANTARCHILE.SNSE','HNR1.XFRA','BAAKOMB.SEP','VMED.XNAS','5405.XTKS','PKBA.XMIC','004817.JASDAQ','004528.OSE','BAATELEC.SEP','BMC.XNAS','EEEK.XATH','EFMS.F.PINX','BOSS.XFRA','500900.XBOM','GTO.XPAR','INVERARGOS.BVC','AMMB.KLSE','VT.XTSE','KD8.XFRA','EDN.XMIL','IPL.UN.XTSE','NYB.XNYS','ARR.XPAR','CVH.XNYS','PXP.XNYS','PRQ.XTSE','FUR.XAMS','FINBN.IBSE','G1A.XFRA','SUN.XNYS','000527.XSHE','A004940.XKRX','IJP.XASX','IDEAL B-1.XBER','CONTINC1.BVL','009783.OSE','BRAP3.XBSP','ROSB.XMIC','NMTC.KWSE','IZZ.XASX','CSCG.XLON','AGP.XNYS','006645.OSE','PUM.XFRA','LSI.XNYS','AGS.XLON','ARBA.XNAS','TGM.XFRA','BYAN.XJKT','MINERA.SNSE','DENIZ.IBSE','SAI.XNYS','601901.XSHG','004536.OSE','RAH.XNYS','UTDI.XFRA','FIE.XFRA','200039.XSHE','006963.OSE','CPN.SET','AMIL3.XBSP','LTO.XMIL','HOT.XFRA','NEL.TTSE','CALICHERAA.SNSE','BCE.CBSE','BSK.WSE','CBK.KWSE','SCOTIAC1.BVL','OHLB3.XBSP','ABK.KWSE','IMN.XTSE','WMTCL.SNSE','PWA.XFRA','SNOS.XMIC','1VS.XFRA','CLS1.XFRA','GDI.XNYS','BTPN.XJKT','THBK.ASE','TUB.XFRA','INDI.XAIM','CHILECTRA.SNSE','VVAR3.XBSP','GARFA.IBSE','CFAO.XPAR','SFD.XNYS','IFS.BVL','FLUX.XBRU','CBEE3.XBSP','SMMA.XJKT','SHAW.XNYS','FIRSTBANK.NGSE','ACM.A.XTSE','SGL.XFRA','53.XHKG','KYN.XNYS','KBHL.XCSE','UEMLAND.KLSE','8815.XTKS','EXG.XNYS','TELEFBC1.BVL','MNV6.XFRA','EMTK.XJKT','OCCIDENTE.BVC','678.XHKG','DNP.XNYS','LEC.XFRA','SANTANGRUP.SNSE','CPMK.PINX','000799.XSHE','RBN.XNYS','RBL.TTSE','PBN.XTSE','WXS.XNYS','BOND.ARCX','PHYS.ARCX','SCL.A.XTSE','MINSURI1.BVL','KZRU.RTS','EMOB.CASE','HAKN.XOME','DTG.XNYS','UKUZ.XMIC','QSFT.XNAS','INTERBC1.BVL','AHCS.DSM','SPLV.ARCX','F&N.KLSE','LMCEMNT.KLSE','NSGB.CASE','WWG.XFRA','TEBNK.IBSE','SIMP.XJKT','PMZ.UN.XTSE','AMRT.XJKT','ITW.XASX','BVL.CCSE','002155.XSHE','ALTE.XNAS','CPNO.XNAS','TDV.D.CCSE','PX.PSE','TIE.XNYS','500676.XBOM','TOWR.XJKT','BPV.CCSE','DCC.XDUB','MOLYMET.SNSE','SZG.XFRA','OGKE.XMIC','ZABA-R-A.ZGSE','FAX.XASE','MVZ.B.CCSE','DJP.ARCX','ACG.XNYS','LACIMAI1.BVL','LIF.UN.XTSE','SMAR.XJKT','500376.XBOM','NUV.XNYS','CMJ.XASX','WRC.XNYS','SGOK.UKR','BTO.XMAD','000602.XSHE','IRGZ.XMIC','EVV.XASE','TUI1.XFRA','VKW.WBAG','003682.GTSM','GEN.XNYS','MRX.XNYS','002065.XSHE','COV.XAIM','BBVACOL.BVC','CLP.XNYS','RHM.XFRA','HLCM.PSE','VSMO.XMIC','KROT11.XBSP','MVV1.XTRA','PASUR.SNSE','ABUK.CASE','ALBH.BAX','DAM.BDM','SHS.XNYS','PROMIGAS.BVC','ABQK.DSM','3789.XTKS','HHFA.XFRA','LSIP.XJKT','MSRS.XMIC','ZIL2.XFRA','GWI1.XFRA','ARCT.XNAS','PRX.XNYS','GET.XNYS','GIB.XFRA','RASP.XMIC','CLT.XTSE','DOU.XFRA','CYMI.XNAS','UTDPLT.KLSE','VTGK.XMIC','NMTP.XMIC','LUFK.XNAS','INCARSO B-.XBER','BSAN.XSWX','FLTR.XLON','INVS.XJKT','JTEL.ASE','GRT.XTSE','HRUM.XJKT','AJ1.XSES','KWS.XFRA','CLH.T.BDM','MPHB.KLSE','SBTT.TTSE','BLKI.B.XOTC','PBZ-R-A.ZGSE','PGF.ARCX','PHK.XNYS','KLCCP.KLSE','601608.XSHG','EDEGELC1.BVL','KENT.IBSE','AMCL.TTSE','OBS.WBAG','RAKBANK.ADX','AFFIN.KLSE','CEGR3.XBSP','C20.XSES','MMXM3.XBSP','BRD.BVB','APAT.RTS','2012.XHKG','NFJ.XNYS','WUW.XTRA','GIGANTE *.XBER','000522.XSHE','CLT.CBSE','BROCALC1.BVL','MRH.BUL','AGR.WBAG','CBD.DFM','NKNC.XMIC','ZWC.WSE','BAATABAK.SEP','OLT.OOTC','FRAGUA B.XBER','BATELCO.BAX','000780.XSHE','OSXB3.XBSP','002590.XSHE','HNB.OOTC','GLOB.TASE','500302.XBOM','BAAUNIPE.SEP','GIAA.XJKT','UBP.PSE','GEMS.XJKT','CEM.XNYS','007947.OSE','300104.XSHE','GLORIAI1.BVL','P8Z.XSES','UTF.XNYS','GAB.KLSE','CSQ.XNAS','FDD.XFRA','GXI.XFRA','GTBO.XJKT','3893.XTKS','GEUPEC B.XBER','001911.OSE','PCBC.XNAS','VNR.XNYS','YAL.XASX','AEG.JSE','3669.XHKG','006457.OSE','MNG.CBSE','AUTO.XJKT','PXP.PSE','006008.GTSM','NIO.XNYS','ETY.XNYS','ET.XNYS','533206.NSEI','506395.XBOM','BRIO.BASE','OGKC.XMIC','QNCD.DSM','LUSURC1.BVL','MFGS.XMIC','500770.XBOM','SUD AMER-A.SNSE','BPHA3.XBSP','008310.SASE','000927.XSHE','ALPHA.PSE','RA.XNYS','TKFEN.IBSE','KESC.KASE','CEY.XLON','FINN.PINX','SSC.SET','BRI.ENXTLS','CCAP.XAMS','ACIBD.IBSE','CMA.CBSE','TUPY3.XBSP','DVB.XFRA','AWF.XNYS','533273.XBOM','PTY.XNYS','ALNU.XMIC','BYW.XFRA','GDV.XNYS','SMDC.PSE','JDAS.XNAS','531162.XBOM','SEMU.F.PINX','BDJ.XNYS','GGC.XNYS','500093.XBOM','ENAEX.SNSE','ACOM.XNAS','CRM.A.CCSE','002004.XSHE','002292.XSHE','5949.XTKS','BBN.XNYS','532839.XBOM','BRMS.XJKT','PAVREIT.KLSE','ORO BLANCO.SNSE','MUL.XAIM','BUMI.XLON','532779.XBOM','BNB.XTSE','JQC.XNYS','008697.JASDAQ','BCI.CBSE','KASSETS.KLSE','JOPH.ASE','532178.XBOM','BJLAND.KLSE','PPC.XNYS','TLAB.XNAS','PBG.XTSE','PWK.JSE','GIM.XNYS','LRI.PSE','532627.XBOM','HERO.XJKT','ONB.XNYS','KNXA.XNYS','GTU.UN.XTSE','BBTN.XJKT','008367.OSE','NWR.XLON','LEO.XFRA','SBD.XAIM','8361.XTKS','500469.XBOM','KRB.WSE','UNACEMC1.BVL','OGKA.XMIC','WAA.CBSE','AKSEN.IBSE','SKRN.XMIC','GQHL.XOTC','000697.XSHE','9787.XTKS','7518.XTKS','CBO.XTSE','EVN.XASX','INVERCAP.SNSE','IDKM.XJKT','EVT.XNYS','008219.OSE','SILM.RTS','TSPC.XJKT','THR.XBRU','2299.XHKG','TDY.PSE','BKI.XNYS','CIMBT.SET','000059.XSHE','RQI.XNYS','9065.XTKS','EDD.XNYS','LCB.PSE','NTG.XNYS','ULEVER.KASE','IRAX.CASE','HRG.XTSE','RCB.PSE','BEL.PSE','ETW.XNYS','LEDO-R-A.ZGSE','2270.XTKS','MEGA.XJKT','ESSO.SET','HBMO.MSM','VIVA.XJKT','EIT.UN.XTSE','MAPI.XJKT','8251.XTKS','MSM.KLSE','500483.XBOM','002293.XSHE','FPT.JSE','5191.XTKS','2068.XHKG','500092.XBOM','IL0.XDUB','SRT.XFRA','MGCR.XLON','MGTS.XMIC','603077.XSHG','UNAC.XMIC','002167.XSHE','JDG.JSE','KRAS.XJKT','TERI3.XBSP','JPS.XNYS','TYG.XNYS','AMOC.CASE','UNG.ARCX','ENGI3.XBSP','8328.XTKS','000852.XSHE','CPD.XTSE','TF.SET','AVAZ.XMIC','500315.XBOM','008069.GTSM','TESB.XBRU','002503.XSHE','WHA.XAMS','SSBR3.XBSP','PSSI.XNAS','000762.XSHE','GGN.XASE','KRSG.XMIC','000932.XSHE','PNB.PSE','008046.XTAI','LLXL3.XBSP','PDI.XNYS','9627.XTKS','002233.XSHE','ESVAL-C.SNSE','000968.XSHE','533207.XBOM','A064420.XKRX','532480.XBOM','002122.XSHE','SCAR3.XBSP','NPM.XNYS','OISHI.SET','KYAK.XNAS','HW.XTSE','MGLU3.XBSP','CCPR3.XBSP','POL.KASE','ASCEL.IBSE','4186.XTKS','DSSA.XJKT','ADMF.XJKT','1TAT01DE.BSSE','8130.XTKS','4553.XTKS','8185.XTKS','ETG.XNYS','000861.XSHE','GAB.XNYS','TGMA3.XBSP','532544.XBOM','VOS.XFRA','MYI.XNYS','GFINTER O.XBER','EKHOLDING.KWSE','9536.XTKS','SMSAAM.SNSE','BEGY.XMIC','ABMM.XJKT','LSP.XLON','NOG.XASE','CNU.NZSE','601010.XSHG','008244.OSE','TY.XNYS','LPSB3.XBSP','BRAU.XJKT','7757.XTKS','000939.XSHE','WAC.XFRA','PER.XNYS','SPALI.SET','SDR.XNYS','JHD.XAIM','BJBR.XJKT','001696.XSHE','8415.XTKS','603366.XSHG','SLCE3.XBSP','SLU.GYSE','002612.XSHE','EISP.OTCNO','002461.XSHE','UFNC.RTS','VAPORES.SNSE','STB1.XFRA','534091.XBOM','000877.XSHE','BORN.XJKT','KZTK.KAS','532276.XBOM','ADX.XNYS','MAGG3.XBSP','2284.XTKS','500477.XBOM','IGR.XNYS','HBM.XFRA','000506.XSHE','001710.XTAI','002154.XSHE','1258.XHKG','4203.XTKS','002168.XSHE','FPH.PSE','3865.XTKS','VILLAS.BVC','HSB.MTSE','000918.XSHE','GORO.XASE','601100.XSHG','EDELNOC1.BVL','532388.XBOM','3050.XTKS','NISP.XJKT','H49.XSES','CTC-N-0000.COSE','A041510.XKRX','CHG.XNYS','A053800.XKRX','CUPRUM.SNSE','PEET.XNAS','A082270.XKRX','000517.XSHE','002181.XSHE','KFC.KLSE','CLF.XTSE','PFV.XFRA','NCEM.CASE','AOD.XNYS','TDF.XNYS','4708.XTKS','002327.XSHE','WCO.TTSE','PHH2.XFRA','NPP.XNYS','SUI.JSE','ASYAB.IBSE','SOP.KLSE','8281.XTKS','EIB.HOSE','LPI.KLSE','002638.XSHE','CMMT.KLSE','6005.XTKS','BESALCO.SNSE','FNBB.BSM','PIA.XMIL','1VUB02AE.BSSE','NPI.XNYS','VLID3.XBSP','MDN.XTRA','ADI.CBSE','8194.XTKS','000830.XSHE','BGZ.WSE','000417.XSHE','VALUEGF O.XBER','4924.XTKS','CWI.WBAG','TISCO.SET','002225.XSHE','STB.HOSE','002337.XTAI','002384.XTAI','200541.XSHE','KYE.XNYS','000860.XSHE','TEG.XFRA','7817.XTKS','HRHO.CASE','ARB.XNYS','JPC.XNYS','BOE.XNYS','KUO B.XBER','EIM.XASE','5726.XTKS','SAC.JSE','BPH.WSE','000962.XSHE','MGI.XNYS','SIE.BVC','BJCORP.KLSE','PGH.BVMT','MCCS.DSM','002662.XSHE','002006.XTAI','532418.XBOM','ZCN.XTSE','GPPL.CASE','8425.XTKS','IJMPLNT.KLSE','PAL.PSE','YKSGR.IBSE','BT.BVMT','008589.OSE','IFSI.A.XNAS','CHY.XNAS','GBM O.XBER','VTA.XNYS','008545.OSE','KOZAA.IBSE','7984.XTKS','3048.XTKS','002534.XSHE','7994.XTKS','BARKA.BAX','CLWR.XNAS','A051370.XKRX','507878.XBOM','ORL.TASE','002450.XTAI','CLAS B.XOME','PMG.XTSE','NBG6.XFRA','7739.XTKS','9956.XTKS','BIPORT.KLSE','7740.XTKS','IGD.XNYS','1334.XTKS','AELP3.XBSP','KHCD.DSM','MCF.XASE','2685.XTKS','000501.XSHE','FAR.OOTC','VVR.XNYS','500674.XBOM','7522.XTKS','2264.XTKS','BISA3.XBSP','002651.XSHE','5407.XTKS','PB.SET','HIX.XNYS','008010.SASE','SWDY.CASE','000655.XSHE','4541.XTKS','BRIN3.XBSP','BRISA.IBSE','FDC.PSE','RVT.XNYS','3098.XTKS','INDY.XJKT','QL.KLSE','RBP.JSE','MNPZ.XMIC','GJTL.XJKT','NUHCM.IBSE','532531.XBOM','KIP.NZSE','KMAZ.XMIC','ELECMETAL.SNSE','000823.XSHE','001304.XTAI','IZMDC.IBSE','532915.XBOM','KK.SET','002447.XSHE','IFN.XNYS','8088.XTKS','UAB.ADX','8397.XTKS','CEB.PSE','CNC.GYSE','KSB.XFRA','TST.XMAD','PUCOBRE-A.SNSE','6508.XTKS','PPR.XNYS','IBPO.XAIM','8110.XTKS','1979.XTKS','3240.XTKS','ACB.HASTC','ODINSA.BVC','6773.XTKS','PALTEL.PLSE','SHELL.KLSE','USA.XNYS','000426.XSHE','AKCNS.IBSE','6376.XTKS','RIE.OOTC','532885.XBOM','CHI.XNAS','EUO.ARCX','A3TV.XMAD','UNPZ.RTS','500877.XBOM','DLADY.KLSE','YULC.XLON','MARL.ICSE','FESH.XMIC','ASGN.XNAS','AOX.XFRA','AMAG.WBAG','1SLN01AE.BSSE','NQU.XNYS','590071.XBOM','ATN.JSE','7412.XTKS','RET.A.XTSE','VGM.XNYS','KONYA.IBSE','RALS.XJKT','ZON.ENXTLS','ICB.DSE','KU2.XFRA','533398.XBOM','RIO.XTSE','002226.XSHE','HOL.CBSE','WBO.JSE','O9E.XSES','EVEN3.XBSP','006286.XTAI','ESRS.CASE','RNP.XNYS','ALTIN.IBSE','KAP.JSE','002845.XTAI','TRKCM.IBSE','8345.XTKS','PNL.XLON','DAL.XFRA','AT.PSE','8346.XTKS','9204741.BDL','MEDIA.KLSE','DIRR3.XBSP','PROJ.XNAS','000900.XSHE','FFC.XNYS','000680.XSHE','001717.XTAI','SYC.JSE','NIKL.PSE','5946.XTKS','8276.XTKS','006005.XTAI','ETV.XNYS','EMI.JSE','GAM.XNYS','A112040.XKRX','000988.XSHE','TEI.XNYS','MSSV.XMIC','000888.XSHE','000816.XSHE','JDAN.XCSE','000809.XSHE','000926.XSHE','002320.XSHE','CAT.JSE','522275.XBOM','000652.XSHE','SIX2.XFRA','002242.XSHE','003105.GTSM','IBI.XNYS','IMCH3.XBSP','BGY.XNYS','JSN.XNYS','KCEM.KWSE','004932.GTSM','WIW.XNYS','CWB.ARCX','002313.XSHE','A122900.XKRX','601388.XSHG','007050.SASE','CLC.XTSE','PPT.XNYS','NOEJ.XFRA','000719.XSHE','COP.XFRA','CABLE.KWSE','ALAFCO.KWSE','002588.XSHE','A78.XSES','8278.XTKS','000566.XSHE','NYR.XBRU','VBPS.WBAG','8283.XTKS','FBR.JSE','JLIF.XLON','TGKA.XMIC','BREB.XBRU','AFG.OOTC','STR.PSE','7581.XTKS','006269.XTAI','ENMA3B.SOMA','500290.XBOM','GMA7.PSE','500034.XBOM','TPC.SET','002358.XSHE','SALFACORP.SNSE','533107.XBOM','601677.XSHG','NWRS.MSM','000863.XSHE','002360.XTAI','PCT.XLON','5440.XTKS','001212.SASE','008008.XTAI','BELN.XSWX','PML.XNYS','3028.XTKS','002078.XSHE','MHY.UN.XTSE','CXS.XNYS','1277.XHKG','000989.XSHE','USBN.XMIC','000850.XSHE','GBT.A.XTSE','CRBC.XNAS','BOV.MTSE','HSPLANT.KLSE','GRTY.A.PINX','2590.XTKS','500260.XBOM','MFT.NZSE','BARCLAYS.BSM','300267.XSHE','MRKC.XMIC','000667.XSHE','002601.XSHE','TLGF.SET','8360.XTKS','002889.XTAI','6330.XTKS','CFG.XMAD','VASTN.XAMS','IBTC.NGSE','CGX.XASX','AKFEN.IBSE','ANO.NZSE','002123.XSHE','RSID3.XBSP','01002T.XTAI','A004010.XKRX','SCFR.PINX','KTCG.WBAG','CSR.CBSE','DEX.XTRA','TCSA3.XBSP','IRET.XNAS','002037.XSHE','MIN.XNYS','STAR.KLSE','532505.XBOM','JTIASA.KLSE','ETB.BVC','004105.GTSM','4921.XTKS','ABA.XFRA','AHL1V.HLSE','A003670.XKRX','002489.XSHE','NBOB.MSM','601636.XSHG','003532.XTAI','UN6A.XDUB','ESSBIO-C.SNSE','HENX.XBRU','984.XHKG','002478.XSHE','ETJ.XNYS','TWSPLNT.KLSE','532368.XBOM','3593.XTKS','8543.XTKS','002463.XSHE','VHS.XNYS','A096530.XKRX','TECN3.XBSP','5727.XTKS','DOAS.IBSE','OSSR.ICSE','FCSS.XLON','002186.XSHE','LEW.JSE','TPIPL.SET','BIAT.BVMT','4985.XTKS','RPO.XLON','008806.OSE','6581.XTKS','LWDB.XLON','6641.XTKS','EAD.XASE','MRCB.KLSE','DYHOL.IBSE','BGR.XNYS','LLIS3.XBSP','004010.SASE','008422.XTAI','EFM.XLON','3880.XTKS','ARCHER.OOTC','MYD.XNYS','TWS.KLSE','UOADEV.KLSE','AWP.XNYS','9793.XTKS','ALQURAIN.KWSE','SCUN.XFRA','SVH.SET','A078160.XKRX','ABL.KASE','NML.TTSE','002497.XSHE','JSE.JSE','8279.XTKS','002665.XSHE','000631.XSHE','8344.XTKS','1115.XHKG','005820.GTSM','003211.GTSM','GODREJPROP.NSEI','GALI.BASE','ROYT.XNYS','8566.XTKS','002545.XTAI','TINS.XJKT','000636.XSHE','8078.XTKS','8982.XTKS','9477.XTKS','A056190.XKRX','4023.XTKS','KLED.XOME','532947.XBOM','AP.SET','PDT.XNYS','533286.XBOM','3738.XTKS','NORTEGRAN.SNSE','CIH.CBSE','OPNT.XNAS','SEM.WBAG','TS.B.XTSE','000066.XSHE','SFBT.BVMT','002812.XTAI','FLI.PSE','MOAR3.XBSP','TAER.XMIC','GHL.TTSE','SIP.XBRU','9869.XTKS','000919.XSHE','601798.XSHG','SOROUH.ADX','GENP.XPAR','SUSS.XNAS','MAKE.PSE','SGJ.JMSE','4044.XTKS','3608.XTKS','FENER.IBSE','2001.XTKS','TPIA.XJKT','NET.WSE','BAR.XBRU','601208.XSHG','CMB.XBRU','UNIB SDB.XOME','BALAT.IBSE','KIPA.IBSE','2815.XTKS','MVF.XASE','5301.XTKS','002585.XSHE','BTZ.XNYS','9744.XTKS','8956.XTKS','002849.XTAI','TLH.XTSE','8986.XTKS','002511.XTAI','WHS.NZSE','TGKI.XMIC','AQUACHILE.SNSE','JAM.XLON','SKBNK.IBSE','JCY.KLSE','ANHYT.IBSE','PPY.XTSE','MVC.XMAD','SMBL.XNAS','OPTR.XNAS','BDT.XFRA','DPG.XNYS','006134.NSE','300315.XSHE','TOL.PSE','3635.XTKS','WCT.KLSE','CTR.XNYS','000529.XSHE','BLUF.XOTC','AFX.JSE','HTD.XNYS','EXE.XTSE','BECL.SET','1179080.BDL','000722.XSHE','SELEC.IBSE','000759.XSHE','500084.XBOM','002190.XSHE','9945.XTKS','2005.XHKG','BKS.WBAG','8522.XTKS','GUJFLUORO.NSEI','002506.XSHE','SBM.MUSE','MUT.XLON','002847.XTAI','6816.XTKS','VNT.SET','PLIN.XJKT','001234.XTAI','APLN.XJKT','HOT.TASE','3110.XTKS','000789.XSHE','PCN.XNYS','CDZ.XTSE','002400.JASDAQ','9861.XTKS','NCV.XNYS','002466.XSHE','DFDS.XCSE','BFK.XNYS','000897.XSHE','300077.XSHE','300224.XSHE','000931.XSHE','HOMEX *.XBER','ITE.JSE','002276.XSHE','500630.XBOM','9010.XTKS','GPG.XLON','002070.XSHE','300004.XSHE','002855.XTAI','TVO.SET','IVT.JSE','4626.XTKS','9661.XTKS','INKP.XJKT','ERAA.XJKT','CRUZBLANCA.SNSE','BKSL.XJKT','50290077.BDL','ERC.XASE','526881.XBOM','2322.XTKS','UIE.XCSE','200418.XSHE','DOCK.OOTC','DPM.HOSE','NTB.BH.BER','LCA1.XFRA','AB.PSE','ARZ.XTSE','002025.XSHE','002230.SASE','ALEX.CASE','ZAP.WSE','300185.XSHE','FERREYC1.BVL','6651.XTKS','FAST.XJKT','DCC.SET','6140.XTKS','4041.XTKS','1028.XHKG','TPIS3.XBSP','007040.SASE','LAS CONDES.SNSE','MASISA.SNSE','LITRAK.KLSE','7274.XTKS','002574.XSHE','005449.OSE','ALBRK.IBSE','TGN.BVB','002392.XSHE','1893.XTKS','GEO.XMIL','002400.XSHE','MUC.XNYS','002016.XSHE','2602.XTKS','001907.XTAI','002458.XTAI','002528.XSHE','LDP.XNYS','002650.XSHE','RUE.XNAS','001214.SASE','BLW.XNYS','000050.XSHE','NMA.XNYS','SOMS.MSM','9792.XTKS','BCX.XNYS','FSD.XNYS','000987.XSHE','NMO.XNYS','5541.XTKS','OIZ.XDUB','300337.XSHE','ACTV.XNYS','500125.XBOM','EAST.CASE','6436.XTKS','500575.XBOM','SID.CBSE','MEDIQ.XAMS','002263.XSHE','A082740.XKRX','3141.XTKS','FXAI.PINX','AVN.XAIM','VQ.XNYS','QGRI.DSM','8056.XTKS','9427.XTKS','002389.XSHE','MBB.HOSE','PFN.XNYS','HEXA.XJKT','ZEH.XSWX','003149.XTAI','SYSR.XOME','1833.XTKS','002135.XSHE','NMDC.ADX','BIPI.XJKT','002393.XTAI','8584.XTKS','002288.XSHE','3397.XTKS','8051.XTKS','TEO1L.NSEL','FMIC.PSE','BEIJ B.XOME','000511.XSHE','532129.XBOM','CMO.BDM','4548.XTKS','005371.GTSM','RCS.XMIL','2211.XTKS','523457.XBOM','4201.XTKS','PEO.XNYS','A081660.XKRX','ISLAMIBANK.DSE','000036.XSHE','002535.XSHE','1146.XHKG','000930.XSHE','8174.XTKS','A078520.XKRX','NCB.KLSE','4917.XTKS','7238.XTKS','1379.XTKS','533274.XBOM','2726.XTKS','BKI.SET','TSH.KLSE','7616.XTKS','7239.XTKS','COMB-N-000.COSE','007716.JASDAQ','PZQA.XDUB','MLP.XFRA','002608.XTAI','SUCE.CASE','NLCS.DSM','GUBRF.IBSE','005483.GTSM','1762.XTKS','HAG.HOSE','RAM.SET','8600.XTKS','000951.XSHE','003406.XTAI','PECB.PSE','BON.OOTC','SAM.CBSE','002006.XSHE','BJTM.XJKT','8140.XTKS','002548.XTAI','4526.XTKS','532514.XBOM','5393.XTKS','MUI.XNYS','CLA B.XOME','DLEDF.XAMS','000078.XSHE','000935.XSHE','CDM.CBSE','000531.XSHE','300134.XSHE','NKX.XASE','300187.XSHE','002539.XSHE','GRO.OOTC','000600.XSHE','GMARTI *.XBER','300309.XSHE','000627.XSHE','JULPHAR.ADX','EMIS.XAIM','000518.XSHE','TPCG.XNAS','8342.XTKS','2681.XTKS','TGKJ.XMIC','006324.JASDAQ','002306.XSHE','008217.OSE','3432.XTKS','OCE.JSE','8841.XTKS','EVS.XBRU','000683.XSHE','CARS-N-000.COSE','002474.XSHE','SUPREMEIND.NSEI','HPS.XNYS','003091.SASE','MCOT.SET','COSH.PINX','002010.XSHE','002511.XSHE','6104.XTKS','002327.XTAI','002332.XSHE','002336.XSHE','BWPT.XJKT','002340.SASE','KSL.SET','TIMECOM.KLSE','VLTR.XNAS','3148.XTKS','1332.XTKS','002656.XSHE','4471.XTKS','BCF.XNYS','002042.XSHE','MARR.XMIL','4551.XTKS','EFT.XNYS','8153.XTKS','ADRS-R-A.ZGSE','000099.XSHE','CELL.XNAS','ALK B.XCSE','9830.XTKS','300064.XSHE','KMF.XNYS','002170.XSHE','007279.OSE','000777.XSHE','EMO.XNYS','7729.XTKS','DUTI.XJKT','CIMSA.IBSE','HRI.XLON','002138.XSHE','9934.XTKS','NDRO.XNYS','MMT.XTSE','005534.XTAI','002176.XSHE','2659.XTKS','CORAREI1.BVL','000561.XSHE','002238.XSHE','6282.XTKS','WEHB.XBRU','002600.XSHE','RILBA.XCSE','BLU.JSE','HPI.XNYS','A014620.XKRX','SBM.CBSE','7231.XTKS','MMU.XNYS','VESTA.UKR','8022.XTKS','A121440.XKRX','8133.XTKS','OLB.XFRA','ISD.XNYS','SPRT.XLON','002407.XSHE','533151.XBOM','NZF.XASE','009436.JASDAQ','GAZ.CBSE','A023160.XKRX','KNZM-R-A.ZGSE','FMF.WSE','GTC.WSE','3087.XTKS','GKSR.XNAS','500488.XBOM','WEB.PSE','300128.XSHE','TISI.XNYS','MINEROS.BVC','002833.XTAI','QSR.KLSE','QIMD.DSM','BBRK3.XBSP','001503.XTAI','PROTECCION.BVC','NPT.XNYS','002079.XSHE','300156.XSHE','5481.XTKS','7220.XTKS','000616.XSHE','STA.SET','002867.GTSM','532654.XBOM','APSA.BASE','004628.JASDAQ','AFK.OOTC','NCBJ.JMSE','6101.XTKS','PRSG.RTS','WZR.XTSE','BSPB.XMIC','5741.XTKS','009940.XTAI','IPEKE.IBSE','BWO.OOTC','JII.XLON','WWH.XLON','008311.SASE','KTM.WBAG','RBW.JSE','SHANG.KLSE','8958.XTKS','A015750.XKRX','TAL1T.TLSE','SHOW3.XBSP','MRCH.XLON','000007.XSHE','7287.XTKS','BOS.ADX','002561.XSHE','2201.XTKS','NAD.XNYS','SODA.IBSE','VID.XMAD','590086.XBOM','SIB.ADX','HVPE.XLON','MFL.XNYS','DAB.XLON','GEOY.XNAS','9873.XTKS','1377.XTKS','1417.XTKS','003090.SASE','002345.XSHE','300101.XSHE','FMO.XNYS','CRES.BASE','8527.XTKS','MRKP.XMIC','1950.XTKS','004150.SASE','CII.XNYS','002838.XTAI','002220.XSHE','512199.XBOM','002210.SASE','LEO.XNYS','BEST.XJKT','HUBC.KASE','NFP.XNYS','GEO B.XBER','003272.JASDAQ','3788.XHKG','VKI.XASE','JTINTER.KLSE','1969.XTKS','000973.XSHE','000752.XSHE','000592.XSHE','EGIS.BUSE','OYOG.XNAS','002489.XTAI','NQM.XNYS','HGM.XAIM','000068.XSHE','TLV.BVB','5738.XTKS','EASTW.SET','VKQ.XNYS','002398.XSHE','2796783.BDL','DI.UN.XTSE','BAZA3.XBSP','9605.XTKS','AST.XMIL','002093.XSHE','500378.XBOM','GRC.XASE','NHC.XASE','004080.SASE','JFR.XNYS','7157.XTKS','524804.XBOM','6986.XTKS','MYN.XNYS','1941.XTKS','SGY.XTSE','KNRI11.XBSP','002449.XTAI','NMC.XLON','BCH.SET','000993.XSHE','533177.XBOM','8173.XTKS','KLOV.XOME','CRDN.XNAS','GLO.XASE','000407.XSHE','IHI.MTSE','001313.XTAI','SSIA.XJKT','000717.XSHE','SPB.XFRA','002436.XSHE','004966.GTSM','UBN.NGSE','NZR.NZSE','005907.XTAI','1766.XTKS','PZ.NGSE','AZA.XOME','002683.XSHE','002255.XSHE','002607.XTAI','E&O.KLSE','AMWAY.KLSE','BANVIDA.SNSE','ECILC.IBSE','DUCP.XOTC','6310.XTKS','TRLG.XNAS','300068.XSHE','FMBL.XOTC','AMATA.SET','3151.XTKS','NQI.XNYS','8362.XTKS','000708.XSHE','002033.XSHE','8182.XTKS','VIEI.OTCNO','8879.XTKS','002391.XSHE','SGRO.XJKT','000659.XSHE','ZAR.XFRA','000516.XSHE','9430.XTKS','511288.XBOM','6996.XTKS','6454.XTKS','002283.XSHE','300195.XSHE','000571.XSHE','300127.XSHE','VAN.XBRU','000608.XSHE','000159.XSHE','UTG.XASE','7756.XTKS','002298.XSHE','009939.XTAI','NISTI.XAMS','UST.ARCX','A122870.XKRX','KLNMA.IBSE','IZOCM.IBSE','004270.SASE','000886.XSHE','MNT.XTSE','MONEX B.XBER','JTP.XNYS','IRSA.BASE','MMT.XNYS','9511.XTKS','PEY1.XTRA','7447.XTKS','2004.XTKS','200011.XSHE','NBB.XNYS','KTF.XNYS','001409.XTAI','SMI.CBSE','VIGR3.XBSP','Q CPO.XBER','300186.XSHE','ADDT B.XOME','002157.XSHE','500008.XBOM','JGT.XNYS','ACHL.XAIM','008508.OSE','BSET.XLON','603399.XSHG','PMO.XNYS','BEEF3.XBSP','SGP.PSE','FACIL.KWSE','004001.SASE','002048.XSHE','002372.XSHE','002083.XSHE','004040.SASE','POS.KLSE','000301.XSHE','BGHL.XAMS','ASCA.XNAS','002501.XSHE','009957.GTSM','ORMT.TASE','003376.XTAI','002481.XSHE','003001.SASE','531213.XBOM','SHF.XNYS','002628.XSHE','5701.XTKS','6135.XTKS','SPG.JSE','7545.XTKS','CASC.XNYS','3201.XTKS','000905.XSHE','008030.SASE','001319.XTAI','007485.NSE','5451.XTKS','A060310.XKRX','002469.XSHE','CVAL.XMIL','002278.XSHE','500710.XBOM','ENC.XMAD','KBW.XNYS','1722.XTKS','MCA.XNYS','002564.XSHE','5BT.BUL','9749.XTKS','ENGRO.KASE','RMYI.PINX','MING.OOTC','UEM.XLON','TRGYO.IBSE','CGL.XTSE','7864.XTKS','002538.XSHE','532873.XBOM','SIRI.SET','006285.XTAI','002597.XSHE','300188.XSHE','MBK.SET','XLG.ARCX','MCR.XNYS','FASW.XJKT','EFR.XNYS','LIAB.XOME','002270.SASE','LPZ.PSE','002547.XSHE','003035.XTAI','9069.XTKS','WABCO-TVS.NSEI','8160.XTKS','532121.XBOM','601218.XSHG','5310.XTKS','EFOODS.KASE','002545.XSHE','8387.XTKS','002080.XSHE','APGN.XSWX','300119.XSHE','7937.XTKS','002897.GTSM','000822.XSHE','000687.XSHE','WBSN.XNAS','300289.XSHE','006932.OSE','4694.XTKS','NPX.XNYS','002551.XSHE','HLNG.OOTC','7483.XTKS','ECONB.XBRU','BPC.XFRA','BBKP.XJKT','9746.XTKS','200429.XSHE','7867.XTKS','000514.XSHE','HGRE11.XBSP','INH.XFRA','532805.XBOM','SOFF.OOTC','002543.XSHE','DIG.XLON','NQS.XNYS','TYY.XNYS','BPAT.BASE','IZN.XTSE','LAS.A.XTSE','6767.XTKS','002504.XTAI','000090.XSHE','002460.XSHE','BIZIM.IBSE','ITM.XMIL','002059.XTAI','SLTL-N-000.COSE','002068.XSHE','EMGS.OOTC','AMER.XAIM','HANA.SET','006271.XTAI','300332.XSHE','AD.XTSE','EKO.OOTC','EEEL3.XBSP','PSE.PSE','QH.SET','SIOFF.OOTC','002567.XSHE','DSI.DFM','8343.XTKS','002390.XSHE','PAJ.XPAR','009941.XTAI','005970.JASDAQ','GDG.XAIM','002254.XSHE','SCHO.XCSE','CNIA.CBSE','002194.XSHE','590001.XBOM','002218.XSHE','STANLY.SET','300054.XSHE','004210.SASE','5857.XTKS','MQY.XNYS','002449.XSHE','AAV.SET','OV8.XSES','A036830.XKRX','002476.XSHE','124.XHKG','8871.XTKS','MELR.LJSE','ROTI.XJKT','NCZ.XNYS','2220.XTKS','A031430.XKRX','8392.XTKS','002637.XTAI','4282.XTKS','002376.XTAI','8973.XTKS','OCOI.MSM','002905.XTAI','HGBS11.XBSP','ALT.JSE','002483.XSHE','DOF.OOTC','002554.XSHE','002208.XTAI','000410.XSHE','4958.XTKS','MLCFM.XPAR','500040.XBOM','EOS.XNYS','002387.XSHE','8237.XTKS','ROCK.PSE','601028.XSHG','MEDC.XJKT','7976.XTKS','532638.XBOM','002767.JASDAQ','RAIVV.HLSE','SDP.XLON','URBI *.XBER','002177.XSHE','1946.XTKS','OLG.XFRA','4928.XTKS','002171.XSHE','002104.XTAI','7541.XTKS','KHEL.XMIC','9946.XTKS','008312.SASE','OTKAR.IBSE','002790.JASDAQ','CTI.SNSE','300045.XSHE','BNP.WSE','PAM.JSE','002566.XSHE','8530.XTKS','002589.XSHE','BUKI-N-000.COSE','004725.XTAI','G33.XSES','002100.XSHE','1SRA001E.BSSE','ANACM.IBSE','BERNAS.KLSE','BEIA B.XOME','A006730.XKRX','JPI.XNYS','EW.PSE','TCP.JSE','LAT1V.HLSE','007564.JASDAQ','S&P.SET','DEXB.XBRU','5812.XTKS','MFB.XNYS','009936.OSE','DUV.XBRU','006010.SASE','533122.XBOM','ORB.WSE','8050.XTKS','4711.XTKS','APAG.F.XNAS','VMO.XNYS','8703.XTKS','ZV.XMIL','001229.XTAI','FRE.NZSE','UMCCA.KLSE','SKBN.TASE','1008.XHKG','ACTI.XOME','A078340.XKRX','SPI.SET','RODA.XJKT','000713.XSHE','NN B.XOME','SPC.SET','BFZ.XNYS','PXT.XTSE','4914.XTKS','7224.XTKS','000594.XSHE','ABOB.MSM','6905.XTKS','000950.XSHE','5423.XTKS','PHT.XNYS','6796.XTKS','300020.XSHE','1366.XHKG','003046.JASDAQ','REZT.XOME','000912.XSHE','DUF.XNYS','002313.XTAI','LMA.XTSE','9715.XTKS','PVA.XMAD','TPOU.XLON','002001.SASE','BATBC.DSE','JD..XLON','000882.XSHE','LEDE.BASE','AEONCR.KLSE','A085660.XKRX','SOCOVESA.SNSE','LHBANK.SET','000916.XSHE','300229.XSHE','500219.XBOM','200029.XSHE','002439.XSHE','VIXY.ARCX','SVXY.ARCX','CONG.ARCX','GRPC.ARCX','NORW.ARCX','SCHZ.ARCX','NKY.ARCX','MORT.ARCX','TDTT.ARCX','MOAT.ARCX','SOIL.ARCX','AXDI.ARCX','ALD.ARCX','VIXM.ARCX']

	f = open('missing.tsv', 'w')
	f.write('symbol\tisin\tmessage\r')
	for symbol in symbols:
		isin = '?'
		message = ''
		try:
			response = request(get_eod_quote_for_date % (symbol, 'Symbol', as_of_date, xignite_token))
			security = response.get('Security', None)
			message = response.get('Message', None)
			sec_message = response.get('Security Message', '')
			if message is None: message = sec_message
			if security is not None: isin = security.get('ISIN', '?')
			
			if message != '':
				print symbol, message			
		except:
			isin = 'not_available_from_xignite'
	
		f.write('%s\t%s\t%s\r' % (symbol, isin, message))
	f.flush()
	f.close()
	
def get_fundamental_files(as_of_date, include_isins=False):
	download_files(as_of_date, get_fundamental_file, 'fundamentals.csv')
	if include_isins == True:
		f1 = open('fundamentals.csv', 'r')
		f2 = open('fundamentals_with_isins.csv', 'w')
		split_content = f1.read().split('\r')
		f2.write('%s,ISIN\r' % split_content[0])
		for line in split_content[1:]:
			if line.strip() != '':
				tokens = line.split(',')
				symbol = tokens[1]
				exchange = tokens[3]
				market_cap = tokens[4]
				if market_cap is None or market_cap == '0':
					print 'Skipping %s' % symbol
					continue
				
				isin = ''
				try:
					identifier_with_exchange = '%s.%s' % (symbol, exchange)		
					response = request(get_eod_quote_for_date % (identifier_with_exchange, 'Symbol', as_of_date, xignite_token))
					if response.get('Message') is None:
						isin = response['Security']['ISIN']
					else:
						print 'No ISIN available for %s: %s' % (symbol, response.get('Message', ''))
				except Exception as e:
					print 'Error retrieving ISIN for %s: %s' % (symbol, e)
				print 'Writing %s, %s, %s' % (symbol, isin, exchange) 
				f2.write('%s,%s\r' % (line, isin))
		f2.flush()
		f1.close()
		f2.close()
	
def get_price_files(as_of_date):
	download_files(as_of_date, get_price_file, 'prices.csv')

def download_files(as_of_date, download_method, file_name):
	exchanges = get_exchanges()
	data = []
	for exchange in exchanges:
		try:
			response = download_method(exchange, as_of_date)
			ba = bytearray(response.read())
			if data == []:
				data.extend(zlib.decompress(bytes(ba), 15+32).split('\n')[:])
			else:
				data.extend(zlib.decompress(bytes(ba), 15+32).split('\n')[1:])
		except Exception as e:
			pass
	f = open(file_name, 'w')
	f.write(''.join(data))
	f.flush()
	f.close()	
	return data

def get_fundamentals_for_symbol(symbol, identifier_type):
	results = get_fundamentals_by_symbol(symbol, identifier_type)
	if results and 'Fundamentals' in results:
		records = results['Fundamentals']
		data = {}
		if records is not None:
			for record in records:
				key = record['Type']
				if record['Value'] != '':
					try:
						value = float(record['Value'])
					except:
						print 'Unable to convert to float: %s' % record
						value = 0
					data[key] = value
		return data
	return {}

def get_symbols_for_exchange(exchange_code, asset, as_of_date):
	results = get_symbols_by_exchange(exchange_code, asset, as_of_date)
	if results:
		records = results['ArrayOfIdentifierRecords']
		if records:
			return records
	return []
	
def get_isins_for_exchange(exchange_code, asset, as_of_date):
	results = get_isins_by_exchange(exchange_code, asset, as_of_date)
	if results:
		records = results['ArrayOfIdentifierRecords']
		if records:
			return records
	return []
	
def get_securities_for_exchanges(as_of_date):
	f = open('symbols.tsv', 'w')
	f.write('Type\tIdentifier\tISIN\tName\tExchange\n')	
	failures = []
	asset_list = ['Bond', 'Indices', 'Stock', 'Other', 'StructuredProduct', 'Fund', 'MoneyMarket', 'Derivative', 'Currency', 'Technical', 'Commodity', 'CurrencyForward', 'InterestRateSwaps', 'DepositoryReceipt', 'ExchangeTradedFund']
	asset_list = ['Indices', 'Stock', 'Other', 'Fund', 'MoneyMarket', 'DepositoryReceipt', 'ExchangeTradedFund']
	records_by_symbol = {}
	for asset in asset_list:
		print 'Running for %s...' % asset
		exchanges = get_exchanges()
		for exchange in exchanges:
			failure = False
			print 'Requesting %s, %s...' % (asset, exchange)
			try:
				symbol_records = get_symbols_for_exchange(exchange, asset, as_of_date)
				isin_records = get_isins_for_exchange(exchange, asset, as_of_date)
			except Exception as e:
				failure = True
				error = 'Failed on (%s, %s): %s' % (asset, exchange, e)
				print error
				failures.append(error)
				
			if not failure:
				print 'Found %s symbol records and %s isin records for exchange %s.' % (len(symbol_records), len(isin_records), exchange)
				
				isin_by_name = {}
				for record in isin_records:
					isin_by_name[record['Name']] = {'ISIN':record['Identifier']}
				
				for record in symbol_records:
					isin_record = isin_by_name.get(record['Name'], {})
					isin = isin_record.get('ISIN', '')
					if isin is None: isin = ''
					
					record_ident = record['Identifier'] 
					output = '%s\t%s\t%s\t%s\t%s\n' % (asset, record_ident, isin, record['Name'], exchange)
					if record_ident not in records_by_symbol:													
						records_by_symbol[record_ident] = {}	
					records_by_symbol[record_ident][exchange] = output
					#f.write('%s\t%s\t%s\t%s\t%s\n' % (asset, record['Identifier'], isin, record['Name'], exchange))
	
	for symbol, records in records_by_symbol.items():
		exchange_with_max_volume = None
		max_volume = 0
		
		if len(records) == 1:
			for exchange, _ in records.items():
				exchange_with_max_volume = exchange
		else:
			for exchange, _ in records.items():
				fundamentals = get_fundamentals_for_symbol('%s.%s' % (symbol, exchange), 'Symbol')		
				adv = fundamentals.get('AverageDailyVolumeLastTwentyDays', None)
				adv = 0 if adv is None else float(adv)
					
				if adv >= max_volume:
					max_volume = adv
					exchange_with_max_volume = exchange
					
			if len(records) > 1:
				print 'Multiple records found for %s - keeping %s' % (symbol, exchange_with_max_volume)
			for exchange, _ in records.items():
				if exchange != exchange_with_max_volume:
					print 'Dropping: (%s, %s)' % (symbol, exchange)
		f.write(records[exchange_with_max_volume])
	f.flush()			
	f.close()
	print failures

def partition(lst, n):
    if n == 0:
        return [lst]
    division = len(lst) / float(n)
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in xrange(n) ]
    
def retrieve_days_prices(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    today = datetime.date.today()
    query_date = today - datetime.timedelta(days=daysback)
    filename = '%s.tsv' % str(query_date)
    f = open(filename, 'w')
    f.write('xignite market\txignite symbol\tqa id\tqa company\tqa exchange\tqa ticker\tqa currency\txignite previous_close\txignite last\txignite latest_close\txignite native currency\txignite industry sector\txignite isin\tmarket cap\t20 day adv\tsplit ratio\tdividend\n')

    err_filename = 'err_%s.tsv' % str(today)
    e = open(err_filename, 'w')
    e.write('symbol\tisin\terror\n')
    
    success = 0
    error = 0
    skip = 0
    missing_isin = 0

    for stock in Pwp_Pwp_Xignite_Stocks.select():
        #if error > 5: break
        #if success > 5: break
        stock_id = stock.stock
        company = stock.stock_name
        exchange = stock.stock_exchange
        symbol = stock.stock_symbol
        isin = stock.isin
        local_ccy = stock.currency

        if len(isin) != 12:
            missing_isin += 1            
            msg = 'Invalid ISIN in db for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue
        
        if stock.has_db_price_for_date(query_date):
            skip += 1
            msg = 'Price already exists in db for (%s, %s).' % (symbol, isin)
            print msg
            continue
                
        quote = stock._retrieve_closing_quote_for_date()
        if quote is None:
            error += 1
            msg = 'No quote available for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
        
        security = quote.get('Security', None)
        if security is None:
            error += 1
            msg = 'No security found on xignite quote for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
        
        xignite_symbol = security['Symbol']
        xignite_market = security['Market']
        xignite_prev_close = quote['LastClose']
        xignite_last = quote['Last']
        xignite_latest_close = quote['EndOfDayPrice']
        xignite_change_amt = quote['ChangeFromLastClose']
        xignite_change_pct = quote['PercentChangeFromLastClose']
        xignite_industry = security.get('CategoryOrIndustry', '')
        xignite_ccy = quote['Currency']
        
        xignite_split = quote.get('SplitRatio', '')
        #xignite_div = quote.get('CummulativeCashDividend', '')
        
        if xignite_market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 
                              'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
            error += 1
            msg = 'Exchange %s not supported for (%s, %s).' % (xignite_market, symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
            
        if xignite_last is None or xignite_prev_close is None or xignite_latest_close is None:
            error += 1
            msg = 'Unable to find price for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
        
        if local_ccy != xignite_ccy:
            error += 1
            msg = 'Persisted currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, xignite_ccy, symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue
        
        print 'Px %s found on xignite quote for (%s, %s).' % (xignite_latest_close, symbol, isin)
        
        fundamentals = get_fundamentals_for_symbol(isin, 'ISIN')
        
        market_cap = fundamentals.get('MarketCapitalization', None)
        market_cap = 0 if market_cap is None else str(float(market_cap))
        adv = fundamentals.get('AverageDailyVolumeLastTwentyDays', None)
        adv = 'n/a' if adv is None else str(float(adv))
        xignite_div = fundamentals.get('LastDividendYield', None)
        xignite_div = 0 if xignite_div is None else str(float(xignite_div))
        
        if persist:
            try:
                stock.update_closing_price_for_date(xignite_latest_close)                		
                stock.update_data(xignite_prev_close, xignite_last, xignite_change_amt, xignite_change_pct,
								market_cap, xignite_div)
            except Exception as ex:
                error += 1
                msg = 'Exception %s persisting px %s for (%s, %s).' % (ex, xignite_latest_close, symbol, isin)
                e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
                print msg
                continue
		
        try:
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % 
                    (xignite_market.encode('utf-8'), 
                     xignite_symbol.encode('utf-8'), 
                     stock_id, 
                     company.encode('utf-8'), 
                     exchange.encode('utf-8'), 
                     symbol.encode('utf-8'), 
                     local_ccy.encode('utf-8'), 
                     xignite_prev_close, 
                     xignite_last, 
                     xignite_latest_close, 
                     xignite_ccy.encode('utf-8'), 
                     xignite_industry.encode('utf-8'), 
                     isin.encode('utf-8'),
                     market_cap.encode('utf-8'),
                     adv.encode('utf-8'),
                     str(xignite_split).encode('utf-8'),
                     str(xignite_div).encode('utf-8')))
            success += 1
        except Exception as ex:            
            print 'Exception writing to file for (%s, %s): %s' % (symbol, isin, ex)
    f.flush()
    f.close()
    e.close()
    
    body = 'Successfully updated prices for %s stocks.' % success
    body += '<br/>Skipped updating %s stocks as they were already current.' % skip
    body += '<br/>Found errors updating %s stocks.' % error
    body += '<br/>ISINs missing on %s stocks.' % missing_isin
    print body
    
	#    send_mail('craig.perler@gmail.com', emailto, '[QA] xIgnite Report: %s' % str(today), '<h3>Please find the latest pricing data from xIgnite attached.</h3><br/>' + body, files=[filename, err_filename])
    #send_mail('craig.perler@gmail.com', emailto, 'xIgnite Report: %s' % str(today), '<h3>Please find the latest pricing data from xIgnite attached.</h3><br/>' + body, files=[filename, err_filename])



    
def batch_retrieve_days_prices(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    isins = [stock.identifier_and_suffix()[0] for stock in Pwp_Pwp_Stocks.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.stock_symbol) for stock in Pwp_Pwp_Stocks.select() if stock.isin is None]
    
    print 'Updating %s stocks that have ISINs.' % len(isins)
    print 'Not updating %s stocks that do not have ISINs.' % len(no_isins)

    all_isins = []
    success = []
    skips = []
    errors = []

    today = datetime.date.today()
    query_date = today - datetime.timedelta(days=daysback)
    filename = '%s.tsv' % str(query_date)
    f = open(filename, 'w')
    f.write('xignite market\txignite symbol\tqa id\tqa company\tqa exchange\tqa ticker\tqa currency\txignite previous_close\txignite last\txignite latest_close\txignite native currency\txignite industry sector\txignite isin\n')
    
    for lst in partition(isins, len(isins) / 100):      
        eod_quotes = get_eod_quotes(lst, 'ISIN', query_date)
        for eod_quote in eod_quotes:
            if eod_quote:
                security = eod_quote.get('Security', None)
                if security:
                    xignite_market = security['Market']
                    xignite_symbol = security['Symbol']
                    xignite_isin = security['ISIN']
                    xignite_prev_close = eod_quote['LastClose']
                    xignite_last = eod_quote['Last']
                    xignite_latest_close = eod_quote['EndOfDayPrice']
                    
                    if xignite_last is None or xignite_last == 0.0 or xignite_prev_close is None or xignite_prev_close == 0.0 or xignite_latest_close is None or xignite_latest_close == 0.0:
                        errors.append((xignite_symbol, xignite_isin, 'No or 0.0 value price available for (%s, %s).' % (xignite_symbol, xignite_isin)))
                        all_isins.append(xignite_isin)
                    
                    try:                    
                        stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == xignite_isin).get()
                    except:
                        all_isins.append(xignite_isin)
                        errors.append((xignite_symbol, xignite_isin, 'Unable to locate stock for (%s, %s).' % (xignite_symbol, xignite_isin)))
                        continue

                    stock_id = stock.stock
                    company = stock.stock_name
                    exchange = stock.stock_exchange
                    symbol = stock.stock_symbol
                    isin = stock.isin

                    if xignite_market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 
                                          'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
                        all_isins.append(isin)
                        errors.append((symbol, isin, 'Exchange %s not supported for (%s, %s).' % (xignite_market, symbol, isin)))

                    local_ccy = stock.currency
                    xignite_ccy = eod_quote['Currency']
                    if local_ccy != xignite_ccy:
                        all_isins.append(isin)
                        errors.append((symbol, isin, 'Persisted currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, xignite_ccy, symbol, isin)))
                    
                    xignite_industry = security.get('CategoryOrIndustry', '')
                    
                    try:
                        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % 
                                (xignite_market.encode('utf-8'), 
                                 symbol, 
                                 stock_id, 
                                 company, 
                                 exchange, 
                                 symbol, 
                                 local_ccy, 
                                 xignite_prev_close, 
                                 xignite_last, 
                                 xignite_latest_close, 
                                 xignite_ccy.encode('utf-8'), 
                                 xignite_industry.encode('utf-8'), 
                                 isin.encode('utf-8')))
                    except:
                        all_isins.append(isin)
                        errors.append((symbol, isin, 'Error writing data to file for (%s, %s).' % (symbol, isin)))
                        continue
                    
                    if persist:
                        stock.update_closing_price_for_date(px=xignite_prev_close)
                    all_isins.append(isin)
                else:
                    print 'Error retrieving quote: %s' % eod_quote['Message']
            else:
                print 'Unexpected error occurred.'
    '''
    for isin in isins:
        if isin not in all_isins:
            try:
                stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == isin).get()
                symbol = stock.stock_symbol
                print 'Unable to retrieve quote for (%s, %s)' % (symbol, isin)
                errors.append((symbol, isin, 'Unable to retrieve quote for (%s, %s)' % (symbol, isin)))
            except:
                print 'Unable to retrieve quote for (%s, %s)' % ('Unknown Symbol', isin)
                errors.append((symbol, isin, 'Unable to retrieve quote for (%s, %s)' % ('Unknown Symbol', isin)))
    '''         
    f.close()

    err_filename = 'err_%s.tsv' % str(today)
    e = open(err_filename, 'w')
    e.write('symbol\tisin\terror\n')
    for error_details in errors:
        symbol = error_details[0]
        isin = error_details[1]
        msg = error_details[2]
        if symbol or isin:
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
    e.close()

    print 'Successfully updated prices for %s stocks.' % len(success)
    print 'Skipped updating %s stocks as they were already current.' % len(skips)
    print 'Found errors updating %s stocks.' % len(errors)
    print 'ISINs missing on %s stocks.' % len(no_isins)

    send_mail('craig.perler@gmail.com', emailto, '[QA] xIgnite Report: %s' % str(today), '<h3>Please find the latest pricing data from xIgnite attached.</h3>', files=[filename, err_filename], server='smtp.gmail.com', username='craig.perler@gmail.com', password='')    