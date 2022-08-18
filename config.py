HARVEST_BASE_DIR = '/proj/ads/abstracts/sources/Crossref2/'

HARVEST_LOG_DIR = HARVEST_BASE_DIR + '/UpdateAgent/'

OUTPUT_MAP_FILENAME = 'doi_bibc_xref.map'

ISSN2BIBSTEM_file = 'adscompstat/data/issn2bibstem.txt'
ISSN2BIBSTEM = dict()
with open(ISSN2BIBSTEM_file, 'r') as fi:
    for l in fi.readlines():
        (issn, bibstem) = l.strip().split('\t')
        ISSN2BIBSTEM[issn] = bibstem

NAME2BIBSTEM_file = 'adscompstat/data/name2bibstem.txt'
NAME2BIBSTEM = dict()
with open(NAME2BIBSTEM_file, 'r') as fi:
    for l in fi.readlines():
        (bibstem, name) = l.strip().split('\t')
        NAME2BIBSTEM[name] = bibstem

CLASSIC_DOI_FILE = '/proj/ads/abstracts/config/links/DOI/all.links'
CLASSIC_ALTBIBS = '/proj/ads/abstracts/config/bibcodes.list.alt'
CLASSIC_DELBIBS = '/proj/ads/abstracts/config/bibcodes.list.del'
CLASSIC_ALLBIBS = '/proj/ads/abstracts/config/bibcodes.list.all'
CLASSIC_CANONICAL = '/proj/ads/abstracts/config/bibcodes.list.can'

APS_BIBSTEMS = ['PhRvL', 'PhRvX', 'RvMP.', 'PhRvA', 'PhRvB', 'PhRvC', 'PhRvD', 'PhRvE', 'PhRvS', 'PhRvS', 'PhRvP', 'PhRvF', 'PhRvM', 'PRPER', 'PRSTP', 'PhRv.', 'PhRvI', 'PhyOJ', 'PhRvR']

IOP_BIBSTEMS = ['ApJ..', 'JCAP.', 'EJPh.', 'RAA..', 'PhyM.', 'JPhD.', 'AJ...', 'APExp', 'ApJL.', 'ApJS.', 'BiBi.', 'BioFa', 'BioMa', 'CQGra', 'ChPhC', 'CoTPh', 'EL...', 'ERCom', 'ERExp', 'ERL..', 'EleSt', 'FCS..', 'FlDyR', 'IzMat', 'JBR..', 'JOpt.', 'JPCM.', 'JPEn.', 'JPhA.', 'JPhB.', 'JPhCo', 'JPCom', 'JSSST', 'IOPSN', 'JPhG.', 'JPhM.', 'JPhP.', 'LaPhL', 'MRE..', 'MatQT', 'MLS&T', 'MeScT', 'MuMat', 'NanoE', 'NJPh.', 'NanoF', 'Nanot', 'Nonli', 'PASP.', 'Metro', 'PMB..', 'PPCF.', 'PlREx', 'PhyS.', 'PhyEd', 'PSJ..', 'PhyU.', 'PlST.', 'PrEne', 'RNAAS', 'RPPh.', 'RuMaS', 'SeScT', 'SuScT', 'TDM..', 'TDM..', 'JElS.', 'TDM..', 'ERIS.', 'ECSMA', 'PBioE', 'RuCRv', 'NucFu', 'JMiMi', 'ChPhL', 'InvPr', 'JRP..', 'PSST.', 'SMaS.', 'MSMSE', 'QuEle', 'SbMat', 'JaJAP', 'ANSNN', 'MApFl', 'SuTMP', 'QS&T.', 'E&ES.', 'MS&E.', 'PhBio', 'LaPhy', 'ChPhB', 'JSemi', 'JNEng', 'JGE..', 'JSMTE', 'JPhCS', 'PhyW.', 'PPS..', 'JPhC.', 'JPhF.', 'IJExM', 'ECSTr', 'JInst', 'NanoE']

OUP_BIBSTEMS = ['MNRAS', 'PASJ.', 'PTEP.', 'GeoJI']

AIP_BIBSTEMS = ['AIPA.', 'AIPC.', 'ApPhL', 'APL..', 'APLM.', 'APLP.', 'AVSQS', 'AmJPh', 'ApPRv', 'Chaos', 'JAP..', 'JChPh', 'JMP..', 'JPCRD', 'LTP..', 'PhFl.', 'PhPl.', 'PhTea', 'RScI']
