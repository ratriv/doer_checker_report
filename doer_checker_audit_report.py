import pandas as pd
import re, os, sys, logging, random, argparse
import operator, datetime
from xlsxwriter.utility import xl_rowcol_to_cell

def create_cmdb(input_file=None):
	try:
		df=pd.read_excel(input_file,sheet_name=None,header=0)
		import sqlite3
		con = sqlite3.connect('cmdb/CMDB.db')
		cur = con.cursor()
		MAX_SQLITE_INT = 2 ** 63 - 1
		sqlite3.register_adapter(int, lambda x: hex(x) if x > MAX_SQLITE_INT else x)
		sqlite3.register_converter('integer', lambda b: int(b, 16 if b[:2] == b'0x' else 10))
		logger.info("creating cmdb....")
		for tbl, d in df.items():
			d.to_sql(tbl, con,if_exists="replace")
		con.close()
		logger.info("Latest CMDB Synced..")
	except Exception as e:
		logger.error(str(e))

def load_CMDB(db):
    import sqlite3
    try:
        con = sqlite3.connect(db)
        cur = con.cursor()
        server_df=pd.read_sql("SELECT distinct(Name), ifnull(Environment,'Undefiined') Environment  FROM server", con)
        application_df=pd.read_sql("SELECT distinct(Name), ifnull(Environment,'Undefiined') Environment  FROM application", con)
        logger.info("Preparing CMDB Data...")
        cmdb_df=pd.concat([server_df,application_df])
        cmdb_df.rename(columns = {'Name':'Configuration item'}, inplace = True)
        
    except Exception as e:
        raise
    return cmdb_df

def identify_first(data):
    doer_checker_re = re.compile(('checker|doer'))
    return re.search(doer_checker_re,data).group()

def get_time(str_date):
    return datetime.datetime.strptime(str_date,"%Y-%m-%d %H:%M:%S")

def check_violations(doer,checker,doer_date,checker_date,srd=None,env=None):
    map_violations={}
    if (doer and checker) and (doer != checker):
        if (doer_date and checker_date) and (get_time(checker_date) >= get_time(doer_date)):
            map_violations['state']=True
            map_violations['comment']="No Violation"
        else:
            map_violations['state']=False
            map_violations['comment']="Violation: Doer/Checker date order is not correct"     
    else:
        map_violations['state']=False
        map_violations['comment']="Violation: Doer or Checker or both missing"  
    
    if srd and (srd in excluded_srd):
        map_violations['state']="Not Applicable"
        map_violations['comment']="[IGNORE]: Excluded SRD" 
    
    if env and (env != 'Production'):
        map_violations['state']="Not Applicable"
        map_violations['comment']="[IGNORE]: This is Non Prod CI" 
        
    return map_violations['state'],map_violations['comment']
    
def get_doer_checker(data):   
    data=str(data)
    #Compile a pettern of 2020-12-14 14:51:31 - Amit ABHANG (Work-Notes) [code]Requested chain is Activated.[/code]
    pattern = re.compile("(\d{4}.\d{2}.\d{2}.\d{2}.\d{2}.\d{2})\W{3}(\S*[\w ]+) (.*)\n(.+)")    
    parsed=re.finditer(pattern,data)
    sorted_data=sorted(parsed,key=operator.itemgetter(0))
    map_data={'checker':None,'doer':None,'checker_date':None,'doer_date':None}
    for i in sorted_data:  
        date=i.groups()[0]
        name=i.groups()[1]
        item=i.groups()[2]
        data=i.groups()[3]        
        if 'checker' in data.lower() and 'checker' in identify_first(data.lower()):
            map_data['checker'] = name
            map_data['checker_date']=date
               
        if 'doer' in data.lower() and 'doer' in identify_first(data.lower()):
            map_data['doer'] = name
            map_data['doer_date']=date
                  
    return map_data['doer'],map_data['checker'],map_data['doer_date'],map_data['checker_date']

def inc_minning(df):
    logger.info(f'Loading the CMDB data for {len(df)} items...')
    cmdb_df=load_CMDB('cmdb/CMDB.db')    
    selection=['restart','reboot','disk free', 'service stop'] #Exclude keywords
    pattern = '|'.join(selection)
    logger.info(f'+Validation Short description Criteria...')
    inc = df[~df['Short description'].str.contains(pattern, case=False)]
    final_df=pd.merge(inc,cmdb_df, on='Configuration item')
    return final_df
 
def setHeader(sheet,row,col,h):  
    global LastCol
    for hCol, hVal in enumerate(h):
        sheet.write(row,hCol+col,hVal,header_format)
    LastCol = col + hCol
            
def create_data(df_name,sheet_name=None,sheet_header=None):
    global LastRow
    df_name.to_excel(writer, sheet_name=sheet_name, startrow=start_row+1, startcol=start_col, index=False, header=False,encoding='latin1')
    LastRow = start_row + df_name.shape[0] -1
    worksheet = writer.sheets[sheet_name]
    setHeader(worksheet,start_row,start_col,sheet_header)
    return worksheet 

def generate_excel(data_frame,sheet_name,op_file):
    global start_row, start_col, LastCol, LastRow, writer, header_format
    start_row= 1
    start_col = 0
    LastRow = 0
    LastCol = 0
    
    writer = pd.ExcelWriter(op_file, engine='xlsxwriter')
    workbook  = writer.book
    #EXCEL FORMATTING   
    blue_format = workbook.add_format({'bg_color':'#AED6F1','border':1,'font_name':'calibri light','font_size':'9','align':"center"})
    red_format = workbook.add_format({'bg_color':'#F74A4A','border':1,'font_name':'calibri light','font_size':'9','align':"center"})
    green_format = workbook.add_format({'bg_color':'#50DE89','border':1,'font_name':'calibri light','font_size':'9','align':"center"})
    header_format = workbook.add_format({'bold':True,'bg_color':"#808080",'border':1, 'font_name':"calibri",'font_size':10,'align':"center", 'color':"#FFFFFF"})
    data_format = workbook.add_format({'border':1,'font_name':'calibri light','font_size':'9','align':"center"})
    merge_format = workbook.add_format({'bold':True, 'color':"#FFFFFF",'font_size':14,'align': 'center','bg_color':"#404244",'border': 1})
    date_format = workbook.add_format({'border':1, 'font_name':'calibri light','font_size':'9','num_format':'yyyy-mm-dd hh:mm:ss'})    
           
    header=data_frame.columns.values
    worksheet_dc=create_data(data_frame,sheet_name,header)         
    '''Arrange The Row Col to set Data'''
    col_range=xl_rowcol_to_cell(start_row + 1, start_col)+":"+xl_rowcol_to_cell(LastRow + 1, LastCol)
    FirstCell = xl_rowcol_to_cell(start_row - 1, start_col)
    LastCell = xl_rowcol_to_cell(start_row - 1, LastCol)
    merge_range = FirstCell + ":" + LastCell
    TopLeftCell = xl_rowcol_to_cell(start_row, start_col)
    BottomRightCell = xl_rowcol_to_cell(LastRow, LastCol)
    FilterRange = TopLeftCell+":"+BottomRightCell
          
    now_date = datetime.datetime.strptime('1900-01-01 12:00:00', "%Y-%m-%d %H:%M:%S")  
    worksheet_dc.merge_range(merge_range, "Doer Checker Audit Report", merge_format)
    worksheet_dc.set_column(col_range, None, data_format)    
    worksheet_dc.conditional_format(col_range, {'type': 'text', 'criteria': 'containing','value':'FALSE','format': red_format})
    worksheet_dc.conditional_format(col_range, {'type': 'text', 'criteria': 'containing','value':'TRUE','format': green_format})
    worksheet_dc.conditional_format(col_range, {'type': 'text', 'criteria': 'containing','value':'Not Applicable','format': blue_format})
    worksheet_dc.conditional_format(col_range, {'type': 'date','criteria': 'greater than','value': now_date,'format': date_format})              
    worksheet_dc.conditional_format(col_range, {'type': 'blanks','format': date_format})   
    worksheet_dc.set_column(start_col,LastCol,15)
    worksheet_dc.autofilter(FilterRange)
    
    writer.save()   
    logger.info(f'Output File is ready >> {op_file}')
    
    
def process_file(xls):    
    try:          
        xl = pd.ExcelFile(xls)
        df = xl.parse(xl.sheet_names[0],skiprows=0, index_col=None)       
        logger.info(f'Total  {len(df)} items qualified to target Doer/Checker')
        df['Merged'] = df['Work-Notes'].fillna('').str.cat(df['Additional comments'].fillna(''))
        df['Doer'],df['Checker'],df['Doer Date'],df['Checker Date']=zip(*df['Merged'].map(get_doer_checker))
        df.drop(['Work-Notes','Additional comments','Merged'],axis=1, inplace=True) 
        if 'INC' in df.Number.iloc[0]:
            df=inc_minning(df)
            sheet_name='Incidents'
            df['Compliant'],df['Comment']= zip(*df.apply(lambda x: check_violations(x['Doer'],x['Checker'],x['Doer Date'],x['Checker Date'],env=x['Environment']), axis=1))
        else:
            sheet_name='RITMs'
            df['Compliant'],df['Comment']= zip(*df.apply(lambda x: check_violations(x['Doer'],x['Checker'],x['Doer Date'],x['Checker Date'],srd=x['SRD_ID']), axis=1))            
        #Add Auditor Column        
        df.columns = map(str.lower, df.columns)
        df['Auditor']= df['assignment group'].apply(lambda x: random.choice([item for k,v in auditor_selection.items() for item in auditor_selection[x]]))
        df.columns = map(str.title, df.columns)
        start,end=df['Closed'].sort_values(ascending=True,ignore_index=True).apply(lambda x : str(x.day)+'_'+str(x.month_name())+'_'+str(x.year)).iloc[[0,-1]]
        op_file=os.path.abspath(os.path.splitext(xls)[0]+'_doer_checker_audit_report_'+start+'-'+end+os.path.splitext(xls)[1])        
        generate_excel(df,sheet_name,op_file)
        
    except Exception as e:
        logger.error(str(e))
		
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)
##Configuration parameters
excluded_srd=['SRD000001002591','TCH000000000086','SRD000001002366','SRD000001001746','SRD000001000647','SRD000001000650']
auditor_selection = {
            'TRESO-CONSO.OPERATIONS_GLB_TGS':['Rahul TRIVEDI','Anshu ANAND'],
            'MIDDELWARE.WEBWAS.OPERATIONS_GLB_TGS': ['Bibhuti NARAYAN','Anshu ANAND','Deepak KRISHNAN'], 
            'MIDDLEWARE.EDD.OPERATIONS_GLB_TGS':['Ajit SRIVASTAVA','Pravin GIRADKER','Nishant GODBOLE'], 
            'APPLI.BATCH.OPERATIONS_GLB_TGS':['Sowjanya KOTTAKKI','Preetam DHENGRE','Ankit GUPTA'],
            'APPLI.CA.OPERATIONS_GLB_TGS':['Shail KUMARI','Jasvinder BAKSHI','Sayali GADKARI'],
            'ENGINEERING.OPERATIONS_GLB_TGS':[None],
            'RELEASE.OPERATIONS_GLB_TGS':['Nisha TOMAR','Nishant KUMAR']
        }

if __name__=='__main__':
	parser = argparse.ArgumentParser(description='This doer_checker module process the input service now inc/ritm data with work-notes and additional comments columns and provide results with doer and checker data.')
	parser.add_argument('-f', '--file', help="Provide the service now incident/ritm excel file.", dest='source_file')
	parser.add_argument('-c', '--cmdb_file', help="Provide the latest cmdb excel file.", dest='cmdb_file')	
	args = parser.parse_args()
	if args.cmdb_file:
		logger.info(f'CMDB Sync: enabled')
		logger.info(f'Wait till the CMDB sync is going on...')
		create_cmdb(args.cmdb_file)
		process_file(args.source_file)
	elif args.source_file:
		logger.info(f'CMDB Sync: disabled')
		process_file(args.source_file)
	else:
		logger.warning(f'No Parameter provided, please provide the input file to process') 