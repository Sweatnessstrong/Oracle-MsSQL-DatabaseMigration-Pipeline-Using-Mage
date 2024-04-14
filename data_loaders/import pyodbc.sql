import pyodbc

def execute_sql_in_mssql(sql_command, server='your_server', database='your_database',
                         username='your_username', password='your_password'):
    conn_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    with pyodbc.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_command)
            conn.commit()

# Example usage
view_sql = "CREATE VIEW SampleView AS SELECT * FROM YourTable"
execute_sql_in_mssql(view_sql)


SELECT 
    fch.col_refid,
    fpd.pa_refid,
    fpd.inst_pa_refid,
    fch.inst_pa_refid AS parent_inst,
    fch.amount_recieved,
    fch.collection_date,
    fch.gst_applicable,
    fch.gst_percent,
    fch.gst_amount,
    fch.interest_percent,
    fch.interest_amount,
    fch.payable_amount,
    fch.reciept_number,
    fch.created_by,
    fch.created_date,
    fch.updated_by,
    fch.updated_date,
    fch.is_void,
    fch.remarks,
    fch.payer_rs_refid,
    fch.gst_inclusive,
    fpd.due_amount
FROM 
    fms_pa_collection_map AS map 
INNER JOIN 
    fms_collection_header AS fch ON fch.col_refid = map.col_refid
LEFT OUTER JOIN 
    fms_pa_details AS fpd ON map.inst_pa_refid = fpd.inst_pa_refid;


result_text = '''
    SELECT 
        UPPER(fch.col_refid) AS COL_REFID,
        UPPER(fpd.pa_refid) AS PA_REFID,
        UPPER(fpd.inst_pa_refid) AS INST_PA_REFID,
        UPPER(fch.inst_pa_refid) AS PARENT_INST,
        UPPER(fch.amount_recieved) AS AMOUNT_RECIEVED,
        UPPER(fch.collection_date) AS COLLECTION_DATE,
        UPPER(fch.gst_applicable) AS GST_APPLICABLE,
        UPPER(fch.gst_percent) AS GST_PERCENT,
        UPPER(fch.gst_amount) AS GST_AMOUNT,
        UPPER(fch.interest_percent) AS INTEREST_PERCENT,
        UPPER(fch.interest_amount) AS INTEREST_AMOUNT,
        UPPER(fch.payable_amount) AS PAYABLE_AMOUNT,
        UPPER(fch.reciept_number) AS RECIEPT_NUMBER,
        UPPER(fch.created_by) AS CREATED_BY,
        UPPER(fch.created_date) AS CREATED_DATE,
        UPPER(fch.updated_by) AS UPDATED_BY,
        UPPER(fch.updated_date) AS UPDATED_DATE,
        UPPER(fch.is_void) AS IS_VOID,
        UPPER(fch.remarks) AS REMARKS,
        UPPER(fch.payer_rs_refid) AS PAYER_RS_REFID,
        UPPER(fch.gst_inclusive) AS GST_INCLUSIVE,
        UPPER(fpd.due_amount) AS DUE_AMOUNT
        FROM 
            sqldb.fms_csv.fms_pa_collection_map AS map 
        INNER JOIN 
            sqldb.fms_csv.fms_collection_header AS fch ON fch.col_refid = map.col_refid
        LEFT OUTER JOIN 
            sqldb.fms_csv.fms_pa_details AS fpd ON map.inst_pa_refid = fpd.inst_pa_refid;'''
SELECT FCH.INST_PA_REFID,

    SUM(FCD.PAYMENT_AMOUNT) PAID_AMOUNT,

    MAX(FCH.RECIEPT_NUMBER) RECIEPT_NUMBER ,

    MAX(FCH.COLLECTION_DATE)COLLECTION_DATE,

    MAX(FCH.COL_REFID) COL_REFID

  FROM sqldb.fms_csv.FMSV_COLLECTION_HEADER_FOR_EXCEPTION_REPORT FCH

  INNER JOIN sqldb.fms_csv.FMS_COLLECTION_DETAILS FCD

  ON FCH.COL_REFID       =FCD.COL_REFID

  WHERE NVL(IS_VOID,'N')!='Y'

  AND NVL(IS_FAILED,'N')!='Y'

  GROUP BY INST_PA_REFID

VIEW_SQL: CREATE VIEW sqldb.fms_csv.fmsv_collection_details_edc_for_exception_report AS None

ISNULL(FCH.IS_VOID, 'N') != 'Y';