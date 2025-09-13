import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"   #a=append
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
        WITH 
        freightSummary as (
            select 
                VendorNumber,
                sum(freight) as FreightCost 
                from vendor_invoice
                group by vendorNumber),
        
        purchaseSummary as (
            select
            p.VendorNumber,
            p.vendorname,
            p.brand,
            p.purchaseprice,
            pp.volume,
            pp.Description,
            pp.price as ActualPrice, 
            sum(p.Quantity) as TotalPurchaseQuantity, 
            sum(p.dollars) as TotalPurchaseDollars 
            from purchases p 
            join purchase_prices pp on pp.brand = p.brand 
            where p.purchaseprice>0 
            group by p.VendorNumber,p.VendorName,p.Brand 
            order by TotalPurchaseDollars),
        salesSummary as (
            select
            vendorno,
            brand,
            sum(SalesDollars) as TotalSalesDollars,
            sum(SalesPrice) as TotalSalesPrice,
            sum(SalesQuantity) as TotalSalesQuantity,
            sum(ExciseTax) as TotalExciseTax
            from sales
            group by vendorno,brand order by TotalSalesDollars) 
        
        select
            ps.VendorNumber,
            ps.vendorname,
            ps.brand,
            ps.Description,
            ps.purchaseprice,
            ps.ActualPrice,
            ps.volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalSalesQuantity,
            ss.TotalExciseTax,
            fs.FreightCost
        from purchaseSummary ps
        left join salesSummary ss on ps.VendorNumber = ss.vendorno and ps.brand = ss.brand
        left join freightSummary fs on ss.vendorno = fs.VendorNumber
        order by ps.TotalPurchaseDollars desc""",conn)
    
    return vendor_sales_summary



def clean_data(df):
    '''This function will clean the data'''
    #changing datatype to float
    df['volume']=df['volume'].astype('float')


    #filling missing values with 0
    df.fillna(0,inplace = True)


    #removing spaces from categorical columns
    df['vendorname'] = df['vendorname'].str.strip()
    df['Description'] = df['Description'].str.strip()



    #creating  new columns for better analysis.
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars']) * 100
    #Always less than 1. because stockturnover means sold quantity / purchased quantity.
    #if stockturnover > 1 then vendor should have had some old stocks.
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

    return df

if __name__ == '__main__':
    #creating database connection.
    conn = sqlite3.connect('inventory.db')

    logging.info('....Creating Vendor Summary Table....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('....Cleaning Data....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('....Ingesting Data....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')
    

         