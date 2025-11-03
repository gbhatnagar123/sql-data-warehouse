/*
Bulk Insert using CSV Insert in Postgres pgadmin4
Header: Yes
Encoding: UTF-8
Delimiter: ','
*/

/* Validation check. Ensure data is in the right place */
select * from bronze.crm_cust_info

select count(*) from bronze.crm_cust_info
/* Total Rows from CSV: 18,494 Total Rows from Table: 18,494 */

/*
Alternatively use this psql command to upload files from the local folder.
*/
\COPY bronze.crm_cust_info FROM '/Users/gauravbhatnagar/Documents/Personal_projects/SQL Data Warehouse/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv' DELIMITER ',' CSV HEADER;
