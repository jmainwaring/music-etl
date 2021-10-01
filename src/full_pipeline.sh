#!/bin/bash

cd staging/
python create_tables.py 
python copy_tables.py 
echo "Staging portion completed successfully"

cd ../star_schema/
python create_tables.py 
python insert_records.py 
python run_dq_checks.py 
echo "Star schema portion completed successfully"

cd ../analytical_views/
python create_tables.py 
python run_dq_checks.py
echo "Analytical view portion completed successfully"

echo "Pipeline finished"