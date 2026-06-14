#!/usr/bin/env python3
"""Test script to see the actual error from list_iceberg_tables()"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Set up environment
os.environ["ICEBERG_CATALOG_TYPE"] = "rest"
os.environ["ICEBERG_CATALOG_URI"] = "http://cdp-utility.cdp.local:8443/gateway/cdp-datashare-access/iceberg-rest"

# Use the JWT token you provided
os.environ["KNOX_JWT"] = "eyJxa3UiOiJodHRwOi8vY2RwLXV0aWxpdHkuY2RwLmxvY2FsOjg0NDMvZ2F0ZXdheS9jZHAtZGF0YXNoYXJlLWFjY2Vzcy9rbm94dG9rZW4vYXBpL3YxL2p3a3MuanNvbiIsImtpZCI6IlZ5SWhPemNLbk5JYzBrdThwYUVJMGxDaUoyd1N3SEVfeG5OLVhtSzM0TFUiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImF1ZCI6ImNkcC1kYXRhc2hhcmUtYWNjZXNzIiwiamt1IjoiaHR0cDovL2NkcC11dGlsaXR5LmNkcC5sb2NhbDo4NDQzL2dhdGV3YXkvY2RwLWRhdGFzaGFyZS1hY2Nlc3Mva25veHRva2VuL2FwaS92MS9qd2tzLmpzb24iLCJraWQiOiJWeUloT3pjS25OSWMwa3E4cGFFSTBsQ2lKMndTd0hFX3huTi1YbUszNExVIiwiaXNzIjoiS05PWFNTTyIsImV4cCI6MTc3ODM5MjYwNiwibWFuYWdlZC50b2tlbiI6InRydWUiLCJrbm94LmlkIjoiMzg1NjllMjYtOWI2NS00ZDM0LTg0NGQtZmVhYzY1YjM1YWUwIn0.Z1Ne3T6KqKfeEcSp1gCCn4h5me3c4YN6iBe4RUaZHJwh0biadlZsxAatAZLF8nklCw4beRuqbAlE_g2f3yPg3znq8i4OK-pOAWuXF1_ocE_MmkH0Xa9ot1w2uUrWQ8VY4wL74fFKTZFCbiex4WCglMzzA8HZf8VQUtIgKwHD09jdFd2UxHerrGjXjCEd_5i_BFcqyqf8VVQmFmO3R-jID8uLgxRWPbxLZ4J5sUIywW2wdFI0wNooRYbJ5APkJlvLBRhp5yRAo1-7qJO0Ohh8mVLKhVtmugM6gHNxIDspcWyooLTqELPKo7v7sr2UkNVbIN9h6iFb1z2Iz2J5HG3IGw"

print("Testing list_iceberg_tables()...\n")

try:
    from tools.iceberg.iceberg_tools import list_iceberg_tables
    result = list_iceberg_tables()
    print(f"Result: {result}")
except Exception as e:
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {e}")
    import traceback
    traceback.print_exc()
