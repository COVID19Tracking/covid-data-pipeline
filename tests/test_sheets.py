# simple round trip to google spreadsheet
#
# to configure:
#    1. login to google's cloud at https://console.developers.google.com
#    2. select/create a project
#    2. go to APIs and enable sheets
#    3. add a service account
#    4. save the credentials to a local file (exclude from git)
#    5. get the email for the service account
#    6. add it to the google sheet
#


from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# edit these
KEY_PATH = "credentials-scanner.json"
SAMPLE_SPREADSHEET_ID = '1DaUeiQjGwlQWLxRchbZalFe10Y6HsIJVCTx2u1uQkJA'
SAMPLE_RANGE_NAME = 'Status!A1:C2'

def main():

    print("load credentials")
    # pylint: disable=no-member
    creds = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=SCOPES)
     
    print(f"  email {creds.service_account_email}")
    print(f"  project {creds.project_id}")
    print(f"  scope {creds._scopes[0]}")
    print()

    print("connect")
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # read 
    print("read")
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    print(f"  {result}")

    # add one
    values = result.get('values', [])
    values[1][2] = int(values[1][2]) + 1

    # update
    print(f"update")
    result = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME,
                                valueInputOption="USER_ENTERED", 
                                body={ "values": values} ).execute()
    print(f"  {result}")

    # read again 
    print(f"read again")
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    print(f"  {result}")


if __name__ == '__main__':
    main()