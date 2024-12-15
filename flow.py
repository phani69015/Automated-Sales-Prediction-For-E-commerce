from prefect import flow, task
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib
import os
from datetime import timedelta

# Setup Google Sheets access
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'spark.json'

def get_google_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service

@task
def fetch_data_from_sheets():
    service = get_google_service()
    spreadsheet_id = '1vfUWmgZZAykj3dMOlbP6xoS7bIpigxrI9Cm05q3CM3o'
    range_name = 'sales_data'
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    df = pd.DataFrame(values[1:], columns=values[0])
    df = df.astype({'Sales': 'float', 'DayOfWeek': 'int', 'Month': 'int', 'IsWeekend': 'int'})
    return df

@task
def train_forecast_model(df):
    # Feature Engineering (use Lag features)
    df['Sales_Lag_7'] = df['Sales'].shift(7)
    df = df.dropna()

    X = df[['Sales_Lag_7', 'DayOfWeek', 'Month', 'IsWeekend']]
    y = df['Sales']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f"Model Test MSE: {mse}")

    # Save the model
    model_path = 'model.joblib'
    joblib.dump(model, model_path)
    
    return model

@task
def forecast_next_week(model, df):
    # Prepare data for forecasting next 7 days
    last_week = df.tail(7)
    next_week_features = last_week[['Sales_Lag_7', 'DayOfWeek', 'Month', 'IsWeekend']].values

    predictions = model.predict(next_week_features)
    print(f"Next week's sales predictions: {predictions}")

    return predictions

@task
def update_google_sheets(predictions):
    service = get_google_service()
    spreadsheet_id = '1vfUWmgZZAykj3dMOlbP6xoS7bIpigxrI9Cm05q3CM3o'
    range_name = 'Sheet1!B2:B8'  # Adjust the range based on the number of predictions
    body = {
        'values': [[p] for p in predictions]
    }
    result = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_name,
                                                     valueInputOption='RAW', body=body).execute()
    print(f"{result.get('updatedCells')} cells updated.")

@flow(log_prints=True)
def sales_forecasting_flow():
    data = fetch_data_from_sheets()
    model = train_forecast_model(data)
    predictions = forecast_next_week(model, data)
    update_google_sheets(predictions)

if __name__ == "__main__":
    # Schedule the flow to run every week (604800 seconds)
    sales_forecasting_flow.serve(
        name="weekly-sales-forecasting",
        tags=["sales", "forecasting"],
        interval=604800  # 604800 seconds = 7 days
    )
