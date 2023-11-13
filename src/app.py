import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup

url = 'https://www.macrotrends.net/stocks/charts/TSLA/tesla/revenue'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    historical_data_table = soup.find('table', {'class': 'historical_data_table'})

    if historical_data_table:
        tesla_revenue = pd.DataFrame(columns=["Date", "Revenue"])

        for row in historical_data_table.find_all("tr"):
            col = row.find_all("td")
            if col:
                Date = col[0].text.strip()
                Revenue = col[1].text.replace("$", "").replace(",", "").strip()
                tesla_revenue = pd.concat([tesla_revenue, pd.DataFrame({
                    "Date": [Date],
                    "Revenue": [Revenue]
                })], ignore_index=True)

        tesla_revenue['Revenue'] = pd.to_numeric(tesla_revenue['Revenue'], errors='coerce')
        tesla_revenue['Date'] = pd.to_datetime(tesla_revenue['Date'], errors='coerce')

        tesla_revenue = tesla_revenue.sort_values('Date')

        print(tesla_revenue.head())

        con = sqlite3.connect('src/tesla_revenue.db')
        con.execute('CREATE TABLE IF NOT EXISTS trim_revenue(Date DATE, Revenue REAL)')

        tesla_revenue['Date'] = tesla_revenue['Date'].dt.strftime('%Y-%m-%d')

        con.executemany('INSERT INTO trim_revenue values(?,?)', tesla_revenue.values)

        con.commit()
        for row in con.execute('SELECT * FROM trim_revenue'):
            print(row)
        print()
        con.close()
    else:
        print("No se encontró la tabla histórica.")
else:
    print(f"Error al obtener la página. Código de estado: {response.status_code}")
