from unicodedata import name
import yfinance as yf
from typing import List
import pandas as pd


class YahooFinanceClient:

    def load_stock_data(self, stock_symbols: List[str], period: str) -> pd.DataFrame:
        return yf.download(stock_symbols, period=period)


if __name__ == "__main__":
    client = YahooFinanceClient()
    stock_data = client.load_stock_data(
        stock_symbols=['AAPL'], period="max")
    print(stock_data)
    percent_diffs = [stock_data]
    for period in [30, 90, 365, 730]:
        percent_change = stock_data.pct_change(periods=period)['Close']
        percent_change_df = percent_change.to_frame().rename(
            columns={'Close': f'{period} days percent close'}
        )
        percent_diffs.append(percent_change_df)

    overall_data = pd.concat(percent_diffs, axis=1)
    overall_data.to_csv()
    print(overall_data)

    # Code to merge the data
    # df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
