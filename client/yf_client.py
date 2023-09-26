from unicodedata import name
import yfinance as yf
from typing import List
import pandas as pd


class YahooFinanceClient:

    def load_stock_data(self, stock_symbols: List[str], period: str) -> pd.DataFrame:
        return yf.download(stock_symbols, period=period)


if __name__ == "__main__":
    client = YahooFinanceClient()
    stock_data = client.load_stock_data(stock_symbols=['AAPL'], period="1d")
    print(stock_data)
