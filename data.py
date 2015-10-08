# data.py

import os
import os.path
import pandas as pd
import queue

from abc import ABCMeta, abstractmethod

from event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCV) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, n=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")


class MinuteHistoricDataHandlerCSV(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self, events, csv_dir, symbols):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbols = symbols

        self.bars_data = {}
        self.latest_bars_data = pd.Panel(items=self.symbols, major_axis=None,
                                         minor_axis=('open', 'high', 'low', 'close', 'volume'))
        self.continue_backtest = True

        self._open_convert_csv_files()
        self.bars_data = pd.Panel(self.bars_data)
        self.bars_generator = self._get_new_bars(self.symbols)
        self._create_bars_generator(self.symbols)

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from DTN IQFeed. Thus its format will be respected.
        """
        comb_index = None
        for symbol in self.symbols:
            # Load the CSV file with no header information, indexed on date
            self.bars_data[symbol] = pd.read_csv(
                os.path.join(self.csv_dir, '%s.txt' % symbol),
                header=0, index_col=[0],
                parse_dates=[['date', 'time']],
                names=['date', 'time', 'open', 'high', 'low', 'close', 'volume'],
                nrows=50
            )
            self.bars_data[symbol].index.rename('datetime', inplace=True)

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.bars_data[symbol].index
            else:
                comb_index.union(self.bars_data[symbol].index)

            # Set the latest symbol_data to None
            #self.latest_bars_data[symbol] = pd.DataFrame(columns=('open', 'high', 'low', 'close', 'volume'))

        # Reindex the dataframes
        for symbol in self.symbols:
            self.bars_data[symbol].reindex(index=comb_index)
            self.bars_data[symbol]['volume'].fillna(0, inplace=True)
            self.bars_data[symbol].fillna(method='ffill', inplace=True)

    def _get_new_bars(self, symbols):
        """
        Returns the latest bar from the data feed as a tuple of
        (sybmbol, datetime, open, low, high, close, volume).
        """
        for datetime in self.bars_data[symbols].major_axis:
            yield self.bars_data.major_xs(datetime)

    def get_latest_bars(self, symbols, n=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_bars_data[symbols]
        except KeyError:
            print("One or more of the symbols are not available in the historical data set.")
        else:
            return bars_list[-n:]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for symbol in self.symbols:
            try:
                bar = next(self.bars_generator[symbol])
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_bars_data[symbol] = self.latest_bars_data[symbol].append(bar)
                    self.latest_bars_data[symbol].index.rename('datetime', inplace=True)
        self.events.put(item=MarketEvent())


##################################################################

if __name__ == "__main__":
    eventQueue = queue.Queue()
    dh = MinuteHistoricDataHandlerCSV(eventQueue, '/home/mikhail/Kibot Data/', ['AAPL'])
    for i in range(10):
        dh.update_bars()
    print(dh.get_latest_bars('AAPL', n=10))

