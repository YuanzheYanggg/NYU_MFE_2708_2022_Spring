from TAQTradesReader import TAQTradesReader
from TAQQuotesReader import TAQQuotesReader
import TAQFilter
import os

trades_test_path_0824 = "./data/trades_test/20070824/TXT_trades.binRT"
quotes_test_path_0824 = "./data/quotes_test/20070824/TXT_quotes.binRQ"
trades_test_path_0827 = "./data/trades_test/20070827/TXT_trades.binRT"
quotes_test_path_0827 = "./data/quotes_test/20070827/TXT_quotes.binRQ"

trades_test_path_0731 = "./data/trades_test/20070731/ABC_trades.binRT"
quotes_test_path_0731 = "./data/quotes_test/20070731/ABC_quotes.binRQ"
trades_test_path_0801 = "./data/trades_test/20070801/ABC_trades.binRT"
quotes_test_path_0801 = "./data/quotes_test/20070801/ABC_quotes.binRQ"


def TAQreader(path_name,file_type):
    if file_type == "trade":
        obj = TAQTradesReader(path_name)
        print("output of getN: ", obj.getN())
        print("output of getSecsFromEpocToMidn: ", obj.getSecsFromEpocToMidn())
        index = 10
        print("output of getPrice at {}: ".format(index), obj.getPrice(index))
        print("output of getMillisFromMidn at {}: ".format(index), obj.getMillisFromMidn(index))
        print("output of getTimestamp at {}: ".format(index), obj.getTimestamp(index))
        print("output of getSize at {}: ".format(index), obj.getSize(index))
        print()

    elif file_type == "quote":
        obj = TAQQuotesReader(path_name)
        print("output of getN",obj.getN())
        print("output of getSecsFromEpocToMidn",obj.getSecsFromEpocToMidn())
        index = 10
        print("output of getMillisFromMidn at {}".format(index),obj.getMillisFromMidn(index))
        print("output of getAskSize at {}".format(index),obj.getAskSize(index))
        print("output of getAskPrice at {}".format(index),obj.getAskPrice(index))
        print("output of getBidSize at {}".format(index),obj.getBidSize(index))
        print("output of getBidPrice at {}".format(index),obj.getBidPrice(index))
        print()


if __name__ == "__main__":
    TAQreader(trades_test_path_0824,"trade")
    TAQreader(trades_test_path_0827,"trade")
    TAQreader(quotes_test_path_0824, "quote")
    TAQreader(quotes_test_path_0827, "quote")

    TAQreader(trades_test_path_0731, "trade")
    TAQreader(trades_test_path_0801, "trade")
    TAQreader(quotes_test_path_0731, "quote")
    TAQreader(quotes_test_path_0801, "quote")

