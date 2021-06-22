import os
import pandas as pd


def back_test_regress():
    file_list = os.listdir()
    regress_list = []
    neg_return = []
    pos_return = []
    total_return = []

    for filename in file_list:
        if '.csv' in filename:
            regress_list.append(filename)

    for file in regress_list:
        df = pd.read_csv(file, header=None)
        df.columns = ['datetime', 'direction', 'price']
        symbol_return = 1
        holding = False
        for index, row in df.iterrows():
            if row['direction'] == 'BUY' and holding == False:
                cost = row['price'] * 1.001
                holding = True
            elif row['direction'] == 'SELL' and holding == True:
                symbol_return = symbol_return * (row['price'] / cost) * 0.999
                cost = 0
                holding = False
        print(f"{file} has return {symbol_return}, holding: {holding}, cost: {cost}")
        total_return.append(symbol_return)
        if symbol_return < 1:
            neg_return.append([file, symbol_return])
        else:
            pos_return.append([file, symbol_return])

    print(f"negative returns {len(neg_return)}")
    print(f"positive returns {len(pos_return)}")
    print(f"average return {sum(total_return)/len(regress_list)}")

if __name__ == '__main__':
    back_test_regress()