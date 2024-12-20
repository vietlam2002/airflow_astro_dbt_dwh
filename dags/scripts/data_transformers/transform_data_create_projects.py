import pandas as pd

def transform_data(data):
    if isinstance(data, pd.DataFrame):
        data.rename(columns={"_is": "id"}, inplace=True)
    print("DONE!!")
    # print(data.columns())
    return data