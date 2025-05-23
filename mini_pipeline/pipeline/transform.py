# Transform module
def filter_rows(df, step):
    col = step["column"]
    op = step["operator"]
    val = step["value"]

    if op == ">":
        return df[df[col] > val]
    elif op == "<":
        return df[df[col] < val]
    elif op == "==":
        return df[df[col] == val]
    else:
        raise ValueError(f"Unsupported operator: {op}")

def add_column(df, step):
    name = step["name"]
    value = step["value"]
    df[name] = value
    return df

def drop_column(df, step):
    name = step["name"]
    if name in df.columns:
        return df.drop(columns=[name])
    return df

def map_column(df, step):
    col = step["column"]
    mapping = step["mapping"]
    df[col] = df[col].map(mapping).fillna(df[col])  # replace only mapped values
    return df
