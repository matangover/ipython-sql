from datanb import sql, transform_query, parse

def merge(query, user_ns):
    transformed = transform_query(parse(query), user_ns)
    return sql(transformed)