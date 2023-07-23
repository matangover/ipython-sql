from datanb import sql, transform_query, parse

def merge(query, user_ns, autolimit=None):
    # TODO: What about multiple sql statements in the same query? This was supported before.
    # Also, what if it's not a select statement?
    transformed = transform_query(parse(query), user_ns)
    autolimit_applied = False
    if autolimit is not None and autolimit > 0:
        if "limit" in transformed.args:
            # It is a select statement.
            if transformed.args["limit"] is None:
                # It does not have an existing limit, add the autolimit.
                transformed = transformed.limit(autolimit)
                autolimit_applied = True
    return sql(transformed), autolimit_applied
