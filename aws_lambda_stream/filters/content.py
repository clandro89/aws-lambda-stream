from functools import reduce



def filter_on_content(rule, uow):
    return reduce(lambda a,c: a and c(uow, rule),
                    rule.get('filters', []),
                    True
                )
