
def to_batch_uow(batch):
    return {'batch': batch}

def unbatch_uow(uow):
    return list(map(
        lambda _uow: _uow,
        uow['batch']
    ))
