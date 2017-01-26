import redis

def get_database(args):
    """Returns a Redis database connection.

    This returns a Redis database connection access to its functions if the
    module is imported.
    Returns:
        StrictRedis: a redis connection object
    """
    return redis.StrictRedis(host=args['host'], port=args['port'],
                             password=args['password'])


def node_desc(rdb, stable_id):
    if stable_id.startswith('unmapped'):
        return (None, stable_id, stable_id)
    alias = rdb.get('::'.join(['stable', stable_id, 'alias']))
    if alias is None: alias = stable_id
    else: alias = alias.decode()
    desc = rdb.get('::'.join(['stable', stable_id, 'desc']))
    if desc is None: desc = stable_id
    else: desc = desc.decode()
    typ = rdb.get('::'.join(['stable', stable_id, 'type']))
    if typ is None: pass
    else: typ = typ.decode()
    return (typ, alias, desc)


def get_node_info(rdb, foreign_key, hint, taxid):
    ntype = rdb.get('::'.join(['stable', foreign_key, 'type']))
    stable_id = None
    if ntype is None:
        stable_id = conv_gene(rdb, foreign_key, hint, taxid)
    elif ntype.decode() == "Gene":
        stable_id = conv_gene(rdb, foreign_key, hint, taxid)
    elif ntype.decode() == "Property":
        stable_id = foreign_key
    return (foreign_key, stable_id) + node_desc(rdb, stable_id)


def conv_gene(rdb, foreign_key, hint, taxid):
    """Uses the redis database to convert a gene to enesmbl stable id
    This checks first if there is a unique name for the provided foreign key.
    If not it uses the hint and taxid to try and filter the foreign key
    possiblities to find a matching stable id.
    Args:
        rdb (redis object): redis connection to the mapping db
        foreign_key (str): the foreign gene identifer to be translated
        hint (str): a hint for conversion
        taxid (str): the species taxid, 'unknown' if unknown
    Returns:
        str: result of seaching for gene in redis DB
    """
    hint = hint.upper()
    #use ensembl internal uniprot mappings
    if hint == 'UNIPROT' or hint == 'UNIPROTKB':
        hint = 'UNIPROT_GN'
    foreign_key = foreign_key.upper()
    unique = rdb.get('unique::' + foreign_key)
    if unique is None:
        return 'unmapped-none'
    if unique != 'unmapped-many'.encode():
        return unique.decode()
    mappings = rdb.smembers(foreign_key)
    taxid_match = list()
    hint_match = list()
    both_match = list()
    for taxid_hint in mappings:
        taxid_hint = taxid_hint.decode()
        taxid_hint_key = '::'.join([taxid_hint, foreign_key])
        taxid_hint = taxid_hint.split('::')
        if len(taxid_hint) < 2: # species key in redis
            continue
        if taxid == taxid_hint[0] and hint in taxid_hint[1]:
            both_match.append(taxid_hint_key)
        if taxid == taxid_hint[0]:
            taxid_match.append(taxid_hint_key)
        if hint in taxid_hint[1] and len(hint):
            hint_match.append(taxid_hint_key)
    if both_match:
        both_ens_ids = set(rdb.mget(both_match))
        if len(both_ens_ids) == 1:
            return both_ens_ids.pop().decode()
    if taxid_match:
        taxid_ens_ids = set(rdb.mget(taxid_match))
        if len(taxid_ens_ids) == 1:
            return taxid_ens_ids.pop().decode()
    if hint_match:
        hint_ens_ids = set(rdb.mget(hint_match))
        if len(hint_ens_ids) == 1:
            return hint_ens_ids.pop().decode()
    return 'unmapped-many'

