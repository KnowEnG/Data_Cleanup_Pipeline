import redis


class RedisUtil:
    def __init__(self, credential, source_hint, taxonid):
        """Returns a Redis database connection.

        This returns a Redis database connection access to its functions if the
        module is imported.
        Returns:
            StrictRedis: a redis connection object
        """
        self.redis_db = redis.StrictRedis(host=credential['host'], port=credential['port'],
                                 password=credential['password'], socket_timeout=10)
        self.hint = source_hint
        self.taxid = taxonid


    def get_node_info(self, fk_array, ntype):
        """Uses the redis database to convert a node alias to KN internal id
        Figures out the type of node for each id in fk_array and then returns
        all of the metadata associated or unmapped-*
        Args:
            fk_array (list): the array of foreign gene identifers to be translated
            ntype (str): 'Gene' or 'Property' or None
            hint (str): a hint for conversion
            taxid (str): the species taxid, None if unknown
        Returns:
            list: list of lists containing 5 col info for each mapped gene
        """
        hint = None if self.hint == '' or self.hint is None else self.hint.upper()
        taxid = None if self.taxid == '' or self.taxid is None else str(self.taxid)
        if ntype == '':
            ntype = None

        if ntype is None:
            res_arr = self.redis_db.mget(['::'.join(['stable', str(fk), 'type']) for fk in fk_array])
            fk_prop = [fk for fk, res in zip(fk_array, res_arr) if res is not None and res.decode() == 'Property']
            fk_gene = [fk for fk, res in zip(fk_array, res_arr) if res is not None and res.decode() == 'Gene']
            if len(fk_prop) > 0 and len(fk_gene) > 0:
                raise ValueError("Mixture of property and gene nodes.")
            ntype = 'Property' if len(fk_prop) > 0 else 'Gene'

        if ntype == "Gene":
            stable_array = self.conv_gene(fk_array)
        elif ntype == "Property":
            stable_array = fk_array
        else:
            raise ValueError("Invalid ntype")

        return list(zip(fk_array, *self.node_desc(stable_array)))


    def conv_gene(self, fk_array):
        """Uses the redis database to convert a gene to ensembl stable id
        This checks first if there is a unique name for the provided foreign key.
        If not it uses the hint and taxid to try and filter the foreign key
        possiblities to find a matching stable id.
        Args:
            fk_array (list): the foreign gene identifers to be translated
            hint (str): a hint for conversion
            taxid (str): the species taxid, 'unknown' if unknown
        Returns:
            str: result of searching for gene in redis DB
        """
        hint = None if self.hint == '' or self.hint is None else self.hint.upper()
        taxid = None if self.taxid == '' or self.taxid is None else str(self.taxid)

        #use ensembl internal uniprot mappings
        if hint == 'UNIPROT' or hint == 'UNIPROTKB':
            hint = 'UNIPROT_GN'

        ret_stable = ['unmapped-none'] * len(fk_array)

        def replace_none(ret_st, pattern):
            """Search redis for genes that still are unmapped
            """
            curr_none = [i for i in range(len(fk_array)) if ret_st[i] == 'unmapped-none']
            if curr_none:
                vals_array = self.redis_db.mget([pattern.format(str(fk_array[i]).upper(), taxid, hint) for i in curr_none])
                for i, val in zip(curr_none, vals_array):
                    if val is None: continue
                    ret_st[i] = val.decode()

        if hint is not None and taxid is not None:
            replace_none(ret_stable, 'triplet::{0}::{1}::{2}')
        if taxid is not None:
            replace_none(ret_stable, 'taxon::{0}::{1}')
        if hint is not None:
            replace_none(ret_stable, 'hint::{0}::{2}')
        if taxid is None:
            replace_none(ret_stable, 'unique::{0}')
        return ret_stable


    def node_desc(self, stable_array):
        """Uses the redis database to find metadata about node given its stable id
        Return all metadata for each element of stable_array
        Args:
            stable_array (str): the array of stable identifers to be searched
        Returns:
            list: list of lists containing 4 col info for each mapped node
        """
        ret_type = ["None"] * len(stable_array)
        ret_alias = list(stable_array)
        ret_desc = list(stable_array)
        st_map_idxs = [idx for idx, st in enumerate(stable_array) if not st.startswith('unmapped')]
        if st_map_idxs:
            vals_array = self.redis_db.mget(['::'.join(['stable', stable_array[i], 'type']) for i in st_map_idxs])
            for i, val in zip(st_map_idxs, vals_array):
                if val is None: continue
                ret_type[i] = val.decode()
            vals_array = self.redis_db.mget(['::'.join(['stable', stable_array[i], 'alias']) for i in st_map_idxs])
            for i, val in zip(st_map_idxs, vals_array):
                if val is None: continue
                ret_alias[i] = val.decode()
            vals_array = self.redis_db.mget(['::'.join(['stable', stable_array[i], 'desc']) for i in st_map_idxs])
            for i, val in zip(st_map_idxs, vals_array):
                if val is None: continue
                ret_desc[i] = val.decode()
        return stable_array, ret_type, ret_alias, ret_desc

