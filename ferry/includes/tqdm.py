from functools import partial

from tqdm import tqdm as std_tqdm


class tqdm(std_tqdm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, disable=None, **kwargs)
