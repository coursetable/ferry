"""
tqdm handler for customization
(i.e. disable progress bars in production)
"""
from tqdm import tqdm as std_tqdm


class tqdm(std_tqdm):
    """
    Custom tqdm handler
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, disable=None, **kwargs)
