from typing import Any, Generic, Iterable, Iterator, TypeVar

from tqdm import tqdm as std_tqdm

_T = TypeVar("_T")


class tqdm(std_tqdm, Iterator[_T], Generic[_T]):
    def __init__(self, iterable: Iterable[_T], *args: Any, **kwargs: Any) -> None:
        super().__init__(iterable, *args, disable=None, **kwargs)
