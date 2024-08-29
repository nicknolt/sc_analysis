import logging
from typing import Type, TypeVar

# from resource_finder import ResourceFinder

# # COUCOU 2
#
# to_exclude: List[str] = list()
# loggers: Dict[str, logging.Logger] = dict()

_FT = TypeVar('_FT')

LOG_FORMAT = logging.Formatter(
    '%(thread)d %(threadName)s %(asctime)s%(msecs)04d -- %(short_name)s -- %(levelname)s -- %(message)s --  %(filename)s:%(lineno)d')


def basic_config_log(level: int = logging.INFO):

    ch = logging.StreamHandler()
    # ch.setLevel(logging.CRITICAL)
    ch.setFormatter(LOG_FORMAT)
    ch.addFilter(ShortNameFilter())

    logging.basicConfig(level=level, handlers=[ch])


def fully_qualified_name(obj: object) -> str:
    res = "{}.{}".format(obj.__module__, obj.__class__.__qualname__)

    return res


def create_fake_type(cls: Type[_FT], additional_id: str = "") -> Type[_FT]:
    res_type: type = type('Fake{}{}'.format(cls.__name__, additional_id), (cls,), {})
    res_type.__abstractmethods__ = frozenset()  # type: ignore

    return res_type


def create_logger(obj: object) -> logging.Logger:
    id_logger = fully_qualified_name(obj)

    logger = logging.getLogger(id_logger)

    return logger


# https://stackoverflow.com/questions/61456543/logging-in-python-with-yaml-and-filter
class ShortNameFilter(logging.Filter):

    def filter(self, record):
        # if a name is a fully qualified name "a.b.c.class_name" => display only the class name nothing change by otherway
        record.short_name = record.name.split('.')[-1]
        # print("Short Name : {} | Long = {}".format(record.short_name, record.name))
        return True
