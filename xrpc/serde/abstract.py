import logging
import sys
from types import ModuleType

from collections import deque

from xrpc.serde.error import SerdeException

if sys.version_info >= (3, 7):
    from typing import NamedTuple, Any, List, Optional, Union, Dict, Callable, \
        TypeVar, Type, Tuple, Deque, Set
else:
    from typing import Any, Optional, Union, List, Dict, Callable, \
        NamedTuple, Tuple, TypeVar, Type

from dataclasses import dataclass, field

DESER_IN = Union[list, dict, str, float, int, Any]
DESER = Callable[[DESER_IN], Any]
SER_OUT = Union[list, dict, str, float, int, Any]
SER = Callable[[Any], SER_OUT]


def _repr_typevar(x):
    if isinstance(x, TypeVar):
        return f'TypeVar({repr(x)}, {id(x)})'
    else:
        return repr(x)


@dataclass
class SerdeStepContext:
    t: Any = None
    mod: ModuleType = None
    generic_vals: Dict[Any, Any] = field(default_factory=dict)

    def __repr__(self):

        def map_repr(x):
            if isinstance(x, dict):
                items = ['='.join((_repr_typevar(k), _repr_typevar(v))) for k, v in x.items()]
                items = ', '.join(items)
                return f'{{{items}}}'
            else:
                return repr(x)

        items = [self.t, self.mod, self.generic_vals]
        items = [map_repr(x) for x in items if x is not None]
        items = ', '.join(items)
        return f'{self.__class__.__qualname__}({items})'

    def merge(self, other: 'SerdeStepContext'):
        return SerdeStepContext(other.t, other.mod, {**self.generic_vals, **other.generic_vals})


@dataclass
class SerdeNode:
    type: Any
    deps: List[DESER]
    ctx: Optional[SerdeStepContext] = field(default_factory=SerdeStepContext)


class SerdeInst:
    def __init__(self, context: List['SerdeType']):
        self.context = context

    def match(self, t: Any, ctx: SerdeStepContext) -> 'SerdeType':
        for x in self.context:
            try:
                if x.match(t, ctx):
                    logging.getLogger(__name__ + '.match').debug('%s -> %s %s', t, x, ctx)
                    return x
            except:
                raise ValueError(f'GivenClass={t.__class__} GivenType={t} Matcher={x}')
        else:
            raise ValueError(f'Could not match `{t}` `{t.__class__}`')

    def norm(self, t: Any, ctx: SerdeStepContext) -> Any:
        r = self.match(t, ctx).norm(self, t, ctx)
        logging.getLogger(__name__ + '.norm').debug('%s -> %s [%s]', t, r, ctx)
        return r

    def step(self, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return self.match(t, ctx).step(self, t, ctx)

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return self.match(t, SerdeStepContext()).deserializer(t, deps)

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        return self.match(t, SerdeStepContext()).serializer(t, deps)


class SerdeTypeDeserializer:
    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.parent}, {str(self.t)}, {[x.__class__.__name__ for x in self.deps]})'

    def __repr__(self) -> str:
        return str(self)

    def __init__(self, parent: 'SerdeType', t: Any, deps: List[DESER]):
        self.parent = parent
        self.t = t
        self.deps = deps

    def __call__(self, val: Union[list, dict]) -> Any:
        raise NotImplementedError(f'Deserializer {self.parent.__class__.__name__}')


class SerdeTypeSerializer:

    def __str__(self) -> str:
        return SerdeTypeDeserializer.__str__(self)

    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        self.parent = parent
        self.t = t
        self.deps = deps

    def __call__(self, val: Any) -> Union[list, dict]:
        raise NotImplementedError(f'Serializer {self.parent.__class__.__name__}')


class SerdeType:
    """
    This is an instantiable Deserializer Type
    """
    type: str = None
    cls_serializer: Type[SerdeTypeSerializer] = SerdeTypeSerializer
    cls_deserializer: Type[SerdeTypeDeserializer] = SerdeTypeDeserializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        pass

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        return t

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        raise NotImplementedError(self.__class__.__name__)

    # what if the passable object to the serializer
    # is not the same as the object passed to the deserializer ?
    # - it does not matter as we are telling the engine upfront
    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return self.cls_deserializer(self, t, deps)

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        return self.cls_serializer(self, t, deps)


T = TypeVar('T')


class SerdeStruct(NamedTuple):
    deserializers: Dict[Any, DESER]
    serializers: Dict[Any, SER]

    def deserialize(self, t: Any, val) -> T:
        try:
            return self.deserializers[t](val)
        except Exception as e:
            raise SerdeException(val, '$', t=t, par=e)

    def serialize(self, t, val):
        try:
            return self.serializers[t](val)
        except Exception as e:
            raise SerdeException(val, '$', t=t, par=e)


@dataclass
class SerdeSet:
    items: List[SerdeNode] = field(default_factory=list)

    def __iter__(self):
        return iter(self.items)

    def __or__(self, other):
        return self.merge(other)

    def merge(self, *xs: 'SerdeSet') -> 'SerdeSet':
        r: List[SerdeNode] = []

        for x in (self,) + xs:
            for y in x:
                for rx in r:
                    if rx.type == y.type:
                        break
                else:
                    r.append(y)

        return SerdeSet(r)

    def struct(self, i: SerdeInst) -> SerdeStruct:
        desers_pre = {}
        sers_pre = {}

        def raiser_deser(val: dict) -> Any:
            assert False, 'should never happen'

        def raiser_ser(val: dict) -> Any:
            assert False, 'should never happen'

        for x in self.items:
            desers_pre[x.type] = [raiser_deser for _ in x.deps]
            sers_pre[x.type] = [raiser_ser for _ in x.deps]

        desers = {}
        sers = {}

        for t, deps in desers_pre.items():
            desers[t] = i.deserializer(t, deps)

        for t, deps in sers_pre.items():
            sers[t] = i.serializer(t, deps)

        try:
            for n, (t, deps) in zip(self.items, desers_pre.items()):
                assert len(n.deps) == len(deps), (n.deps, deps)
                for dt, i in zip(n.deps, range(len(deps))):
                    deps[i] = desers[dt]

            for n, (t, deps) in zip(self.items, sers_pre.items()):
                assert len(n.deps) == len(deps), (n.deps, deps)
                for dt, i in zip(n.deps, range(len(deps))):
                    deps[i] = sers[dt]
        except KeyError:
            raise ValueError(f'{desers} {sers}')

        return SerdeStruct(desers, sers)

    @classmethod
    def empty(cls):
        return SerdeSet([])

    @classmethod
    def walk(cls, i: SerdeInst, t: Any, ctx: SerdeStepContext = SerdeStepContext()) -> 'SerdeSet':
        assert isinstance(ctx, SerdeStepContext)

        def do_visit(obj, ctx):
            x = i.step(obj, ctx)

            r[x.type] = x

            for dep in x.deps:
                if dep not in seen:
                    seen.add(dep)
                    to_visit.append((dep, x.ctx))

        seen: Set[Any] = {t, }
        r: Dict[Any, SerdeNode] = {}

        to_visit: Deque[Tuple[t, SerdeStepContext]] = deque()

        do_visit(t, ctx)

        while len(to_visit):
            x, ctx = to_visit.popleft()
            do_visit(x, ctx)

        return SerdeSet(list(r.values()))
