from types import ModuleType
from typing import NamedTuple, Any, List, _ForwardRef, _FinalTypingBase, _type_check, Optional, Union, Dict, Callable, \
    _tp_cache, TypeVar, Type

DESER = Callable[[Union[list, dict]], Any]
SER = Callable[[Any], Union[list, dict]]


class SerdeNode(NamedTuple):
    type: Any
    deps: List[DESER]


class SerdeInst:
    def __init__(self, context: List['SerdeType']):
        self.context = context

    def match(self, t: Any) -> 'SerdeType':
        for x in self.context:
            try:
                if x.match(t):
                    return x
            except:
                raise ValueError(f'GivenClass={t.__class__} GivenType={t} Matcher={x}')
        else:
            raise ValueError(f'Could not match `{t}`')

    def norm(self, t: Any, mod: ModuleType) -> Any:
        return self.match(t).norm(self, t, mod)

    def step(self, t: Any, mod: ModuleType) -> SerdeNode:
        return self.match(t).step(self, t, mod)

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return self.match(t).deserializer(t, deps)

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        return self.match(t).serializer(t, deps)


class SerdeType:
    """
    This is an instantiable Deserializer Type
    """
    type: str = None

    def match(self, t: Any) -> bool:
        pass

    def norm(self, i: SerdeInst, t: Any, mod: ModuleType) -> Any:
        return t

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        raise NotImplementedError(self.__class__.__name__)

    # what if the passable object to the serializer
    # is not the same as the object passed to the deserializer ?
    # - it does not matter as we are telling the engine upfront
    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        raise NotImplementedError(f'Deserializer {self.__class__.__name__}')

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        raise NotImplementedError(f'Serializer {self.__class__.__name__}')


T = TypeVar('T')


class SerdeStruct(NamedTuple):
    deserializers: Dict[Any, DESER]
    serializers: Dict[Any, SER]

    def deserialize(self, t: Type[T], val) -> T:
        return self.deserializers[t](val)

    def serialize(self, t, val):
        return self.serializers[t](val)


class SerdeSet(NamedTuple):
    items: List[SerdeNode]

    def __iter__(self):
        return iter(self.items)

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

        for n, (t, deps) in zip(self.items, desers_pre.items()):
            assert len(n.deps) == len(deps), (n.deps, deps)
            for dt, i in zip(n.deps, range(len(deps))):
                deps[i] = desers[dt]

        for n, (t, deps) in zip(self.items, sers_pre.items()):
            assert len(n.deps) == len(deps), (n.deps, deps)
            for dt, i in zip(n.deps, range(len(deps))):
                deps[i] = sers[dt]

        return SerdeStruct(desers, sers)

    @classmethod
    def empty(cls):
        return SerdeSet([])

    @classmethod
    def walk(cls, i: SerdeInst, t: Any, mod: ModuleType) -> 'SerdeSet':
        x = i.step(t, mod)

        visited: Dict[Any, SerdeNode] = {}
        visited[x.type] = x

        to_visit = [y for y in x.deps if y not in visited]

        while len(to_visit):
            x = to_visit.pop()

            x = i.step(x, mod)

            visited[x.type] = x

            to_visit = [y for y in x.deps if y not in visited and y not in to_visit] + to_visit
            to_visit = list(set(to_visit))

        return SerdeSet(list(visited.values()))
