import base64
import datetime
import inspect
import uuid
from enum import Enum
from functools import partial
from inspect import FullArgSpec
from types import ModuleType
from typing import Any, _ForwardRef, _FinalTypingBase, Optional, Union, List, Dict, _tp_cache, _type_check, Callable, \
    NamedTuple, Tuple

from xrpc.util import time_parse
from xrpc.serde.abstract import SerdeType, SerdeInst, SerdeNode, DESER, SER


class ForwardRefSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return isinstance(t, _ForwardRef)

    def norm(self, i: SerdeInst, t: Any, mod: ModuleType):
        t = t._eval_type(mod.__dict__, mod.__dict__)
        t = i.norm(t, mod)
        return t

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return i.step(i.norm(t, mod), mod)


class _Joint(_FinalTypingBase, _root=True):
    """Joint type.

    Joint[X] is equivalent to Union[X, _Joint.Tag].
    """

    __slots__ = ()

    class Tag:
        pass

    @_tp_cache
    def __getitem__(self, arg):
        arg = _type_check(arg, "Joint[t] requires a single type.")
        return Union[arg, self.Tag]


Joint = _Joint(_root=True)


class UnionSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return t.__class__.__name__ == '_Union'

    def norm(self, i: SerdeInst, t: Any, mod: ModuleType) -> Any:
        if t.__args__[-1] == type(None):
            *vt, _ = t.__args__
            return Optional[i.norm(Union[tuple(i.norm(vtt, mod) for vtt in vt)], mod)]
        elif t.__args__[-1] == _Joint.Tag:
            *vt, _ = t.__args__
            return Joint[i.norm(Union[tuple(i.norm(vtt, mod) for vtt in vt)], mod)]
        else:
            st = [i.norm(x, mod) for x in t.__args__]
            return Union[st]

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        assert hasattr(t, '__args__')

        # todo build all of the dependencies that would later on be used in the deserializer

        sub_args = t.__args__

        if sub_args[-1] == type(None):
            *vt, _ = t.__args__
            vt = tuple(vt)

            if len(vt) == 1:
                vt = i.norm(vt[0], mod)

                return SerdeNode(t, [vt])
            else:
                vt = i.norm(Union[vt], mod)
                r = i.step(vt, mod)
                return SerdeNode(t, r.deps)
        elif sub_args[-1] == _Joint.Tag:
            *vt, _ = t.__args__
            vt = tuple(vt)
            if len(vt) == 1:
                vt = i.norm(vt[0], mod)

                fields = [z for i, (f, z) in enumerate(vt._field_types.items())]

                rtn_deps = [i.norm(z, mod) for z in fields]

                return SerdeNode(t, rtn_deps)
            else:
                assert False
        elif len(sub_args) == 1:
            assert False, [2]
        else:
            assert False, [1]

        rtn_deps = []

        while True:

            if sub_args[-1] == type(None):
                *vt, _ = t.__args__
                vt = tuple(vt)
                vt = i.norm(vt, mod)
                rtn_deps = [vt]
            elif sub_args[-1] == _Joint.Tag:
                *vt, _ = t.__args__
                vt = tuple(vt)
                vt = i.norm(Union[vt], mod)

                fields = [z for i, (f, z) in enumerate(vt._field_types.items())]

                rtn_deps = [i.norm(z, mod) for z in fields]

                return SerdeNode(t, rtn_deps)
            elif len(sub_args) == 1:
                pass
                break
            else:
                assert False

        return SerdeNode(t, rtn_deps)

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        assert hasattr(t, '__args__')

        sub_args = t.__args__

        funcs_to = []

        while True:
            if sub_args[-1] == type(None):
                vt, *_ = sub_args

                def deser_none(val, fn):
                    return fn(val) if val is not None else None

                funcs_to.append(deser_none)
                sub_args = sub_args[:-1]
            elif sub_args[-1] == _Joint.Tag:
                vt, *_ = sub_args
                fields = [(i, f) for i, (f, _) in enumerate(vt._field_types.items())]

                def deser_joint(val, fn):
                    r = {}
                    i, f = None, None
                    try:
                        for i, f in fields:
                            r[f] = deps[i](val)
                    except BaseException as e:
                        raise NotImplementedError(f'{t} {i} {f} {e}')
                    return vt(**r)

                funcs_to.append(deser_joint)
                sub_args = sub_args[:-1]
                break
            elif len(sub_args) == 1:
                vt, *_ = sub_args

                def deser_value(val, fn):
                    return deps[0](val)

                funcs_to.append(deser_value)
                break
            else:
                assert False, ('possibly a union type', t, t.__args__, deps)

        def central_dummy(val):
            return val

        prev_item = central_dummy
        for x in funcs_to[::-1]:
            prev_item = partial(x, fn=prev_item)

        def central(val):
            return prev_item(val=val)

        return central

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        assert hasattr(t, '__args__')

        sub_args = t.__args__

        funcs_to = []

        while True:
            if sub_args[-1] == type(None):
                vt, *_ = sub_args

                def deser_none(val, fn):
                    return fn(val) if val is not None else None

                funcs_to.append(deser_none)
                sub_args = sub_args[:-1]
            elif sub_args[-1] == _Joint.Tag:
                vt, *_ = sub_args
                fields = [(i, f) for i, (f, _) in enumerate(vt._field_types.items())]

                def deser_joint(val, fn):
                    r = {}
                    i, f = None, None
                    try:
                        for i, f in fields:
                            r[f] = deps[i](val)
                    except BaseException as e:
                        raise NotImplementedError(f'{t} {i} {f} {e}')
                    return vt(**r)

                funcs_to.append(deser_joint)
                sub_args = sub_args[:-1]
                break
            elif len(sub_args) == 1:
                vt, *_ = sub_args

                def deser_value(val, fn):
                    return deps[0](val)

                funcs_to.append(deser_value)
                break
            else:
                assert False, ('possibly a union type', t, t.__args__, deps)

        def central_dummy(val):
            return val

        prev_item = central_dummy
        for x in funcs_to[::-1]:
            prev_item = partial(x, fn=prev_item)

        def central(val):
            return prev_item(val=val)

        return central


class AtomSerde(SerdeType):
    ATOMS = (int, str, bool, float)

    def match(self, t: Any) -> bool:
        return t in self.ATOMS

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        mapper, *_ = [x for x in self.ATOMS if t == x]
        return lambda val: mapper(val)

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        mapper, *_ = [x for x in self.ATOMS if t == x]

        def serializer_callable(val):
            assert isinstance(val, mapper), f'{val} {mapper}'

            return val

        return serializer_callable


class BytesSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(t, bytes)

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        def deserializer_callable(val: Any) -> bytes:
            return base64.b64decode(val.encode())

        return deserializer_callable

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        def serializer_callable(val):
            return base64.b64encode(val).decode()

        return serializer_callable


class NoneSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(type(t), type(None))

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: None

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: None


class UUIDSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(t, uuid.UUID)

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: uuid.UUID(hex=val)



ISO8601 = '%Y-%m-%dT%H:%M:%S.%f'


class DateTimeSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(t, datetime.datetime)

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: time_parse(val, ISO8601)

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: format(val, ISO8601)


class ListSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(t, List)

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        assert hasattr(t, '__args__')
        st, = t.__args__
        st = i.norm(st, mod)
        return SerdeNode(List[st], [st])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        def list_deser(val):
            d, *_ = deps
            return [d(v) for v in val]

        return list_deser

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        def list_ser(val):
            s, *_ = deps
            return [s(v) for v in val]

        return list_ser


class DictSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(t, Dict)

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        kt, vt = t.__args__
        kt, vt = i.norm(kt, mod), i.norm(vt, mod)
        return SerdeNode(Dict[kt, vt], [kt, vt])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        def deser_dict(val):
            r = {}
            try:
                for k, v in val.items():
                    r[deps[0](k)] = deps[1](v)
            except BaseException as e:
                raise NotImplementedError(f'{t} {k} {v} {e}')
            return r

        return deser_dict

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        def ser_dict(val):
            ks, vs = deps
            return {ks(k): vs(v) for k, v in val.items()}

        return ser_dict


class EnumSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(t, Enum)

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        assert False, ''


class NamedTupleSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return hasattr(t, '_field_types')

    def step(self, i: SerdeInst, t: Any, mod: Any) -> SerdeNode:
        return SerdeNode(t, [i.norm(t, mod) for f, t in t._field_types.items()])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        # NamedTuple meta
        fields = [f for f, _ in t._field_types.items()]

        def deser_namedtuple(val):
            r = {}

            i, f = None, None

            try:

                for i, f in enumerate(fields):
                    r[f] = deps[i](val.get(f))
            except BaseException as e:
                raise ValueError(f'[1] {t}, {i}, {f}, {val}: {e}')

            try:

                return t(**r)
            except BaseException as e:
                raise ValueError(f'[2] {t}, {i}, {f}, {val}: {e}')

        return deser_namedtuple

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        fields = [f for f, _ in t._field_types.items()]

        def ser_namedtuple(val):
            return {v: deps[k](getattr(val, v)) for k, v in enumerate(fields)}

        return ser_namedtuple


class CallableArgsWrapper(NamedTuple):
    method: bool
    name: str
    spec: FullArgSpec

    @classmethod
    def from_func(self, fn: Callable):
        return CallableArgsWrapper(inspect.ismethod(fn), f'{fn.__module__}.{fn.__name__}', inspect.getfullargspec(fn))

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return super().__eq__(other)
        else:
            return False

    def __hash__(self) -> int:
        r = (
            'CallableArgsWrapper',
            self.name,
            self.method,
            tuple(self.spec.args),
            self.spec.varargs,
            self.spec.varkw,
            self.spec.defaults,
            tuple(self.spec.kwonlyargs),
            None if self.spec.kwonlydefaults is None else tuple(sorted(self.spec.kwonlydefaults.items())),
            None if self.spec.annotations is None else tuple(sorted(self.spec.annotations.items())),
        )
        return hash(r)


class CallableRetWrapper(NamedTuple):
    method: bool
    name: str
    spec: FullArgSpec

    @classmethod
    def from_func(self, fn: Callable):
        return CallableRetWrapper(inspect.ismethod(fn), f'{fn.__module__}.{fn.__name__}', inspect.getfullargspec(fn))

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return super().__eq__(other)
        else:
            return False

    def __hash__(self) -> int:
        r = (
            'CallableRetWrapper',
            self.name,
            self.method,
            tuple(self.spec.args),
            self.spec.varargs,
            self.spec.varkw,
            self.spec.defaults,
            tuple(self.spec.kwonlyargs),
            None if self.spec.kwonlydefaults is None else tuple(sorted(self.spec.kwonlydefaults.items())),
            None if self.spec.annotations is None else tuple(sorted(self.spec.annotations.items())),
        )
        return hash(r)


class CallableArgsSerde(SerdeType):
    # fullargspect -> Type (or a function)
    # args_kwargs -> INPUT

    args_var = '$var'
    args_kw = '$kw'

    def match(self, t: Any) -> bool:
        return isinstance(t, CallableArgsWrapper)

    def _build_args(self, t: CallableArgsWrapper):
        at = t.spec

        missing_args = []
        map = {}

        args = at.args

        def get_annot(name):
            if name not in at.annotations:
                missing_args.append(name)
                return None
            return at.annotations[name]

        if t.method:
            args = args[1:]

        for arg in args:
            map[arg] = get_annot(arg)

        if at.varargs:
            map[self.args_var] = get_annot(at.varargs)

        if at.varkw:
            map[self.args_kw] = get_annot(at.varkw)

        if at.kwonlyargs:
            for arg in at.kwonlyargs:
                map[arg] = get_annot(arg)

        if len(missing_args):
            raise NotImplementedError(f'Could not find annotations for: `{missing_args}`')

        return map

    def step(self, i: SerdeInst, t: CallableArgsWrapper, mod: Any) -> SerdeNode:
        types = sorted(self._build_args(t).items(), key=lambda x: x[0])

        types = [x for _, x in types]

        return SerdeNode(t, types)

    def deserializer(self, t: CallableArgsWrapper, deps: List[DESER]) -> DESER:
        at = t.spec
        map = self._build_args(t)
        map = sorted(map.items(), key=lambda x: x[0])
        map = zip(map, enumerate(deps))
        map = {k: i for (k, _), (i, _) in map}

        def get_map(name):
            return deps[map[name]]

        def callable_deserializer(val: list) -> list:
            val: Tuple[List[Any], Dict[str, Any]]
            args, kwargs = val

            # we need to "eat" the arguments correctly (and then map them to something else).

            map_args = t.spec.args

            if t.method:
                map_args = map_args[1:]

            r_args, r_kwargs = tuple(), {}

            for arg in args:
                if len(map_args):
                    curr_arg, map_args = map_args[0], map_args[1:]

                    r_args = r_args + (get_map(curr_arg)(arg),)
                elif t.spec.varargs:
                    r_args = r_args + (get_map(self.args_var)(arg),)
                else:
                    raise ValueError(f'Could not find mapping for argument `{arg}`')

            for kwarg_name, kwarg_val in kwargs.items():
                if kwarg_name not in map:
                    if at.varkw:
                        r_kwargs[kwarg_name] = get_map(self.args_kw)(kwarg_val)
                    else:
                        raise ValueError(f'Function does not accept `{kwarg_name}`')
                else:
                    r_kwargs[kwarg_name] = get_map(kwarg_name)(kwarg_val)

            return [r_args, r_kwargs]

        return callable_deserializer

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        at = t.spec
        map = self._build_args(t)
        map = sorted(map.items(), key=lambda x: x[0])
        map = zip(map, enumerate(deps))
        map = {k: i for (k, _), (i, _) in map}

        def get_map(name):
            return deps[map[name]]

        def callable_serializer(val: SER) -> list:
            val: Tuple[List[Any], Dict[str, Any]]
            args, kwargs = val

            map_args = t.spec.args

            if t.method:
                map_args = map_args[1:]

            r_args, r_kwargs = tuple(), {}

            for arg in args:
                if len(map_args):
                    curr_arg, map_args = map_args[0], map_args[1:]

                    r_args = r_args + (get_map(curr_arg)(arg),)
                elif t.spec.varargs:
                    r_args = r_args + (get_map(self.args_var)(arg),)
                else:
                    raise ValueError(f'Could not find mapping for argument `{arg}`')

            for kwarg_name, kwarg_val in kwargs.items():
                if kwarg_name not in map:
                    if at.varkw:
                        r_kwargs[kwarg_name] = get_map(self.args_kw)(kwarg_val)
                    else:
                        raise ValueError(f'Function does not accept `{kwarg_name}`')
                else:
                    r_kwargs[kwarg_name] = get_map(kwarg_name)(kwarg_val)

            return [r_args, r_kwargs]

        return callable_serializer


class CallableRetSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return isinstance(t, CallableRetWrapper)

    def step(self, i: SerdeInst, t: CallableRetWrapper, mod: Any) -> SerdeNode:
        RET = 'return'

        return SerdeNode(t, [t.spec.annotations.get(RET, None)])

    def deserializer(self, t: CallableArgsWrapper, deps: List[DESER]) -> DESER:
        def callable_ret_deser(val):
            return deps[0](val)

        return callable_ret_deser

    def serializer(self, t: CallableArgsWrapper, deps: List[SER]) -> SER:
        def callable_ret_ser(val):
            return deps[0](val)

        return callable_ret_ser
