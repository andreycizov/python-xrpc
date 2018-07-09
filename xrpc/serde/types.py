import base64
import datetime
import inspect
import sys
import uuid
from enum import Enum
from functools import partial
from inspect import FullArgSpec
from types import ModuleType

from xrpc.generic import build_generic_context as _build_generic_context


def build_generic_context(*args):
    _, r = _build_generic_context(*args)
    return r


if sys.version_info >= (3, 7):
    from typing import Any, ForwardRef, Optional, Union, List, Dict, _tp_cache, _type_check, Callable, \
        NamedTuple, Tuple, TypeVar, _SpecialForm, Iterable
else:
    from typing import Any, _ForwardRef, _FinalTypingBase, Optional, Union, List, Dict, _tp_cache, _type_check, \
        Callable, \
        NamedTuple, Tuple, TypeVar
from dataclasses import is_dataclass, fields

from xrpc.util import time_parse
from xrpc.serde.abstract import SerdeType, SerdeInst, SerdeNode, DESER, SER, SerdeTypeDeserializer, SerdeTypeSerializer, \
    SerdeStepContext

if sys.version_info >= (3, 7):
    FR = ForwardRef
else:
    FR = _ForwardRef


class ForwardRefSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return isinstance(t, FR)

    def norm(self, i: SerdeInst, t: FR, ctx: SerdeStepContext):
        if sys.version_info >= (3, 7):
            t = t._evaluate(ctx.mod.__dict__, ctx.mod.__dict__)
        else:
            t = t._eval_type(ctx.mod.__dict__, ctx.mod.__dict__)
        t = i.norm(t, ctx)
        return t

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return i.step(i.norm(t, ctx), ctx)


#
#
# class _Joint(_SpecialForm, _root=True):
#     """Joint type.
#     Joint[X] is equivalent to Union[X, _Joint.Tag].
#     """
#
#     __slots__ = ()
#
#     class Tag:
#         pass
#
#     @_tp_cache
#     def __getitem__(self, arg):
#         arg = _type_check(arg, "Joint[t] requires a single type.")
#         return Union[arg, self.Tag]


# Joint = _SpecialForm('Joint', doc='Joined attribs')

class UnionNext(Exception):
    pass


class UnionDeserializer(SerdeTypeDeserializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[DESER]):
        super().__init__(parent, t, deps)

        assert hasattr(t, '__args__')

        sub_args = t.__args__

        stack = []

        while True:
            if sub_args[-1] == type(None):
                vt, *_ = sub_args

                stack.append('deser_none')
                sub_args = sub_args[:-1]
            elif len(sub_args) == 1:
                vt, *_ = sub_args

                stack.append('deser_value')
                break
            else:
                assert False, ('possibly a union type', t, t.__args__, deps)

        self.stack = stack

    def deser_none(self, val):
        if val is None:
            return None
        else:
            raise UnionNext()

    def deser_value(self, val):
        return self.deps[0](val)

    def __call__(self, val: Union[list, dict]) -> Any:
        for item in self.stack:
            try:
                return getattr(self, item)(val)
            except UnionNext:
                pass


class UnionSerde(SerdeType):
    cls_deserializer = UnionDeserializer
    cls_serializer = UnionDeserializer

    def match(self, t: Any) -> bool:
        if sys.version_info >= (3, 7):
            return getattr(t, '__origin__', None) is Union
        else:
            return t.__class__.__name__ == '_Union'

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        if t.__args__[-1] == type(None):
            *vt, _ = t.__args__
            return Optional[i.norm(Union[tuple(i.norm(vtt, ctx) for vtt in vt)], ctx)]
        elif t.__args__[-1] == _Joint.Tag:
            *vt, _ = t.__args__
            return Joint[i.norm(Union[tuple(i.norm(vtt, ctx) for vtt in vt)], ctx)]
        else:
            st = [i.norm(x, ctx) for x in t.__args__]
            return Union[st]

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        assert hasattr(t, '__args__')

        # todo build all of the dependencies that would later on be used in the deserializer

        sub_args = t.__args__

        if sub_args[-1] == type(None):
            *vt, _ = t.__args__
            vt = tuple(vt)

            if len(vt) == 1:
                vt = i.norm(vt[0], ctx)

                return SerdeNode(t, [vt])
            else:
                vt = i.norm(Union[vt], ctx)
                r = i.step(vt, ctx)
                return SerdeNode(t, r.deps)
        elif sub_args[-1] == _Joint.Tag:
            *vt, _ = t.__args__
            vt = tuple(vt)
            if len(vt) == 1:
                vt = i.norm(vt[0], ctx)

                fields = [z for i, (f, z) in enumerate(vt._field_types.items())]

                rtn_deps = [i.norm(z, ctx) for z in fields]

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


class AtomDeserializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)
        self.mapper, *_ = [x for x in self.parent.ATOMS if t == x]

    def __call__(self, val: Any) -> Union[list, dict]:
        return self.mapper(val)


class AtomSerializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)
        self.mapper, *_ = [x for x in self.parent.ATOMS if t == x]

    def __call__(self, val: Any) -> Union[list, dict]:
        if not isinstance(val, self.mapper):
            raise ValueError(f'{val} {self.mapper}')

        return val


class AtomSerde(SerdeType):
    ATOMS = (int, str, bool, float)

    cls_deserializer = AtomDeserializer
    cls_serializer = AtomSerializer

    def match(self, t: Any) -> bool:
        return t in self.ATOMS

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])


class BytesDeserializer(SerdeTypeDeserializer):
    def __call__(self, val: Union[list, dict, str]) -> Any:
        return base64.b64decode(val.encode())


class BytesSerializer(SerdeTypeSerializer):
    def __call__(self, val: Any) -> Union[list, dict]:
        return base64.b64encode(val).decode()


class BytesSerde(SerdeType):
    cls_deserializer = BytesDeserializer
    cls_serializer = BytesSerializer

    def match(self, t: Any) -> bool:
        if inspect.isclass(t):
            return issubclass(t, bytes)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])


class TypeVarSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return isinstance(t, TypeVar)

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        if t in ctx.generic_vals:
            return ctx.generic_vals[t]
        else:
            raise ValueError(f'Only instantated generics are allowed for serialization {t} {ctx}')

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return i.step(self.norm(i, t, ctx), ctx)


class NoneSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return issubclass(type(t), type(None))

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        def deser(val):
            if val is not None:
                raise ValueError(f'Must be None: {val}')
            return None

        return deser

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: None


class UUIDSerde(SerdeType):
    def match(self, t: Any) -> bool:
        if inspect.isclass(t):
            return issubclass(t, uuid.UUID)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: uuid.UUID(hex=val)


ISO8601 = '%Y-%m-%dT%H:%M:%S.%f'


class DateTimeSerde(SerdeType):
    def match(self, t: Any) -> bool:
        if inspect.isclass(t):
            return issubclass(t, datetime.datetime)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: time_parse(val, ISO8601)

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: format(val, ISO8601)


class ListDeserializer(SerdeTypeDeserializer):
    cls_coll = list

    def __call__(self, val: Union[list, dict]) -> Any:
        self.d, *_ = self.deps
        return self.cls_coll(self.d(v) for v in val)


class ListSerializer(ListDeserializer, SerdeTypeSerializer):
    pass


class ListSerde(SerdeType):
    cls_deserializer = ListDeserializer
    cls_serializer = ListSerializer

    def match(self, t: Any) -> bool:
        if inspect.isclass(t):
            return issubclass(t, List)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        assert hasattr(t, '__args__')
        st, = t.__args__
        st = i.norm(st, ctx)
        return SerdeNode(List[st], [st])


class TupleDeserializer(ListDeserializer):
    cls_coll = tuple


class TupleSerializer(TupleDeserializer, SerdeTypeSerializer):

    def __call__(self, val: Union[list, dict]) -> Any:
        return list(super().__call__(val))


class TupleSerde(SerdeType):
    cls_deserializer = TupleDeserializer
    cls_serializer = TupleSerializer

    def match(self, t: Any) -> bool:
        t, _ = _build_generic_context(t)

        if sys.version_info >= (3, 7):
            if hasattr(t, '__origin__'):
                return t.__origin__ is tuple

        if inspect.isclass(t):
            return issubclass(t, Tuple)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        assert hasattr(t, '__args__')
        st, = t.__args__
        st = i.norm(st, ctx)
        return SerdeNode(Tuple[st], [st])


class DictSerde(SerdeType):
    def match(self, t: Any) -> bool:
        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                t = t.__origin__

        if inspect.isclass(t):
            return issubclass(t, Dict)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        kt, vt = t.__args__
        kt, vt = i.norm(kt, ctx), i.norm(vt, ctx)
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
        if inspect.isclass(t):
            return issubclass(t, Enum)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        assert False, ''


def build_obj_module(obj):
    m = obj.__module__

    return sys.modules[m]


class NamedTupleDeserializer(SerdeTypeDeserializer):
    def __init__(self, parent: 'NamedTupleSerde', t: Any, deps: List[DESER]):
        super().__init__(parent, t, deps)
        self.fields = [f for f, _ in t._field_types.items()]

    def __call__(self, val: Union[list, dict]) -> Any:
        r = {}

        i, f = None, None

        try:

            for i, f in enumerate(self.fields):
                # either way (even if a field is non-optional), that field must not accept None as it's argument
                r[f] = self.deps[i](val.get(f))
                # r[f] = self.deps[i](val[f])
        except BaseException as e:
            raise ValueError(f'[1] {self.t}, {i}, {f}, {val}: {e}')

        try:

            return self.t(**r)
        except BaseException as e:
            raise ValueError(f'[2] {self.t}, {i}, {f}, {val}: {e}')


class NamedTupleSerializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)
        self.fields = [f for f, _ in t._field_types.items()]

    def __call__(self, val: Any) -> Union[list, dict]:
        return {v: self.deps[k](getattr(val, v)) for k, v in enumerate(self.fields)}


class NamedTupleSerde(SerdeType):
    cls_deserializer = NamedTupleDeserializer
    cls_serializer = NamedTupleSerializer

    def match(self, t: Any) -> bool:
        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                t = t.__origin__

        return hasattr(t, '_field_types')

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        xt = t

        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                xt = t.__origin__

        ctx = SerdeStepContext(mod=build_obj_module(t))
        return SerdeNode(t, [i.norm(st, ctx) for f, st in xt._field_types.items()], ctx=ctx)


class DataclassDeserializer(SerdeTypeDeserializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[DESER]):
        super().__init__(parent, t, deps)

        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                t = t.__origin__

        self.fields = [f.name for f in sorted(fields(t), key=lambda x: x.name)]

    def __call__(self, val: Union[list, dict]) -> Any:
        return NamedTupleDeserializer.__call__(self, val)


class DataclassSerializer(SerdeTypeDeserializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[DESER]):
        super().__init__(parent, t, deps)

        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                t = t.__origin__

        self.fields = [f.name for f in sorted(fields(t), key=lambda x: x.name)]

    def __call__(self, val: Union[list, dict]) -> Any:
        return NamedTupleSerializer.__call__(self, val)


# def build_generic_context(t, ctx):
#     def mmaps(pars, args):
#         maps = dict(zip(pars, args))
#
#         maps = {k: ctx.generic_vals.get(k, v) if isinstance(v, TypeVar) else v for k, v in maps.items()}
#
#         uninst = {k: isinstance(maps[k], TypeVar) for k in maps}
#
#         if any(uninst.values()):
#             raise ValueError(f'Not all generic parameters are instantiated: {uninst}')
#
#         return maps
#
#     if sys.version_info >= (3, 7):
#         if not hasattr(t, '__origin__'):
#             return ctx
#
#         maps = mmaps(t.__origin__.__parameters__, t.__args__)
#
#         return SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})
#     else:
#         if not hasattr(t, '_gorg'):
#             return ctx
#
#         maps = mmaps(t._gorg.__parameters__, t.__args__)
#
#         return SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})


class DataclassSerde(SerdeType):
    cls_deserializer = DataclassDeserializer
    cls_serializer = DataclassSerializer

    def match(self, t: Any) -> bool:
        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                t = t.__origin__
        return is_dataclass(t)

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        # todo: here, we may need to instantiate the generic items
        ctx = build_generic_context(t, ctx)

        xt = t

        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                xt = t.__origin__

        return SerdeNode(t, [i.norm(f.type, ctx) for f in sorted(fields(xt), key=lambda x: x.name)], ctx)


class CallableArgsWrapper(NamedTuple):
    method: bool
    name: str
    spec: FullArgSpec
    cls: Optional[Any] = None

    @classmethod
    def from_func(self, fn: Callable):
        return CallableArgsWrapper(inspect.ismethod(fn), f'{fn.__module__}.{fn.__name__}', inspect.getfullargspec(fn))

    @classmethod
    def from_func_cls(self, cls: Any, fn: Callable):
        return CallableArgsWrapper(True, f'{fn.__module__}.{fn.__name__}', inspect.getfullargspec(fn), cls)

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
            self.cls
        )
        return hash(r)


class CallableRetWrapper(NamedTuple):
    method: bool
    name: str
    spec: FullArgSpec
    cls: Optional[Any] = None

    @classmethod
    def from_func(self, fn: Callable):
        return CallableRetWrapper(inspect.ismethod(fn), f'{fn.__module__}.{fn.__name__}', inspect.getfullargspec(fn))

    @classmethod
    def from_func_cls(self, cls: Any, fn: Callable):
        return CallableRetWrapper(True, f'{fn.__module__}.{fn.__name__}', inspect.getfullargspec(fn), cls)

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
            self.cls
        )
        return hash(r)


def build_types(spec: FullArgSpec, is_method=False, allow_missing=False):
    missing_args = []
    map = {}

    args = spec.args

    def get_annot(name):
        if name not in spec.annotations:
            missing_args.append(name)
            return None
        return spec.annotations[name]

    if is_method:
        args = args[1:]

    for arg in args:
        map[arg] = get_annot(arg)

    if spec.varargs:
        map[ARGS_VAR] = get_annot(spec.varargs)

    if spec.varkw:
        map[ARGS_KW] = get_annot(spec.varkw)

    if spec.kwonlyargs:
        for arg in spec.kwonlyargs:
            map[arg] = get_annot(arg)

    if spec.annotations and 'return' in spec.annotations:
        map[ARGS_RET] = spec.annotations['return']
    else:
        map[ARGS_RET] = None

    if not allow_missing and len(missing_args):
        raise NotImplementedError(
            f'Can not find annotations for arguments named: `{missing_args}`')

    return map


def pair_spec(spec: FullArgSpec, is_method, *args, **kwargs) -> Tuple[Tuple[Any], Dict[str, Any]]:
    # pair function arguments to their names

    # we need to "eat" the arguments correctly (and then map them to something else).
    map_args = list(spec.args)
    kwonlyargs = list(spec.kwonlyargs) if spec.kwonlyargs else []

    mapped_args = list()

    if is_method:
        assert map_args[0] == 'self'
        map_args = map_args[1:]

    vararg_ctr = 0

    for arg in args:
        if len(map_args):
            curr_arg, map_args = map_args[0], map_args[1:]

            yield curr_arg, None

            mapped_args.append(curr_arg)
        elif spec.varargs:
            yield ARGS_VAR, vararg_ctr
            vararg_ctr += 1

        else:
            raise ValueError(f'Could not find mapping for argument `{arg}`')

    for kwarg_name, kwarg_val in kwargs.items():
        has_matched = False

        if kwarg_name in map_args:
            map_args.remove(kwarg_name)
            has_matched = True
        elif kwarg_name in kwonlyargs:
            kwonlyargs.remove(kwarg_name)
            has_matched = True

        if not has_matched:
            if spec.varkw:
                assert kwarg_name not in mapped_args, kwarg_name
                yield ARGS_KW, kwarg_name
            else:
                raise ValueError(f'Function does not accept `{kwarg_name}`')
        else:
            yield kwarg_name, None

    assert len(map_args) == 0, map_args

    # return r_args, r_kwargs


ARGS_VAR = '$var'
ARGS_KW = '$kw'
ARGS_RET = '$ret'


class CallableArgsSerde(SerdeType):
    # fullargspect -> Type (or a function)
    # args_kwargs -> INPUT

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
            map[ARGS_VAR] = get_annot(at.varargs)

        if at.varkw:
            map[ARGS_KW] = get_annot(at.varkw)

        if at.kwonlyargs:
            for arg in at.kwonlyargs:
                map[arg] = get_annot(arg)

        if len(missing_args):
            raise NotImplementedError(
                f'Function `{t.method}` `{t.name}` not find annotations for arguments named: `{missing_args}`')

        return map

    def step(self, i: SerdeInst, t: CallableArgsWrapper, ctx: SerdeStepContext) -> SerdeNode:
        if t.cls:
            ctx = build_generic_context(t.cls, ctx)
            ctx = ctx.merge(SerdeStepContext(mod=build_obj_module(t.cls)))

        types = sorted(self._build_args(t).items(), key=lambda x: x[0])

        types = [i.norm(x, ctx) for _, x in types]

        return SerdeNode(t, types, ctx)

    def deserializer(self, t: CallableArgsWrapper, deps: List[DESER]) -> DESER:
        at = t.spec
        map = self._build_args(t)
        map = sorted(map.items(), key=lambda x: x[0])
        map = zip(map, enumerate(deps))
        map = {k: i for (k, _), (i, _) in map}

        def get_map(name):
            return deps[map[name]]

        def callable_deserializer(val: list) -> list:
            # given a val, pair the values with the types

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
                    r_args = r_args + (get_map(ARGS_VAR)(arg),)
                else:
                    raise ValueError(f'Could not find mapping for argument `{arg}`')

            for kwarg_name, kwarg_val in kwargs.items():
                if kwarg_name not in map:
                    if at.varkw:
                        r_kwargs[kwarg_name] = get_map(ARGS_KW)(kwarg_val)
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
                    r_args = r_args + (get_map(ARGS_VAR)(arg),)
                else:
                    raise ValueError(f'Could not find mapping for argument `{arg}`')

            for kwarg_name, kwarg_val in kwargs.items():
                if kwarg_name not in map:
                    if at.varkw:
                        r_kwargs[kwarg_name] = get_map(ARGS_KW)(kwarg_val)
                    else:
                        raise ValueError(f'Function does not accept `{kwarg_name}`')
                else:
                    r_kwargs[kwarg_name] = get_map(kwarg_name)(kwarg_val)

            return [r_args, r_kwargs]

        return callable_serializer


class CallableRetSerde(SerdeType):
    def match(self, t: Any) -> bool:
        return isinstance(t, CallableRetWrapper)

    def step(self, i: SerdeInst, t: CallableRetWrapper, ctx: SerdeStepContext) -> SerdeNode:
        if t.cls:
            ctx = build_generic_context(t.cls, ctx)
            ctx = ctx.merge(SerdeStepContext(mod=build_obj_module(t.cls)))

        RET = 'return'

        dt = None

        if RET in t.spec.annotations:
            dt = i.norm(t.spec.annotations[RET], ctx)
        return SerdeNode(t, [dt], ctx)

    def deserializer(self, t: CallableArgsWrapper, deps: List[DESER]) -> DESER:
        def callable_ret_deser(val):
            return deps[0](val)

        return callable_ret_deser

    def serializer(self, t: CallableArgsWrapper, deps: List[SER]) -> SER:
        def callable_ret_ser(val):
            return deps[0](val)

        return callable_ret_ser
