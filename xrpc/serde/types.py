import base64
import inspect
import logging
import sys
from enum import Enum
from inspect import FullArgSpec

import datetime
import uuid

from xrpc.generic import build_generic_context as _build_generic_context
from xrpc.serde.error import SerdeException


def build_generic_context(*args):
    _, r = _build_generic_context(*args)
    return r


if sys.version_info >= (3, 7):
    from typing import Any, ForwardRef, Optional, Union, List, Dict, Callable, \
        Tuple, TypeVar, Iterable
else:
    from typing import Any, _ForwardRef, Optional, Union, List, Dict, Callable, \
        Tuple, TypeVar, Iterable
from dataclasses import is_dataclass, fields, dataclass

from xrpc.util import time_parse
from xrpc.serde.abstract import SerdeType, SerdeInst, SerdeNode, DESER, SER, SerdeTypeDeserializer, SerdeTypeSerializer, \
    SerdeStepContext

if sys.version_info >= (3, 7):
    FR = ForwardRef
else:
    FR = _ForwardRef


class ForwardRefSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
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


class UnionNext(Exception):
    pass


class UnionDeserializer(SerdeTypeDeserializer):
    def __init__(self, parent: 'UnionSerde', t: Any, deps: List[DESER]):
        super().__init__(parent, t, deps)

        assert hasattr(t, '__args__')

        self.ordered = {k: i for i, (k, v) in enumerate(parent.extract_ordered(t))}

    def __call__(self, val: Union[list, dict]) -> Any:
        idx, val = val

        dep_idx = self.ordered.get(idx)

        if dep_idx is None:
            raise SerdeException(val, 'u_idx', idx)

        dep = self.deps[dep_idx]

        try:
            return dep(val)
        except Exception as e:
            raise SerdeException(val, 'u_deps', idx=dep_idx, par=e)


class UnionSerializer(UnionDeserializer):
    def __call__(self, val):
        desc = None

        desc = self.parent.descriptor_fn(True, val)
        nv = desc, val
        try:
            return desc, super().__call__(nv)
        except Exception as e:
            raise SerdeException(val, 'union_ser', desc=desc, par=e)


def descriptor_classname(is_inst, t):
    if not is_inst:
        return t.__name__
    else:
        return t.__class__.__name__


def is_union(t):
    if sys.version_info >= (3, 7):
        return getattr(t, '__origin__', None) is Union
    else:
        return t.__class__.__name__ == '_Union'


class UnionSerde(SerdeType):
    cls_deserializer = UnionDeserializer
    cls_serializer = UnionSerializer

    def __init__(self, descriptor_fn=None):
        if descriptor_fn is None:
            descriptor_fn = descriptor_classname
        self.descriptor_fn = descriptor_fn
        super().__init__()

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        r = is_union(t)

        if r:
            sub_args = t.__args__

            if sub_args[-1] == type(None):
                return False
            elif len(sub_args) == 1:
                return False
            else:
                return True
        else:
            return False

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        st = tuple(i.norm(x, ctx) for x in t.__args__)
        return Union[st]

    def extract_ordered(self, t: Any):
        vt = t.__args__

        ordered = [(self.descriptor_fn(False, x), i) for i, x in enumerate(vt)]
        ordered = sorted(ordered)
        seen = set()

        for n, idx in ordered:
            if n in seen:
                assert False, (n, 'Could not unionize types with the same name')
            seen.add(n)

        return tuple((n, vt[idx]) for n, idx in ordered)

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        ordered = self.extract_ordered(Union[tuple(i.norm(x, ctx) for x in t.__args__)])

        return SerdeNode(Union[tuple(x for _, x in ordered)], [t for _, t in ordered], ctx)


class OptionalDeserializer(SerdeTypeDeserializer):
    def __call__(self, val: Union[list, dict]) -> Any:
        fn = self.deps[0]
        try:
            if val is None:
                return None
            else:
                return fn(val)
        except Exception as e:
            raise SerdeException(val, 'opt', fn=fn, par=e)


class OptionalSerde(SerdeType):
    cls_deserializer = OptionalDeserializer
    cls_serializer = OptionalDeserializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        r = is_union(t)

        if r:
            sub_args = t.__args__

            if sub_args[-1] == type(None) and len(sub_args) == 2:
                return True
            elif len(sub_args) == 1:
                return False
            else:
                return sub_args[-1] == type(None)
        else:
            return False

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        *args, _ = t.__args__
        args = tuple(i.norm(x, ctx) for x in args)

        if len(args) > 1:
            return Optional[Union[args]]
        else:
            return Optional[args[0]]

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        *args, _ = t.__args__

        args = tuple(i.norm(x, ctx) for x in args)

        deps = []

        if len(args) > 1:
            deps = [Union[args]]
        else:
            deps = [args[0]]

        return SerdeNode(self.norm(i, t, ctx), deps, ctx)


class AtomDeserializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)
        self.mapper, *_ = [x for x in self.parent.ATOMS if t == x]

    def __call__(self, val: Any) -> Union[list, dict]:
        try:
            return self.mapper(val)
        except Exception as e:
            raise SerdeException(val, 'a_dv', t=self.mapper.__name__, par=e)


class AtomSerializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)
        self.mapper, *_ = [x for x in self.parent.ATOMS if t == x]

    def __call__(self, val: Any) -> Union[list, dict]:
        if not isinstance(val, self.mapper):
            raise SerdeException(val, 'a_sv', t=self.mapper.__name__)

        return val


class AtomSerde(SerdeType):
    ATOMS = (int, str, bool, float)

    cls_deserializer = AtomDeserializer
    cls_serializer = AtomSerializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        return t in self.ATOMS

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])


class WalkableAtomObjDeserializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)

    def __call__(self, val: Any) -> Union[list, dict]:
        return val


class WalkableAtomObjSerializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)

    def __call__(self, val: Any) -> Union[list, dict]:
        return val


class WalkableAtomObjSerde(SerdeType):
    ATOMS = (list, dict)

    cls_deserializer = WalkableAtomObjDeserializer
    cls_serializer = WalkableAtomObjSerializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        r = any(isinstance(t, x) for x in self.ATOMS)
        r = r or any(issubclass(t, x) for x in self.ATOMS)
        return r

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        z = ([x for x in self.ATOMS if isinstance(t, x)] + [t for x in self.ATOMS if issubclass(t, x)])[0]

        return SerdeNode(z, [])


class BytesDeserializer(SerdeTypeDeserializer):
    def __call__(self, val: Union[list, dict, str]) -> Any:
        return base64.b64decode(val.encode())


class BytesSerializer(SerdeTypeSerializer):
    def __call__(self, val: Any) -> Union[list, dict]:
        return base64.b64encode(val).decode()


class BytesSerde(SerdeType):
    cls_deserializer = BytesDeserializer
    cls_serializer = BytesSerializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if inspect.isclass(t):
            return issubclass(t, bytes)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])


class TypeVarSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        return isinstance(t, TypeVar)

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        if t in ctx.generic_vals:
            return ctx.generic_vals[t]
        else:
            raise ValueError(f'Only instantated generics are allowed for serialization {t} {ctx}')

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return i.step(self.norm(i, t, ctx), ctx)


class NoneSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if t == type(None):
            return True
        if t is None:
            return True
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
        def ser(val):
            assert val is None, val
            return None
        return ser


class UUIDSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if inspect.isclass(t):
            return issubclass(t, uuid.UUID)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        def uuid_serializer(val: uuid.UUID):
            return val.hex

        return uuid_serializer

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: uuid.UUID(hex=val)


class TimeDeltaSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if inspect.isclass(t):
            return issubclass(t, datetime.timedelta)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: datetime.timedelta(seconds=val)

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: val.total_seconds()


ISO8601 = '%Y-%m-%dT%H:%M:%S.%f'
ISO8601_DATE = '%Y-%m-%d'


class DateTimeSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
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


class DateSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if inspect.isclass(t):
            return issubclass(t, datetime.date)
        else:
            return False

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: time_parse(val, ISO8601_DATE).date()

    def serializer(self, t: Any, deps: List[DESER]) -> DESER:
        return lambda val: format(val, ISO8601_DATE)


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

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        t, _ = _build_generic_context(t, ctx)

        if sys.version_info >= (3, 7):
            if hasattr(t, '__origin__'):
                return t.__origin__ is list

        if inspect.isclass(t):
            return issubclass(t, List)
        else:
            return False

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        _, ctx = _build_generic_context(t, ctx)
        st, = t.__args__
        st = i.norm(st, ctx)

        return List[st]

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        t = i.norm(t, ctx)
        return SerdeNode(t, [t.__args__[0]], ctx=ctx)


class TupleDeserializer(ListDeserializer):
    cls_coll = tuple

    def __call__(self, val: Union[list, dict]) -> Any:
        self.d, *_ = self.deps

        if len(val) != len(self.deps):
            raise SerdeException(val, 'tpl_len')

        return tuple(d(x) for d, x in zip(self.deps, val))


class TupleSerializer(TupleDeserializer, SerdeTypeSerializer):

    def __call__(self, val: Union[list, dict]) -> Any:

        if len(val) != len(self.deps):
            raise SerdeException(val, 'tpl_len')

        return [d(x) for d, x in zip(self.deps, val)]


class TupleSerde(SerdeType):
    cls_deserializer = TupleDeserializer
    cls_serializer = TupleSerializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        t, _ = _build_generic_context(t, ctx)

        if sys.version_info >= (3, 7):
            if hasattr(t, '__origin__'):
                return t.__origin__ is tuple

        if inspect.isclass(t):
            return issubclass(t, Tuple)
        else:
            return False

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        _, ctx = _build_generic_context(t, ctx)
        args = tuple(i.norm(st, ctx) for st in t.__args__)

        return Tuple[args]

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        t = i.norm(t, ctx)
        return SerdeNode(t, [x for x in t.__args__], ctx=ctx)


@dataclass(eq=True, frozen=True)
class AnyObjCls:
    pass


AnyObj = AnyObjCls()


class AnySerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if inspect.isclass(t):
            try:
                return issubclass(t, Any)
            except TypeError:
                return True
        else:
            try:
                return isinstance(t, Any)
            except TypeError:
                return True

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        t = t if inspect.isclass(t) else t.__class__

        return SerdeNode(t, [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        def deser_any(val):
            return val

        return deser_any

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        def ser_any(val):
            return val

        return ser_any


class TypeSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        return isinstance(t, type)

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        return SerdeNode((type, t), [])

    def deserializer(self, t: Any, deps: List[DESER]) -> DESER:
        def deser_dict(val):
            return str(val)

        return deser_dict

    def serializer(self, t: Any, deps: List[SER]):
        def deser_dict(val):
            return str(val)

        return deser_dict


class DictSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        t, _ = _build_generic_context(t, ctx)

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
                its = val.items()
            except BaseException as e:
                raise SerdeException(val, 'dict_a', t=t, par=e)
            try:
                for k, v in its:
                    r[deps[0](k)] = deps[1](v)
            except BaseException as e:
                raise SerdeException(val, 'dict_b', t=t, par=e, k=k, v=v)
            return r

        return deser_dict

    def serializer(self, t: Any, deps: List[SER]) -> SER:
        def ser_dict(val):
            ks, vs = deps
            r = {}
            k, v = None, None
            try:
                for k, v in val.items():
                    # either way (even if a field is non-optional), that field must not accept None as it's argument
                    ksk = ks(k)
                    vsv = vs(v)
                    r[ksk] = vsv
                    # r[f] = self.deps[i](val[f])
            except (TypeError, Exception) as e:
                raise SerdeException(val, 'u_sf', idx=k, field=v, par=e)

            return r

        return ser_dict


class EnumSerde(SerdeType):
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
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
        self.fields = sorted([f for f, _ in t._field_types.items()])

    def __call__(self, val: Union[list, dict]) -> Any:
        r = {}

        i, f = None, None

        try:

            for i, f in enumerate(self.fields):
                # either way (even if a field is non-optional), that field must not accept None as it's argument
                r[f] = self.deps[i](val.get(f))
                # r[f] = self.deps[i](val[f])
        except Exception as e:
            raise SerdeException(val, 'u_sf', idx=i, field=f, par=e)

        try:

            return self.t(**r)
        except Exception as e:
            raise SerdeException(val, 'u_inst', fields=r, par=e)


class NamedTupleSerializer(SerdeTypeSerializer):
    def __init__(self, parent: 'SerdeType', t: Any, deps: List[SER]):
        super().__init__(parent, t, deps)
        self.fields = sorted([f for f, _ in t._field_types.items()])

    def __call__(self, val: Any) -> Union[list, dict]:
        return {v: self.deps[k](getattr(val, v)) for k, v in enumerate(self.fields)}


class NamedTupleSerde(SerdeType):
    cls_deserializer = NamedTupleDeserializer
    cls_serializer = NamedTupleSerializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
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
        return SerdeNode(t, [i.norm(st, ctx) for f, st in sorted(xt._field_types.items())], ctx=ctx)


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


class DataclassSerde(SerdeType):
    cls_deserializer = DataclassDeserializer
    cls_serializer = DataclassSerializer

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                t = t.__origin__
        return is_dataclass(t)

    def norm(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> Any:
        t, ctx = _build_generic_context(t, ctx)
        return t

    def step(self, i: SerdeInst, t: Any, ctx: SerdeStepContext) -> SerdeNode:
        # todo: here, we may need to instantiate the generic items
        t, ctx = _build_generic_context(t, ctx)

        xt = t

        if sys.version_info >= (3, 7):
            is_gen = hasattr(t, '__origin__')

            if is_gen:
                xt = t.__origin__

        logging.getLogger(__name__ + '.DataclassSerde.step').debug('%s %s', t, ctx)

        return SerdeNode(i.norm(t, ctx), [i.norm(f.type, ctx) for f in sorted(fields(xt), key=lambda x: x.name)], ctx)


def _get_class_that_defined_method(meth):
    if inspect.ismethod(meth):
        for cls in inspect.getmro(meth.__self__.__class__):
            if cls.__dict__.get(meth.__name__) is meth:
                return cls
        meth = meth.__func__  # fallback to __qualname__ parsing
    if inspect.isfunction(meth):
        cls = getattr(inspect.getmodule(meth),
                      meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0])
        if isinstance(cls, type):
            return cls
    return getattr(meth, '__objclass__', None)  # handle special descriptor objects


def _build_fn_name(fn):
    name = fn.__module__

    if hasattr(fn, '__class__'):
        name += '.' + _get_class_that_defined_method(fn).__name__

    name += '.' + fn.__name__

    return name


@dataclass()
class ArgsWrapper:
    method: bool
    name: str
    spec: FullArgSpec
    cls: Optional[Any] = None

    @classmethod
    def from_func(cls, fn: Callable):
        assert not inspect.ismethod(fn)

        return cls(inspect.ismethod(fn), _build_fn_name(fn), inspect.getfullargspec(fn))

    @classmethod
    def from_func_cls(cls, cls_obj: Any, fn: Callable):
        assert not inspect.ismethod(fn)

        return cls(True, _build_fn_name(fn), inspect.getfullargspec(fn), cls_obj)

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return self._get_comparable() == other._get_comparable()
        else:
            return False

    def _get_comparable(self):
        r = (
            self.__class__.__name__,
            # self.name,
            # self.method,
            tuple(self.spec.args),
            self.spec.varargs,
            self.spec.varkw,
            self.spec.defaults,
            tuple(self.spec.kwonlyargs),
            None if self.spec.kwonlydefaults is None else tuple(sorted(self.spec.kwonlydefaults.items())),
            None if self.spec.annotations is None else tuple(sorted(self.spec.annotations.items())),
            # self.cls
        )
        return r

    def __hash__(self) -> int:
        r = self._get_comparable()
        return hash(r)


class CallableArgsWrapper(ArgsWrapper):
    pass


class CallableRetWrapper(ArgsWrapper):
    pass


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


EMPTY_DEFAULT = object()


class ArgumentsException(Exception):
    def __init__(
            self,
            is_pos=False,
            is_pos_many=False,
            name_missing: Optional[List[str]] = None,
            argument_required: Optional[List[str]] = None,
    ):
        self.is_pos = is_pos
        self.is_pos_many = is_pos_many
        self.name_missing = name_missing
        self.argument_required = argument_required
        super().__init__(is_pos, name_missing, argument_required)

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.is_pos}, {self.is_pos_many}, {self.name_missing}, {self.argument_required})'


@dataclass
class PairSpecValue:
    name: str
    name_idx: Optional[Union[str, int]]
    arg: Any
    """Given argument to the function"""
    default_arg: Any


@dataclass
class PairSpec:
    spec: FullArgSpec
    is_method: bool

    def __call__(self, *args, **kwargs) -> Iterable[PairSpecValue]:
        spec = self.spec
        is_method = self.is_method
        # pair function arguments to their names

        # we need to "eat" the arguments correctly (and then map them to something else).
        map_args = list(spec.args)
        map_args_defaults = list(spec.defaults or [])
        map_args_defaults = [EMPTY_DEFAULT] * (len(map_args) - len(map_args_defaults)) + map_args_defaults

        kwonlyargs = list(spec.kwonlyargs) if spec.kwonlyargs else []
        kwonlydefaults = spec.kwonlydefaults or {}

        mapped_args = list()

        if is_method:
            assert map_args[0] == 'self'
            map_args = map_args[1:]

        vararg_ctr = 0

        names_missing = []

        for arg in args:
            if len(map_args):
                curr_arg, map_args = map_args[0], map_args[1:]
                curr_default_arg, map_args_defaults = map_args_defaults[0], map_args_defaults[1:]

                yield PairSpecValue(curr_arg, None, arg, curr_default_arg)

                mapped_args.append(curr_arg)
            elif spec.varargs:
                yield PairSpecValue(ARGS_VAR, vararg_ctr, arg, None)
                vararg_ctr += 1

            else:
                raise ArgumentsException(is_pos=True, is_pos_many=True, name_missing=[arg])

        matched_kwds = []

        for kwarg_name, kwarg_val in kwargs.items():
            has_matched = False
            default_value = EMPTY_DEFAULT

            if kwarg_name in map_args:
                map_args_idx = map_args.index(kwarg_name)

                has_matched = True
                default_value = map_args_defaults[map_args_idx]

                map_args = map_args[:map_args_idx] + map_args[map_args_idx + 1:]
                map_args_defaults = map_args_defaults[:map_args_idx] + map_args_defaults[map_args_idx + 1:]

            elif kwarg_name in kwonlyargs:
                kwonlyargs.remove(kwarg_name)
                has_matched = True
                default_value = kwonlydefaults.get(kwarg_name, EMPTY_DEFAULT)

            if not has_matched:
                if spec.varkw:
                    assert kwarg_name not in mapped_args, kwarg_name
                    yield PairSpecValue(ARGS_KW, kwarg_name, kwarg_val, None)
                else:
                    names_missing.append(kwarg_name)
                    continue
            else:
                yield PairSpecValue(kwarg_name, None, kwarg_val, default_value)

            matched_kwds.append(kwarg_name)

        reqd = []
        is_pos = True

        for v in kwonlyargs:
            if v in kwonlydefaults:
                is_pos = False
                yield PairSpecValue(ARGS_KW, v, EMPTY_DEFAULT, kwonlydefaults[v])
            else:
                reqd.append(v)

        for x, y in zip(map_args, map_args_defaults):
            if y is EMPTY_DEFAULT:
                reqd.append(y)
            else:
                yield ARGS_KW, x, EMPTY_DEFAULT, y

        if len(reqd):
            raise ArgumentsException(is_pos=is_pos, argument_required=reqd)


def pair_spec(spec: FullArgSpec, is_method, *args, **kwargs):
    return PairSpec(spec, is_method)(*args, **kwargs)


ARGS_VAR = '$var'
ARGS_KW = '$kw'
ARGS_RET = '$ret'


class CallableArgsSerde(SerdeType):
    # fullargspect -> Type (or a function)
    # args_kwargs -> INPUT

    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
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
                f'Function `{t}` `{t.name}` not find annotations for arguments named: `{missing_args}`')

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
    def match(self, t: Any, ctx: SerdeStepContext) -> bool:
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
            dep = deps[0]

            try:
                return dep(val)
            except Exception as e:
                raise SerdeException(val=val, code='ex', par=e)

        return callable_ret_deser

    def serializer(self, t: CallableArgsWrapper, deps: List[SER]) -> SER:
        def callable_ret_ser(val):
            dep = deps[0]

            try:
                return dep(val)
            except Exception as e:
                raise SerdeException(val=val, code='ex', par=e)

        return callable_ret_ser
