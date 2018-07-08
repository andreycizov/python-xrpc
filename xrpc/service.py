import datetime
from typing import NamedTuple, Any, Optional

from xrpc.const import SERVER_SERDE_INST
from xrpc.transform import RPCS, get_rpc
from xrpc.serde.abstract import SerdeStruct, SerdeSet
from xrpc.serde.types import CallableArgsWrapper, CallableRetWrapper


class ServiceDefn(NamedTuple):
    serde: SerdeStruct
    rpcs: RPCS
    rpcs_serde: Any

    @classmethod
    def from_obj(cls, obj, override_method=False):
        rpcs = get_rpc(obj)
        rpcs_serde = {}

        serde_set: SerdeSet = SerdeSet.walk(SERVER_SERDE_INST, datetime.datetime, datetime.datetime.__module__)

        for rpc_name, rpc_def in rpcs.items():
            fa = CallableArgsWrapper.from_func(rpc_def.fn)
            fb = CallableRetWrapper.from_func(rpc_def.fn)

            if override_method:
                fa = fa._replace(method=True)
                fb = fb._replace(method=True)

            new_serde_set1 = SerdeSet.walk(
                SERVER_SERDE_INST,
                fa,
                obj.__module__
            )

            new_serde_set2 = SerdeSet.walk(
                SERVER_SERDE_INST,
                fb,
                obj.__module__
            )

            new_serde_set = new_serde_set1.merge(new_serde_set2)

            new_serde_set = serde_set.merge(new_serde_set)

            serde_set = new_serde_set

            rpcs_serde[rpc_name] = [fa, fb]

        serde_struct: SerdeStruct = serde_set.struct(SERVER_SERDE_INST)

        return ServiceDefn(serde_struct, rpcs, rpcs_serde)