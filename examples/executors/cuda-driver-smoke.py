#!/usr/bin/env python3
"""Minimal real CUDA-driver operation used by the Plurifold hardware smoke test."""

import ctypes
import json

cuda = ctypes.CDLL("libcuda.so.1")

def call(name, *args):
    rc = getattr(cuda, name)(*args)
    if rc != 0:
        raise SystemExit(f"{name} failed with CUDA error {rc}")

call("cuInit", 0)
device = ctypes.c_int()
call("cuDeviceGet", ctypes.byref(device), 0)
context = ctypes.c_void_p()
call("cuCtxCreate_v2", ctypes.byref(context), 0, device)
device_ptr = ctypes.c_uint64()
call("cuMemAlloc_v2", ctypes.byref(device_ptr), ctypes.c_size_t(4096))
call("cuMemsetD32_v2", device_ptr, ctypes.c_uint32(0x3F800000), ctypes.c_size_t(1024))
call("cuCtxSynchronize")
host = (ctypes.c_uint32 * 1024)()
call("cuMemcpyDtoH_v2", host, device_ptr, ctypes.c_size_t(4096))
assert host[0] == 0x3F800000 and host[-1] == 0x3F800000
call("cuMemFree_v2", device_ptr)
call("cuCtxDestroy_v2", context)
print(json.dumps({"backend": "cuda-driver", "device": device.value, "bytes": 4096, "verified": True}, separators=(",", ":")))
