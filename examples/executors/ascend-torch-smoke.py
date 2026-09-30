#!/usr/bin/env python3
"""Minimal real Ascend operation used by the Plurifold hardware smoke test."""

import json
import sys

import torch
import torch_npu  # noqa: F401

index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
device = f"npu:{index}"
torch.npu.set_device(device)
x = torch.ones(1024, dtype=torch.float32, device=device)
y = (x * 3.0).sum()
torch.npu.synchronize()
value = float(y.cpu().item())
assert value == 3072.0, value
print(json.dumps({"backend": "cann-torch", "device": index, "value": value, "verified": True}, separators=(",", ":")))
