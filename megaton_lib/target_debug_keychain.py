"""Explicit macOS Keychain storage for Target debug diagnostics; no token CLI output."""
import ctypes as C
import ctypes.util
import re
import subprocess
import sys
from functools import lru_cache

@lru_cache(maxsize=1)
def _bindings():
    if sys.platform != "darwin":
        raise RuntimeError("MACOS_KEYCHAIN_REQUIRED")
    sec = C.CDLL(ctypes.util.find_library("Security"))
    cf = C.CDLL(ctypes.util.find_library("CoreFoundation"))
    sec.SecKeychainSetUserInteractionAllowed.argtypes = [C.c_bool]
    sec.SecKeychainSetUserInteractionAllowed.restype = C.c_int32
    sec.SecKeychainSetUserInteractionAllowed(False)
    sec.SecKeychainAddGenericPassword.argtypes = [C.c_void_p, C.c_uint32, C.c_char_p, C.c_uint32, C.c_char_p, C.c_uint32, C.c_void_p, C.POINTER(C.c_void_p)]
    sec.SecKeychainAddGenericPassword.restype = C.c_int32
    sec.SecKeychainFindGenericPassword.argtypes = [C.c_void_p, C.c_uint32, C.c_char_p, C.c_uint32, C.c_char_p, C.POINTER(C.c_uint32), C.POINTER(C.c_void_p), C.POINTER(C.c_void_p)]
    sec.SecKeychainFindGenericPassword.restype = C.c_int32
    sec.SecKeychainItemModifyAttributesAndData.argtypes = [C.c_void_p, C.c_void_p, C.c_uint32, C.c_void_p]
    sec.SecKeychainItemModifyAttributesAndData.restype = C.c_int32
    sec.SecKeychainItemFreeContent.argtypes = [C.c_void_p, C.c_void_p]
    sec.SecKeychainItemFreeContent.restype = C.c_int32
    cf.CFRelease.argtypes = [C.c_void_p]
    cf.CFRelease.restype = None
    return sec, cf


def read_token(service: str, account: str):
    sec, _ = _bindings()
    SERVICE, ACCOUNT = service.encode(), account.encode()
    length, pointer = C.c_uint32(), C.c_void_p()
    code = sec.SecKeychainFindGenericPassword(None, len(SERVICE), SERVICE, len(ACCOUNT), ACCOUNT, C.byref(length), C.byref(pointer), None)
    if code:
        raise RuntimeError(f"KEYCHAIN_READ_STATUS_{code}")
    try:
        value = C.string_at(pointer, length.value).decode("utf-8")
    finally:
        sec.SecKeychainItemFreeContent(None, pointer)
    if not re.fullmatch(r"[A-Za-z0-9._~+/=-]{20,4096}", value):
        raise RuntimeError("KEYCHAIN_TOKEN_FORMAT_INVALID")
    return value


def store_clipboard(service: str, account: str):
    sec, cf = _bindings()
    SERVICE, ACCOUNT = service.encode(), account.encode()
    value = subprocess.run(["pbpaste"], capture_output=True, check=True).stdout.decode().strip()
    if not re.fullmatch(r"[A-Za-z0-9._~+/=-]{20,4096}", value):
        raise RuntimeError("CLIPBOARD_TOKEN_UNAVAILABLE")
    raw = value.encode()
    item = C.c_void_p()
    code = sec.SecKeychainAddGenericPassword(None, len(SERVICE), SERVICE, len(ACCOUNT), ACCOUNT, len(raw), raw, C.byref(item))
    if code == -25299:
        code = sec.SecKeychainFindGenericPassword(None, len(SERVICE), SERVICE, len(ACCOUNT), ACCOUNT, None, None, C.byref(item))
        if code == 0:
            code = sec.SecKeychainItemModifyAttributesAndData(item, None, len(raw), raw)
    if item:
        cf.CFRelease(item)
    if code:
        raise RuntimeError(f"KEYCHAIN_STORE_STATUS_{code}")
    if read_token(service, account) != value:
        raise RuntimeError("KEYCHAIN_READBACK_MISMATCH")
