import hashlib

def get_partition(value,parts):
    return int(hashlib.md5(str(value).encode("utf-8")).hexdigest(), parts) % parts

def myHash(text,value):
  hash=0
  for ch in text:
    hash = ( hash*281  ^ ord(ch)*997) & 0xFFFFFFFF
  return hash % value