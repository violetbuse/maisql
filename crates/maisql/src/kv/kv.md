
# Kv File Header Format

All bytes are encoded as big endian.

|4-byte magic string. "kv/1". 0x6b, 0x76, 0x2f, 0x31|
|4-byte u32 root page pointer |
|80-byte array of 20 u32 free page pointers|
|12 bytes of future space|

# Kv File Page Format

|page_type 1-byte; 0 if root; 1 if branch; 2 if leaf;|
|parent_page 4-byte|
|lt child pointer 4 byte|
40 key | cursor | value tuples
|key 32 bytes|child pointer 4 byte|value 66 bytes Nil terminated|
|7 bytes empty space|
