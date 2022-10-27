#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t crc32_ppc(uint64_t crc, unsigned char const *buffer,
                           size_t len);

#ifdef __cplusplus
}
#endif
