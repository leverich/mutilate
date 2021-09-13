#ifndef BINARY_PROTOCOL_H
#define	BINARY_PROTOCOL_H

#define CMD_GET  0x00
#define CMD_GETQ 0x09
#define CMD_TOUCH 0x1c
#define CMD_DELETE 0x04
#define CMD_SET  0x01
#define CMD_NOOP 0x0a
#define CMD_SETQ 0x11
#define CMD_SASL 0x21

#define RESP_OK 0x00
#define RESP_NOT_FOUND 0x01
#define RESP_SASL_ERR 0x20

typedef struct {
  uint8_t magic;
  uint8_t opcode;
  uint16_t key_len;
  uint8_t extra_len;
  uint8_t data_type;
  uint16_t status;  // response use
  uint32_t body_len;
  uint32_t opaque;
  uint64_t cas;

} binary_header_t;

#endif /* BINARY_PROTOCOL_H */
