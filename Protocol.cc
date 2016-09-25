#include <netinet/tcp.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/dns.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "config.h"

#include "Protocol.h"
#include "Connection.h"
#include "distributions.h"
#include "Generator.h"
#include "mutilate.h"
#include "binary_protocol.h"
#include "util.h"

#define unlikely(x) __builtin_expect((x),0)

/**
 *
 * First we build a RESP Array:
 *  1. * character as the first byte 
 *  2.  the number of elements in the array as a decimal number
 *  3.  CRLF
 *  4. The actual RESP element we are putting into the array
 *
 * All Redis commands are sent as arrays of bulk strings. 
 * For example, the command “SET mykey ‘my value’” would be written and sent as:
 * *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$8\r\nmy value\r\n
 *
 * Then package command as a RESP Bulk String to the server
 *
 * Bulk String is the defined by the following:
 *     1."$" byte followed by the number of bytes composing the 
 *        string (a prefixed length), terminated by CRLF.
 *     2. The actual string data.
 *     3. A final CRLF.
 *
 */
int ProtocolRESP::set_request(const char* key, const char* value, int value_len) {
  
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                          "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 
                          strlen(key),key,strlen(value),value);
  //if (read_state == IDLE) read_state = WAITING_FOR_END;
  return l;

  //size_t req_size = strlen("*3$$$") + 7*strlen("\r\n") + strlen(key) + strlen(value);

  //char *req = (char*)malloc(req_size*(sizeof(char)));
  //snprintf(req,req_size,"*%d\r\n$%dSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
  //              3,3,strlen(key),key,strlen(value),value);
  //
  //bufferevent_write(bev, req, req_size);
  //if (read_state == IDLE) read_state = WAITING_FOR_END;
  //return req_size;
}

/**
 * Send a RESP get request.
 */
int ProtocolRESP::get_request(const char* key) {
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                          "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n",strlen(key),key);
  //if (read_state == IDLE) read_state = WAITING_FOR_END;
  return l;
}

/**
 * Handle a RESP response.
 *
 * In RESP, the type of data depends on the first byte:
 * 
 * Simple Strings the first byte of the reply is "+"
 * Errors the first byte of the reply is "-"
 * Integers the first byte of the reply is ":"
 * Bulk Strings the first byte of the reply is "$"
 * Arrays the first byte of the reply is "*"
 *
 * Right now we are only implementing GET response
 * so the RESP type will be bulk string.
 *
 *
 */
bool ProtocolRESP::handle_response(evbuffer *input, bool &done) {
  char *resp = NULL;
  char *resp_bytes = NULL;
  size_t n_read_out; //first read number of bytes
  size_t m_read_out; //second read number of bytes

  //A bulk string resp: "$6\r\nfoobar\r\n"
  //1. Consume the first "$##\r\n", evbuffer_readln returns
  //   "$##", where ## is the number of bytes in the bulk string
  resp_bytes = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF_STRICT);
  resp_bytes++; //consume the "$"

  //2. Consume the next "foobar"
  resp = evbuffer_readln(input,&m_read_out,EVBUFFER_EOL_CRLF_STRICT);
  
  //make sure we got a results
  if (resp == NULL || resp_bytes == NULL) return false;

  //keep track of recieved stats
  conn->stats.rx_bytes += n_read_out + m_read_out;

  //if nil we have a miss, keep waiting? (check if this is correct action)
  if (strncmp(resp, "nil", 3) {
    conn->stats.get_misses++;
    done = true;
  } else {
    //we have a proper respsone
    done = true;
  }
  evbuffer_drain(input,n_read_out + m_read_out + 4) //4 = 2*(CRLF bytes) 
  free(resp);
  free(resp_bytes);
  return true;
}

/**
 * Send an ascii get request.
 */
int ProtocolAscii::get_request(const char* key) {
  int l;
  l = evbuffer_add_printf(
    bufferevent_get_output(bev), "get %s\r\n", key);
  if (read_state == IDLE) read_state = WAITING_FOR_GET;
  return l;
}

/**
 * Send an ascii set request.
 */
int ProtocolAscii::set_request(const char* key, const char* value, int len) {
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                          "set %s 0 0 %d\r\n", key, len);
  bufferevent_write(bev, value, len);
  bufferevent_write(bev, "\r\n", 2);
  l += len + 2;
  if (read_state == IDLE) read_state = WAITING_FOR_END;
  return l;
}

/**
 * Handle an ascii response.
 */
bool ProtocolAscii::handle_response(evbuffer *input, bool &done) {
  char *buf = NULL;
  int len;
  size_t n_read_out;

  switch (read_state) {

  case WAITING_FOR_GET:
  case WAITING_FOR_END:
    buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
    if (buf == NULL) return false;

    conn->stats.rx_bytes += n_read_out;

    if (!strncmp(buf, "END", 3)) {
      if (read_state == WAITING_FOR_GET) conn->stats.get_misses++;
      read_state = WAITING_FOR_GET;
      done = true;
    } else if (!strncmp(buf, "VALUE", 5)) {
      sscanf(buf, "VALUE %*s %*d %d", &len);

      // FIXME: check key name to see if it corresponds to the op at
      // the head of the op queue?  This will be necessary to
      // support "gets" where there may be misses.

      data_length = len;
      read_state = WAITING_FOR_GET_DATA;
      done = false;
    } else {
      // must be a value line..
      done = false;
    }
    free(buf);
    return true;

  case WAITING_FOR_GET_DATA:
    len = evbuffer_get_length(input);
    if (len >= data_length + 2) {
      evbuffer_drain(input, data_length + 2);
      read_state = WAITING_FOR_END;
      conn->stats.rx_bytes += data_length + 2;
      done = false;
      return true;
    }
    return false;

  default: printf("state: %d\n", read_state); DIE("Unimplemented!");
  }

  DIE("Shouldn't ever reach here...");
}

/**
 * Perform SASL authentication if requested (write).
 */
bool ProtocolBinary::setup_connection_w() {
  if (!opts.sasl) return true;

  string user = string(opts.username);
  string pass = string(opts.password);

  binary_header_t header = {0x80, CMD_SASL, 0, 0, 0, {0}, 0, 0, 0};
  header.key_len = htons(5);
  header.body_len = htonl(6 + user.length() + 1 + pass.length());

  bufferevent_write(bev, &header, 24);
  bufferevent_write(bev, "PLAIN\0", 6);
  bufferevent_write(bev, user.c_str(), user.length() + 1);
  bufferevent_write(bev, pass.c_str(), pass.length());

  return false;
}

/**
 * Perform SASL authentication if requested (read).
 */
bool ProtocolBinary::setup_connection_r(evbuffer* input) {
  if (!opts.sasl) return true;

  bool b;
  return handle_response(input, b);
}

/**
 * Send a binary get request.
 */
int ProtocolBinary::get_request(const char* key) {
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, {htons(0)},
                        htonl(keylen) };

  bufferevent_write(bev, &h, 24); // size does not include extras
  bufferevent_write(bev, key, keylen);
  return 24 + keylen;
}



/**
 * Send a binary set request.
 */
int ProtocolBinary::set_request(const char* key, const char* value, int len) {
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, {htons(0)},
                        htonl(keylen + 8 + len) };

  bufferevent_write(bev, &h, 32); // With extras
  bufferevent_write(bev, key, keylen);
  bufferevent_write(bev, value, len);
  return 24 + ntohl(h.body_len);
}

/**
 * Tries to consume a binary response (in its entirety) from an evbuffer.
 *
 * @param input evBuffer to read response from
 * @return  true if consumed, false if not enough data in buffer.
 */
bool ProtocolBinary::handle_response(evbuffer *input, bool &done) {
  // Read the first 24 bytes as a header
  int length = evbuffer_get_length(input);
  if (length < 24) return false;
  binary_header_t* h =
          reinterpret_cast<binary_header_t*>(evbuffer_pullup(input, 24));
  assert(h);

  // Not whole response
  int targetLen = 24 + ntohl(h->body_len);
  if (length < targetLen) return false;

  // If something other than success, count it as a miss
  if (h->opcode == CMD_GET && h->status) {
      conn->stats.get_misses++;
  }

  if (unlikely(h->opcode == CMD_SASL)) {
    if (h->status == RESP_OK) {
      V("SASL authentication succeeded");
    } else {
      DIE("SASL authentication failed");
    }
  }

  evbuffer_drain(input, targetLen);
  conn->stats.rx_bytes += targetLen;
  done = true;
  return true;
}

