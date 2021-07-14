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
 * DBG code
 *   fprintf(stderr,"--\n");
 *   fprintf(stderr,"*3\r\n$3\r\nSET\r\n$%lu\r\n%s\r\n$%d\r\n%s\r\n", 
 *                           strlen(key),key,len,val);
 *   fprintf(stderr,"--\n");
 *
 */
int ProtocolRESP::set_request(const char* key, const char* value, int len, uint32_t opaque) {
 
  //need to make the real value
  char *val = (char*)malloc(len*sizeof(char)+1);
  memset(val, 'a', len);
  val[len] = '\0';

  //check if we should use assoc
  if (opts.use_assoc && strlen(key) > ((unsigned int)(opts.assoc+1)) )
  {    
      int l = hset_request(key,val,len);
      free(val);
      return l;
  }

  else
  {
    int l;
    l = evbuffer_add_printf(bufferevent_get_output(bev),
                            "*3\r\n$3\r\nSET\r\n$%lu\r\n%s\r\n$%d\r\n%s\r\n", 
                            strlen(key),key,len,val);
    l += len + 2;
    if (read_state == IDLE) read_state = WAITING_FOR_GET;
    free(val);
    return l;
  }

}

/**
 * Send a RESP get request.
 */
int ProtocolRESP::get_request(const char* key, uint32_t opaque) {
  
  //check if we should use assoc
  if (opts.use_assoc && strlen(key) > ((unsigned int)(opts.assoc+1)) )
      return hget_request(key);
  else
  {
    int l;
    l = evbuffer_add_printf(bufferevent_get_output(bev),
                            "*2\r\n$3\r\nGET\r\n$%lu\r\n%s\r\n",strlen(key),key);

    if (read_state == IDLE) read_state = WAITING_FOR_GET;
    return l;
  }
}

/**
 * RESP HSET
 * HSET myhash field1 "Hello"
 * We break the key by last assoc bytes for now...
 * We are guarenteed a key of at least assoc+1 bytes...but
 * the vast vast majority are going to be 20 bytes.
 * 
 * DBG code
 * fprintf(stderr,"--\n");
 * fprintf(stderr,"*4\r\n$4\r\nHSET\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n$%d\r\n%s\r\n",
 *           strlen(hash),hash,strlen(field),field,len,value);
 * fprintf(stderr,"--\n");
 */

int ProtocolRESP::hset_request(const char* key, const char* value, int len) {
  
  int l;
  //hash is first n-assoc bytes
  //field is last assoc bytes
  //value is value
  int assoc = opts.assoc;
  char* hash = (char*)malloc(sizeof(char)*((strlen(key)-assoc)+1));
  char* field = (char*)malloc(sizeof(char)*(assoc+1)); 
  strncpy(hash, key, strlen(key)-assoc);
  strncpy(field,key+strlen(key)-assoc,assoc);
  hash[strlen(key)-assoc] = '\0';
  field[assoc] = '\0';
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                          "*4\r\n$4\r\nHSET\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n$%d\r\n%s\r\n",
                          strlen(hash),hash,strlen(field),field,len,value);
  l += len + 2;
  if (read_state == IDLE) read_state = WAITING_FOR_END;
  free(hash);
  free(field);
  return l;

}

/**
 * RESP HGET
 * HGET myhash field1
 * We break the key by last assoc bytes for now...
 * We are guarenteed a key of at least assoc+1 bytes...but
 * the vast vast majority are going to be 20 bytes.
 */
int ProtocolRESP::hget_request(const char* key) {
  int l;
  //hash is first n-assoc bytes
  //field is last assoc bytes
  int assoc = opts.assoc;
  char* hash = (char*)malloc(sizeof(char)*((strlen(key)-assoc)+1));
  char* field = (char*)malloc(sizeof(char)*(assoc+1));
  strncpy(hash, key, strlen(key)-assoc);
  strncpy(field,key+strlen(key)-assoc,assoc);
  hash[strlen(key)-assoc] = '\0';
  field[assoc] = '\0';
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                          "*3\r\n$4\r\nHGET\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n",
                          strlen(hash),hash,strlen(field),field);

  if (read_state == IDLE) read_state = WAITING_FOR_GET;
  free(hash);
  free(field);
  return l;
}

/**
 * RESP DELETE 90 - delete 90 percent of keys in DB
 */
int ProtocolRESP::delete90_request() {
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                            "*1\r\n$8\r\nFLUSHALL\r\n");

  if (read_state == IDLE) read_state = WAITING_FOR_DELETE;
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
bool ProtocolRESP::handle_response(evbuffer *input, bool &done, bool &found, int &obj_size, uint32_t &opaque) {
  opaque = 0;

  char *buf = NULL;
  char *databuf = NULL;
  char *obj_size_str = NULL;
  int len;
  size_t n_read_out;

  switch (read_state) {

  case WAITING_FOR_GET:
    
    buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
    if (buf == NULL) return false;

    obj_size_str = buf+1;
    obj_size = atoi(obj_size_str);

    conn->stats.rx_bytes += n_read_out;
    
    databuf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
    //fprintf(stderr,"--------------------\n");
    //fprintf(stderr,"resp size %lu\n",n_read_out);
    //fprintf(stderr,"data size %d\n",obj_size);
    //fprintf(stderr,"-------header---------\n");
    //fprintf(stderr,"%s\n",buf);
    //fprintf(stderr,"-------data-----------\n");
    //fprintf(stderr,"%s\n",databuf);

    conn->stats.rx_bytes += n_read_out;

    if (!strncmp(buf,"$-1",3)) {
      conn->stats.get_misses++;
      conn->stats.window_get_misses++;
      found = false; 
      done = true;
    } else if ((int)n_read_out != obj_size) {
     

      // FIXME: check key name to see if it corresponds to the op at
      // the head of the op queue?  This will be necessary to
      // support "gets" where there may be misses.

      data_length = obj_size;
      read_state = WAITING_FOR_GET_DATA;
      done = false;
    } else if (!strncmp(buf,"+OK",3) || !strncmp(buf,":1",2) || !strncmp(buf,":0",2) ) {
      found = false;
      done = true;
    } else {
      // got all the data..
      found = true;
      done = true;
    }
    if (databuf)
        free(databuf);
    free(buf);
    return true;

  case WAITING_FOR_GET_DATA:
    
    len = evbuffer_get_length(input);
    
    //finally got all data...
    if (len >= data_length + 2) {
      evbuffer_drain(input, data_length + 2);
      conn->stats.rx_bytes += data_length + 2;
      read_state = WAITING_FOR_GET;
      obj_size = data_length;
      found = true;
      done = true;
      return true;
    }
    return false;

  default: printf("state: %d\n", read_state); DIE("Unimplemented!");
  }

  DIE("Shouldn't ever reach here...");
}

  //char *buf = NUL; //for initial readline
  //char *dbuf = NULL; //for data readline
  //size_t n_read_out;

  //buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
  //if (buf == NULL) 
  //{
  //    done = false;
  //    return false;
  //}
  //conn->stats.rx_bytes += n_read_out;
  //
  //size_t len = evbuffer_get_length(input);

  //fprintf(stderr,"--------------------\n");
  //fprintf(stderr,"resp size %lu\n",n_read_out);
  //fprintf(stderr,"ev len %lu\n",len);
  //fprintf(stderr,"--------------------\n");
  //fprintf(stderr,"%s\n",buf);
  ////RESP null response => miss
  //if (!strncmp(buf,"$-1",3)) 
  //{
  //  conn->stats.get_misses++;
  //  conn->stats.window_get_misses++;
  //  found = false;
  //  
  //}
  ////HSET or SET response was good, just consume the input and move on
  ////with our lives
  //else if (!strncmp(buf,"+OK",3) || !strncmp(buf,":1",2) || !strncmp(buf,":0",2) )
  //{
  //    found = false;
  //    done = true;
  //}
  ////else we got a hit
  //else
  //{
  //  char* nlen = buf+1;
  //  //fprintf(stderr,"%s\n",nlen);
  //  obj_size = atoi(nlen);
  //  // Consume the next "foobar"
  //  //size_t len = evbuffer_get_length(input);
  //  //dbuf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
  //  //if (!dbuf)
  //  //{
  //  //    fprintf(stderr,"--------------------\n");
  //  //    fprintf(stderr,"next foobar (null) %lu\n",n_read_out);
  //  //    fprintf(stderr,"ev len %lu\n",len);
  //  //    fprintf(stderr,"--------------------\n");
  //  //    fprintf(stderr,"%s\n",dbuf);

  //  //    //read_state = WAITING_FOR_GET_DATA;
  //  //    //done = false;
  //  //    //return false;
  //  //}
  //  //else
  //  //{

  //  //    fprintf(stderr,"--------------------\n");
  //  //    fprintf(stderr,"next foobar (null) %lu\n",n_read_out);
  //  //    fprintf(stderr,"ev len %lu\n",len);
  //  //    fprintf(stderr,"--------------------\n");
  //  //    fprintf(stderr,"%s\n",dbuf);
  //  //}

  //  //conn->stats.rx_bytes += n_read_out; 
  //  found = true;
  //}    
  ////read_state = WAITING_FOR_GET;
  ////fprintf(stderr,"--------------------\n");
  ////fprintf(stderr,"read_state %u\n",read_state);
  ////fprintf(stderr,"--------------------\n");
  //done = true;
  ////if (dbuf) 
  ////    free(dbuf);
  //free(buf);
  //return true;

  
//}

/**
 * Send an ascii get request.
 */
int ProtocolAscii::get_request(const char* key, uint32_t opaque) {
  int l;
  l = evbuffer_add_printf(
    bufferevent_get_output(bev), "get %s\r\n", key);
  if (read_state == IDLE) {
      read_state = WAITING_FOR_GET;
  } 
  return l;
}

/**
 * Send an ascii set request.
 */
int ProtocolAscii::set_request(const char* key, const char* value, int len, uint32_t opaque) {
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                          "set %s 0 0 %d\r\n", key, len);
  
  char *val = (char*)malloc(len*sizeof(char)+1);
  memset(val, 'a', len);
  val[len] = '\0';
  
  bufferevent_write(bev, val, len);
  bufferevent_write(bev, "\r\n", 2);
  l += len + 2;
  if (read_state == IDLE) {
      read_state = WAITING_FOR_END;
  }
  free(val);
  return l;
}

/** WARNING UNIMPLEMENTED **/
int ProtocolAscii::delete90_request() {
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                            "*1\r\n$8\r\nFLUSHALL\r\n");

  return l;
}

/**
 * Handle an ascii response.
 */
bool ProtocolAscii::handle_response(evbuffer *input, bool &done, bool &found, int &obj_size, uint32_t &opaque) {
  opaque = 0;
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
      if (read_state == WAITING_FOR_GET) {
          conn->stats.get_misses++;
          conn->stats.window_get_misses++;
          found = false;
      }
      read_state = WAITING_FOR_GET;
      done = true;
    } else if (!strncmp(buf, "STORED", 6)) {
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

  /*
  case WAITING_FOR_GETSET:
    buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
    if (buf == NULL) return false;

    conn->stats.rx_bytes += n_read_out;
    if (!strncmp(buf, "END", 3)) {
        conn->stats.get_misses++;
        conn->stats.window_get_misses++;
        found = false;
        done = true;
        read_state = WAITING_FOR_SET;
        return true;
    } else if (!strncmp(buf, "STORED", 6)) {
        done = true;
        read_state = WAITING_FOR_GET;
        return true;
    }
    
  
  case WAITING_FOR_SET:
    buf = evbuffer_readln(input, &n_read_out, EVBUFFER_EOL_CRLF);
    if (buf == NULL) return false;

    conn->stats.rx_bytes += n_read_out;

    if (!strncmp(buf, "STORED", 6)) {
        done = true;
        read_state = IDLE;
        return true;
    } else {
        done = false;
        return true;
    }
    */

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

  bool b,c;
  int obj_size;
  uint32_t opaque;
  return handle_response(input, b, c, obj_size, opaque);
}

/**
 * Send a binary get request.
 */
int ProtocolBinary::get_request(const char* key, uint32_t opaque) {
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_GET, htons(keylen),
                        0x00, 0x00, {htons(0)},
                        htonl(keylen) };
  h.opaque = htonl(opaque);

  bufferevent_write(bev, &h, 24); // size does not include extras
  bufferevent_write(bev, key, keylen);
  return 24 + keylen;
}



/**
 * Send a binary set request.
 */
int ProtocolBinary::set_request(const char* key, const char* value, int len, uint32_t opaque) {
  uint16_t keylen = strlen(key);

  // each line is 4-bytes
  binary_header_t h = { 0x80, CMD_SET, htons(keylen),
                        0x08, 0x00, {htons(0)},
                        htonl(keylen + 8 + len) };
  h.opaque = htonl(opaque);
  bufferevent_write(bev, &h, 32); // With extras
  bufferevent_write(bev, key, keylen);
  bufferevent_write(bev, value, len);
  return 24 + ntohl(h.body_len);
}

/** WARNING UNIMPLEMENTED **/
int ProtocolBinary::delete90_request() {
  int l;
  l = evbuffer_add_printf(bufferevent_get_output(bev),
                            "*1\r\n$8\r\nFLUSHALL\r\n");

  return l;
}

/**
 * Tries to consume a binary response (in its entirety) from an evbuffer.
 *
 * @param input evBuffer to read response from
 * @return  true if consumed, false if not enough data in buffer.
 */
bool ProtocolBinary::handle_response(evbuffer *input, bool &done, bool &found, int &obj_size, uint32_t &opaque) {
  // Read the first 24 bytes as a header
  int length = evbuffer_get_length(input);
  if (length < 24) return false;
  binary_header_t* h =
          reinterpret_cast<binary_header_t*>(evbuffer_pullup(input, 24));
  assert(h);

  // Not whole response
  int targetLen = 24 + ntohl(h->body_len);
  if (length < targetLen) return false;

  obj_size = ntohl(h->body_len)-4;
  opaque = ntohl(h->opaque);
  // If something other than success, count it as a miss
  if (h->opcode == CMD_GET && h->status) {
      conn->stats.get_misses++;
      conn->stats.window_get_misses++;
      found = false;
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

