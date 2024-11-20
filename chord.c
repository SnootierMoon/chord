#define _DEFAULT_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <ifaddrs.h>
#include <inttypes.h>
#include <net/if.h>
#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>

#define STRINGIFY_WEAK(x) #x
#define STRINGIFY(x) STRINGIFY_WEAK(x)

int main(int argc, char **argv);

#define E_CHORD_OUTOFMEM (-1)
#define E_CHORD_PLATFORM (-2)
#define E_CHORD_PROTOCOL (-3)
#define E_CHORD_UNEXPECTED (-4)

#define E_CHORD_RESPONSE_DUPE_NODE_KEY 1

#define CHORD_RECV_BUF_SIZE 2000
#define CHORD_SEND_BUF_SIZE 2000

#define CHORD_MAX_SUCCESSOR_LIST_LEN 64
#define CHORD_MAX_STABILIZE_PERIOD_MS 60000
#define CHORD_MAX_FIXFINGER_PERIOD_MS 60000
#define CHORD_MAX_PREDECESSOR_TIMEOUT_MS 60000
#define CHORD_DEFAULT_SUCCESSOR_LIST_LEN 10
#define CHORD_DEFAULT_STABILIZE_PERIOD_MS 5000
#define CHORD_DEFAULT_FIXFINGER_PERIOD_MS 1000
#define CHORD_DEFAULT_PREDECESSOR_TIMEOUT_MS 10000

const char *chord_request_stabilize_notify = "STABILIZE_NOTIFY";
const char *chord_request_successor_of_key = "SUCCESSOR_OF_KEY";
const char *chord_request_get_value_of_key = "GET_VALUE_OF_KEY";
const char *chord_request_send_keys_values = "SEND_KEYS_VALUES";
const char *chord_request_fetch_statistics = "FETCH_STATISTICS";

struct chord_node {
    uint8_t addr[4];
    uint8_t port[2];
    uint8_t key[8];
};

struct chord_request {
    enum chord_request_tag {
        CHORD_REQUEST_STABILIZE_NOTIFY,
        CHORD_REQUEST_SUCCESSOR_OF_KEY,
        CHORD_REQUEST_GET_VALUE_OF_KEY,
        CHORD_REQUEST_SEND_KEYS_VALUES,
        CHORD_REQUEST_FETCH_STATISTICS
    } tag;
    union chord_request_data {
        struct chord_request_data_stabilize_notify { struct chord_node self; } stabilize_notify;
        struct chord_request_data_successor_of_key { uint64_t key; } successor_of_key;
        struct chord_request_data_get_value_of_key { uint64_t key; } get_value_of_key;
    } data;
};

struct chord_response_stabilize_notify {
    struct chord_node self, predecessor;
    struct chord_node successor_list[CHORD_MAX_SUCCESSOR_LIST_LEN];
    uint8_t successor_list_len;
};

struct chord_response_successor_of_key {
    struct chord_node self, result;
};

struct chord_response_get_value_of_key {
};

struct chord_response_send_keys_values {
};

struct chord_response_fetch_statistics {
};

struct chord_peer {
    struct chord_peer *free_next;
    struct chord_peer_state {
        enum chord_peer_state_tag {
            CHORD_PEER_STATE_RECV_REQUEST,
            CHORD_PEER_STATE_RECV_RESPONSE,
            CHORD_PEER_STATE_SEND_REQUEST,
            CHORD_PEER_STATE_SEND_RESPONSE
        } tag;
        union chord_peer_state_data {
            struct chord_peer_state_data_recv_request {
                size_t n_recv; 
            } recv_request;
            struct chord_peer_state_data_send_response {
                size_t n_recv, n_sent, n; 
            } send_response;
            struct chord_peer_state_data_send_request {
                size_t n_sent, n;
                enum chord_request_tag request_tag; 
            } send_request;
            struct chord_peer_state_data_recv_response {
                size_t n_recv; 
                enum chord_request_tag request_tag; 
            } recv_response;
        } data;
    } state;
    int socket;
    struct sockaddr_in addr;

    uint8_t recv_buf[CHORD_RECV_BUF_SIZE];
    uint8_t send_buf[CHORD_SEND_BUF_SIZE];
};

#define CHORD_PEER_POOL_NUM_SEG 20
#define CHORD_PEER_POOL_FIRST_SEG_SIZE 32

struct chord_peer_pool {
    struct chord_peer *seg_list[CHORD_PEER_POOL_NUM_SEG];
    struct chord_peer *free_head;
    int next_seg, next_elt;
};
struct chord_peer_pool_it {
    int next_seg, next_elt;
};

struct chord {
    int epoll_fd;

    struct chord_peer_pool peer_pool;

    int server_socket;
    struct sockaddr_in server_addr;

    int stabilize_timer_fd, fixfinger_timer_fd;

    struct chord_node self, successor, predecessor;
    struct timespec predecessor_expire_time;
    uint32_t predecessor_timeout_ms;

    struct chord_peer *stabilize_peer;

    struct chord_node successor_list[CHORD_MAX_SUCCESSOR_LIST_LEN];
    uint8_t max_successor_list_len, successor_list_len;
};

uint8_t chord_epoll_mem_[3];
#define CHORD_EPOLL_EVENT_ACCEPT (&chord_epoll_mem_[0])
#define CHORD_EPOLL_EVENT_STABILIZE_TIMER (&chord_epoll_mem_[1])
#define CHORD_EPOLL_EVENT_FIXFINGER_TIMER (&chord_epoll_mem_[2])
#define CHORD_EPOLL_EVENTS_PER_CALL 32

int chord_init(
    struct chord *chord,
    const struct sockaddr_in *bind_addr,
    const struct sockaddr_in *join_addr,
    const uint64_t *key,
    uint8_t max_successor_list_len,
    uint32_t stabilize_period_ms,
    uint32_t fixfinger_period_ms,
    uint32_t predecessor_timeout_ms
);

void chord_deinit(struct chord *chord);

int chord_blocking_successor_of_key(
    uint64_t key,
    const struct sockaddr_in *start_addr,
    struct chord_node *successor
);

int chord_process_events(struct chord *chord);

int chord_process_accept_event(struct chord *chord);

int chord_process_stabilize_timer_event(struct chord *chord);

int chord_process_fixfinger_timer_event(struct chord *chord);

int chord_process_peer_event(struct chord *chord, struct chord_peer *peer);

int chord_process_peer_request(
    struct chord *chord, 
    struct chord_peer *peer,
    struct chord_peer_state_data_recv_request *state
);

int chord_process_peer_response(
    struct chord *chord, 
    struct chord_peer *peer,
    struct chord_peer_state_data_recv_response *state
);

#define E_CHORD_MESSAGE_INCOMPLETE (-1)
#define E_CHORD_MESSAGE_INVALID (-2)

int chord_read_request(
    uint8_t *buf, 
    size_t buf_len,
    struct chord_request *request,
    size_t *n_read
);

size_t chord_write_request(uint8_t *buf, const struct chord_request *request);

int chord_read_response_stabilize_notify(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_stabilize_notify *response,
    size_t *n_read
);

size_t chord_write_response_stabilize_notify(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_stabilize_notify *response
);

int chord_read_response_successor_of_key(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_successor_of_key *response,
    size_t *n_read
);

size_t chord_write_response_successor_of_key(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_successor_of_key *response
);

int chord_read_response_get_value_of_key(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_get_value_of_key *response,
    size_t *n_read
);

size_t chord_write_response_get_value_of_key(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_get_value_of_key *response
);

int chord_read_response_send_keys_values(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_send_keys_values *response,
    size_t *n_read
);

size_t chord_write_response_send_keys_values(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_send_keys_values *response
);

int chord_read_response_fetch_statistics(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_fetch_statistics *response,
    size_t *n_read
);

size_t chord_write_response_fetch_statistics(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_fetch_statistics *response
);

void chord_peer_pool_init(struct chord_peer_pool *pool);

void chord_peer_pool_deinit(struct chord_peer_pool *pool);

struct chord_peer *chord_peer_pool_it_next(
    struct chord_peer_pool *pool,
    struct chord_peer_pool_it *it
);

struct chord_peer *chord_peer_create(struct chord_peer_pool *pool);

void chord_peer_destroy(struct chord_peer_pool *pool, struct chord_peer *peer);

struct chord_node chord_node_from_addr_key(struct sockaddr_in addr, uint64_t key);

struct chord_node chord_node_from_addr(struct sockaddr_in addr);

struct sockaddr_in chord_node_addr(struct chord_node node);

uint64_t chord_node_key(struct chord_node node);

#define E_PICK_IN4_IF_ADDR_NO_SUITABLE (-1)
#define E_PICK_IN4_IF_ADDR_PLATFORM (-2)

int pick_in4_if_addr(
    struct sockaddr_in *chosen_addr,
    const char *hint_ifname,
    const struct in_addr *hint_addr,
    const uint16_t *hint_port
);

struct args {
    struct args_subcmd {
        enum args_subcmd_tag {
            ARGS_COMMAND_START,
            ARGS_COMMAND_JOIN,
            ARGS_COMMAND_LOOKUP,
            ARGS_COMMAND_STAT,
            ARGS_COMMAND_HELP
        } tag;
        union args_subcmd_data {
            /* struct { } start; */
            struct args_subcmd_data_join { struct sockaddr_in addr; } join;
            struct args_subcmd_data_lookup { struct sockaddr_in addr; uint64_t key; } lookup;
            struct args_subcmd_data_stat { struct sockaddr_in addr; } stat;
        } data;
    } subcmd;

    struct args_flags {
        enum args_flags_mask {
            ARGS_FLAG_ADDR = (1 << 0),
            ARGS_FLAG_PORT = (1 << 1),
            ARGS_FLAG_IFACE = (1 << 2),
            ARGS_FLAG_KEY = (1 << 3)
        } mask;
        struct sockaddr_in addr;
        const char *iface;
        uint64_t key;
        uint8_t max_successor_list_len;
        uint32_t stabilize_period_ms, fixfinger_period_ms, predecessor_timeout_ms;

    } flags;
};

#define E_PARSE_ARGS_MISSING (-1)
#define E_PARSE_ARGS_INVALID_SUBCOMMAND (-2)
#define E_PARSE_ARGS_INVALID_IP (-3)
#define E_PARSE_ARGS_INVALID_PORT (-4)
#define E_PARSE_ARGS_INVALID_KEY (-5)
#define E_PARSE_ARGS_INVALID_FLAG (-6)

int parse_args(struct args *args, int argc, char *const *argv);

int parse_uint64(uint64_t *val, const char *buf);

char *fmt_addr(struct sockaddr_in addr);

struct timespec timespec_from_duration_ms32(uint32_t duration_ms);

struct timespec timespec_add(struct timespec lhs, struct timespec rhs);

int timespec_cmp(struct timespec lhs, struct timespec rhs);

int key_cmp(uint64_t ref, uint64_t lhs, uint64_t rhs);

int starts_with(const char *key, const char *str);

extern const char *help_msg;

int main(int argc, char **argv) {
    int rc;
    struct args args;
    struct sockaddr_in chosen_addr;
    struct chord chord;

    if ((rc = parse_args(&args, argc, argv)) < 0) {
        if (rc == E_PARSE_ARGS_MISSING) {
            fprintf(stderr, "E: missing argument\n");
        } else if (rc == E_PARSE_ARGS_INVALID_SUBCOMMAND) {
            fprintf(stderr, "E: invalid subcommand\n");
        } else if (rc == E_PARSE_ARGS_INVALID_IP) {
            fprintf(stderr, "E: invalid IP\n");
        } else if (rc == E_PARSE_ARGS_INVALID_PORT) {
            fprintf(stderr, "E: invalid port\n");
        } else if (rc == E_PARSE_ARGS_INVALID_KEY) {
            fprintf(stderr, "E: invalid key\n");
        } else if (rc == E_PARSE_ARGS_INVALID_FLAG) {
            fprintf(stderr, "E: invalid flag\n");
        }
        fprintf(stderr, "I: run `chord help` for usage instructions\n");
        return 1;
    } else if (args.subcmd.tag == ARGS_COMMAND_HELP) {
        puts(help_msg);
        return 0;
    }

    if (
        (rc = pick_in4_if_addr(
            &chosen_addr,
            args.flags.mask & ARGS_FLAG_IFACE ? args.flags.iface : NULL,
            args.flags.mask & ARGS_FLAG_ADDR ? &args.flags.addr.sin_addr : NULL,
            args.flags.mask & ARGS_FLAG_PORT ? &args.flags.addr.sin_port : NULL
        )) < 0
    ) {
        if (rc == E_PICK_IN4_IF_ADDR_PLATFORM) {
            fprintf(stderr, "E: platform error %s\n", strerror(errno));
        } else if (rc == E_PICK_IN4_IF_ADDR_NO_SUITABLE) {
            fprintf(stderr, "E: no suitable interface/address found\n");
        }
        return 1;
    }

    if (args.subcmd.tag == ARGS_COMMAND_START || args.subcmd.tag == ARGS_COMMAND_JOIN) {
        if (
            (rc = chord_init(
                &chord,
                &chosen_addr,
                args.subcmd.tag == ARGS_COMMAND_JOIN ? &args.subcmd.data.join.addr : NULL,
                args.flags.mask & ARGS_FLAG_KEY ? &args.flags.key : NULL,
                args.flags.max_successor_list_len,
                args.flags.stabilize_period_ms,
                args.flags.fixfinger_period_ms,
                args.flags.predecessor_timeout_ms
            )) < 0
        ) {
            if (rc == E_CHORD_OUTOFMEM) {
                fprintf(stderr, "E: out of memory\n");
            } else if (rc == E_CHORD_PLATFORM) {
                fprintf(stderr, "E: %s\n", strerror(errno));
            } else if (rc == E_CHORD_PROTOCOL) {
                fprintf(stderr, "E: chord protocol error\n");
            } else if (rc == E_CHORD_UNEXPECTED) {
                fprintf(stderr, "E: inconsistent state\n");
            }
            fprintf(stderr, "E: init failed\n");
            return 1;
        }

        while (1) {
            if ((rc = chord_process_events(&chord)) < 0) {
                if (rc == E_CHORD_OUTOFMEM) {
                    fprintf(stderr, "E: out of memory\n");
                } else if (rc == E_CHORD_PLATFORM) {
                    fprintf(stderr, "E: %s\n", strerror(errno));
                } else if (rc == E_CHORD_PROTOCOL) {
                    fprintf(stderr, "E: chord protocol error\n");
                } else if (rc == E_CHORD_UNEXPECTED) {
                    fprintf(stderr, "E: inconsistent state\n");
                }
                fprintf(stderr, "E: fatal failure\n");
                chord_deinit(&chord);
                return 1;
            }
        }

        chord_deinit(&chord);
    }

    return 0;
}

int chord_init(
    struct chord *chord,
    const struct sockaddr_in *bind_addr,
    const struct sockaddr_in *join_addr,
    const uint64_t *key,
    uint8_t max_successor_list_len,
    uint32_t stabilize_period_ms,
    uint32_t fixfinger_period_ms,
    uint32_t predecessor_timeout_ms
) {
    int rc;
    socklen_t addrlen = sizeof(struct sockaddr_in);
    struct itimerspec set_time;
    struct epoll_event add_ev;

    /* initialize server socket */
    if ((chord->server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        return E_CHORD_PLATFORM;
    }
    if (
        bind(
            chord->server_socket, 
            (const struct sockaddr *)bind_addr,
            sizeof(struct sockaddr_in)
        ) < 0
    ) {
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    if (getsockname(chord->server_socket, (struct sockaddr *)&chord->server_addr, &addrlen) < 0) {
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    if (listen(chord->server_socket, 64) < 0) {
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }

    /* initialize chord protocol structures */
    if (key) {
        chord->self = chord_node_from_addr_key(chord->server_addr, *key);
    } else {
        chord->self = chord_node_from_addr(chord->server_addr);
    }
    if (join_addr) {
        if (
            (rc = chord_blocking_successor_of_key(
                chord_node_key(chord->self),
                join_addr,
                &chord->successor
            )) < 0
        ) {
            close(chord->server_socket);
            return rc;
        }
    } else {
        chord->successor = chord->self;
    }
    chord->predecessor = chord->self;
    chord->predecessor_expire_time.tv_sec = 0;
    chord->predecessor_expire_time.tv_nsec = 0;
    chord->predecessor_timeout_ms = predecessor_timeout_ms;
    chord->max_successor_list_len = max_successor_list_len;
    chord->successor_list_len = 0;

    /* initialize stabilize_timer_fd */
    if ((chord->stabilize_timer_fd = timerfd_create(CLOCK_MONOTONIC, 0)) < 0) {
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    set_time.it_value.tv_sec = 0;
    set_time.it_value.tv_nsec = 1;
    set_time.it_interval = timespec_from_duration_ms32(stabilize_period_ms);
    if (timerfd_settime(chord->stabilize_timer_fd, 0, &set_time, NULL) < 0) {
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }

    /* initialize fixfinger_timer_fd */
    if ((chord->fixfinger_timer_fd = timerfd_create(CLOCK_MONOTONIC, 0)) < 0) {
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    set_time.it_value = timespec_from_duration_ms32(fixfinger_period_ms);
    set_time.it_interval = timespec_from_duration_ms32(fixfinger_period_ms);
    if (timerfd_settime(chord->fixfinger_timer_fd, 0, &set_time, NULL) < 0) {
        close(chord->fixfinger_timer_fd);
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    /* initialize epoll_fd */
    if ((chord->epoll_fd = epoll_create1(0)) < 0) {
        close(chord->fixfinger_timer_fd);
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }

    printf("%p\n", chord_epoll_mem_);
    add_ev.events = EPOLLIN;
    add_ev.data.ptr = CHORD_EPOLL_EVENT_ACCEPT;
    if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_ADD, chord->server_socket, &add_ev) < 0) {
        close(chord->epoll_fd);
        close(chord->fixfinger_timer_fd);
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    add_ev.events = EPOLLIN;
    add_ev.data.ptr = CHORD_EPOLL_EVENT_STABILIZE_TIMER;
    if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_ADD, chord->stabilize_timer_fd, &add_ev) < 0) {
        close(chord->epoll_fd);
        close(chord->fixfinger_timer_fd);
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }
    add_ev.events = EPOLLIN;
    add_ev.data.ptr = CHORD_EPOLL_EVENT_FIXFINGER_TIMER;
    if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_ADD, chord->fixfinger_timer_fd, &add_ev) < 0) {
        close(chord->epoll_fd);
        close(chord->fixfinger_timer_fd);
        close(chord->stabilize_timer_fd);
        close(chord->server_socket);
        return E_CHORD_PLATFORM;
    }

    fprintf(stderr, "I: init success\n");
    fprintf(stderr, "I: listening on %s\n", fmt_addr(chord->server_addr));

    chord_peer_pool_init(&chord->peer_pool);

    return 0;
}

void chord_deinit(
    struct chord *chord
) {
    struct chord_peer_pool_it it = { 0 };
    struct chord_peer *peer;

    while ((peer = chord_peer_pool_it_next(&chord->peer_pool, &it))) {
        close(peer->socket);
    }
    chord_peer_pool_deinit(&chord->peer_pool);

    close(chord->fixfinger_timer_fd);
    close(chord->stabilize_timer_fd);
    close(chord->server_socket);
    close(chord->epoll_fd);
}

int chord_blocking_successor_of_key(
    uint64_t key,
    const struct sockaddr_in *start_addr,
    struct chord_node *successor
) {
    (void)key;
    (void)start_addr;
    (void)successor;
    return -50;
}

int chord_process_events(struct chord *chord) {
    int rc, i;
    struct epoll_event events[CHORD_EPOLL_EVENTS_PER_CALL];

    if ((rc = epoll_wait(chord->epoll_fd, events, CHORD_EPOLL_EVENTS_PER_CALL, -1)) < 0) {
        return E_CHORD_PLATFORM;
    }
    for (i = 0; i < rc; i++) {
        if (events[i].data.ptr == CHORD_EPOLL_EVENT_ACCEPT) {
            if ((rc = chord_process_accept_event(chord)) < 0) {
                return rc;
            }
        } else if (events[i].data.ptr == CHORD_EPOLL_EVENT_STABILIZE_TIMER) {
            if ((rc = chord_process_stabilize_timer_event(chord)) < 0) {
                return rc;
            }
        } else if (events[i].data.ptr == CHORD_EPOLL_EVENT_FIXFINGER_TIMER) {
            if ((rc = chord_process_fixfinger_timer_event(chord)) < 0) {
                return rc;
            }
        } else {
            if ((rc = chord_process_peer_event(chord, events[i].data.ptr)) < 0) {
                return rc;
            }
        }
    }

    return 0;
}

int chord_process_accept_event(struct chord *chord) {
    int socket;
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(struct sockaddr_in);
    struct chord_peer *peer;
    struct epoll_event add_ev;

    if ((socket = accept(chord->server_socket, (struct sockaddr *)&addr, &addrlen)) < 0) {
        return E_CHORD_PLATFORM;
    }
    if (!(peer = chord_peer_create(&chord->peer_pool))) {
        close(socket);
        return E_CHORD_OUTOFMEM;
    }

    add_ev.events = EPOLLIN;
    add_ev.data.ptr = peer;
    if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_ADD, socket, &add_ev) < 0) {
        close(socket);
        chord_peer_destroy(&chord->peer_pool, peer);
        return E_CHORD_PLATFORM;
    }

    peer->socket = socket;
    peer->addr = addr;
    peer->state.tag = CHORD_PEER_STATE_RECV_REQUEST;
    peer->state.data.recv_request.n_recv = 0;

    return 0;
}

int chord_process_stabilize_timer_event(struct chord *chord) {
    uint8_t timerfd_buf[8];

    if (read(chord->stabilize_timer_fd, timerfd_buf, 8) < 0) {
        return E_CHORD_PLATFORM;
    }

    return 0;
}

int chord_process_fixfinger_timer_event(struct chord *chord) {
    uint8_t timerfd_buf[8];

    if (read(chord->fixfinger_timer_fd, timerfd_buf, 8) < 0) {
        return E_CHORD_PLATFORM;
    }

    return 0;
}

int chord_process_peer_event(struct chord *chord, struct chord_peer *peer) {
    int rc;
    struct epoll_event mod_ev;

    if (peer->state.tag == CHORD_PEER_STATE_RECV_REQUEST) {
        if (
            (rc = recv(
                peer->socket,
                peer->recv_buf + peer->state.data.recv_request.n_recv,
                CHORD_RECV_BUF_SIZE - peer->state.data.recv_request.n_recv,
                0
            )) < 0
        ) {
            return E_CHORD_PLATFORM;
        }
        if (rc == 0) {
            close(peer->socket);
            chord_peer_destroy(&chord->peer_pool, peer);
            return 0;
        }
        peer->state.data.recv_request.n_recv += rc;

        if ((rc = chord_process_peer_request(chord, peer, &peer->state.data.recv_request)) < 0) {
            if (rc == E_CHORD_MESSAGE_INVALID) {
                fprintf(stderr, "W: received an invalid request from %s\n",
                    fmt_addr(peer->addr));
            }
        }
        if (rc == 1) {
            mod_ev.events = EPOLLOUT;
            mod_ev.data.ptr = peer;
            if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_MOD, peer->socket, &mod_ev) < 0) {
                return E_CHORD_PLATFORM;
            }
        }
    }
    else if (peer->state.tag == CHORD_PEER_STATE_SEND_RESPONSE) {
        struct chord_peer_state_data_recv_request new_state;

        if (
            (rc = send(
                peer->socket,
                peer->send_buf + peer->state.data.send_response.n_sent,
                peer->state.data.send_response.n - peer->state.data.send_response.n_sent,
                0
            )) < 0
        ) {
            return E_CHORD_PLATFORM;
        }
        if (rc == 0) {
            close(peer->socket);
            chord_peer_destroy(&chord->peer_pool, peer);
            return 0;
        }
        peer->state.data.send_response.n_sent += rc;

        if (peer->state.data.send_response.n_sent == peer->state.data.send_response.n) {
            new_state.n_recv = peer->state.data.send_response.n_recv;
            peer->state.tag = CHORD_PEER_STATE_RECV_REQUEST;
            peer->state.data.recv_request = new_state;

            if ((rc = chord_process_peer_request(chord, peer, &peer->state.data.recv_request)) < 0) {
                return rc;
            }
            if (rc == 0) {
                mod_ev.events = EPOLLIN;
                mod_ev.data.ptr = peer;
                if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_MOD, peer->socket, &mod_ev) < 0) {
                    return E_CHORD_PLATFORM;
                }
            }
        }
    }
    else if (peer->state.tag == CHORD_PEER_STATE_SEND_REQUEST) {
        struct chord_peer_state_data_recv_response new_state;

        if (
            (rc = send(
                peer->socket,
                peer->send_buf + peer->state.data.send_request.n_sent,
                peer->state.data.send_request.n - peer->state.data.send_request.n_sent,
                0
            )) < 0
        ) {
            return E_CHORD_PLATFORM;
        }
        if (rc == 0) {
            // TODO request failure
            return E_CHORD_PROTOCOL;
        }
        peer->state.data.send_request.n_sent += rc;

        if (peer->state.data.send_request.n_sent == peer->state.data.send_request.n) {
            new_state.n_recv = 0;
            new_state.request_tag = peer->state.data.send_request.request_tag;
            peer->state.tag = CHORD_PEER_STATE_RECV_RESPONSE;
            peer->state.data.recv_response = new_state;

            mod_ev.events = EPOLLIN;
            mod_ev.data.ptr = peer;
            if (epoll_ctl(chord->epoll_fd, EPOLL_CTL_MOD, peer->socket, &mod_ev) < 0) {
                return E_CHORD_PLATFORM;
            }
        }
    }
    else if (peer->state.tag == CHORD_PEER_STATE_RECV_RESPONSE) {
        if (
            (rc = recv(
                peer->socket,
                peer->recv_buf + peer->state.data.recv_response.n_recv,
                CHORD_RECV_BUF_SIZE - peer->state.data.recv_response.n_recv,
                0
            )) < 0
        ) {
            return E_CHORD_PLATFORM;
        }
        if (rc == 0) {
            // TODO request failure
            return E_CHORD_PROTOCOL;
        }
        peer->state.data.recv_response.n_recv += rc;

        if ((rc = chord_process_peer_response(chord, peer, &peer->state.data.recv_response)) < 0) {
            return rc;
        }
        if (rc == 1) {
            close(peer->socket);
            chord_peer_pool_destroy(peer);
        }
    }

    return 0;
}

int chord_process_peer_request(
    struct chord *chord, 
    struct chord_peer *peer,
    struct chord_peer_state_data_recv_request *state
) {
    int rc;
    struct chord_request request;
    struct timespec now;
    size_t n_read;
    struct chord_peer_state_data_send_response new_state;

    if 
        ((rc = chord_read_request(
            peer->recv_buf, 
            state->n_recv,
            &request,
            &n_read
        )) < 0
    ) {
        if (rc == E_CHORD_MESSAGE_INCOMPLETE) {
            return 0;
        } else if (rc == E_CHORD_MESSAGE_INVALID) {
            return E_CHORD_PROTOCOL;
        }
        return E_CHORD_UNEXPECTED;
    }
    state->n_recv -= n_read;
    memmove(peer->recv_buf, peer->recv_buf + n_read, state->n_recv);
    if (request.tag == CHORD_REQUEST_STABILIZE_NOTIFY) {
        struct chord_response_stabilize_notify response;

        if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) {
            return E_CHORD_PLATFORM;
        }
        if (timespec_cmp(chord->predecessor_expire_time, now) < 0 || 
            key_cmp(
                chord_node_key(chord->predecessor),
                chord_node_key(request.data.stabilize_notify.self), 
                chord_node_key(chord->self)
            ) < 0
        ) {
            chord->predecessor_expire_time = timespec_add(
                now, timespec_from_duration_ms32(chord->predecessor_timeout_ms)
            );
            chord->predecessor = request.data.stabilize_notify.self;
        } else if (
            (memcmp(&request.data.stabilize_notify.self, &chord->predecessor,
                sizeof(struct chord_node)) && 
            request.data.stabilize_notify.self.key == chord->predecessor.key) ||
            request.data.stabilize_notify.self.key == chord->self.key
        ) {

            return 1;
        }
        chord->predecessor_expire_time = timespec_add(
            now, timespec_from_duration_ms32(chord->predecessor_timeout_ms)
        );

        new_state.n_recv = state->n_recv;
        new_state.n_sent = 0;
        new_state.n = chord_write_response_stabilize_notify(peer->send_buf, 0, &response);
        peer->state.tag = CHORD_PEER_STATE_SEND_RESPONSE;
        peer->state.data.send_response = new_state;
        return 1;
    } else if (request.tag == CHORD_REQUEST_SUCCESSOR_OF_KEY) {
        return 1; // TODO
    } else if (request.tag == CHORD_REQUEST_GET_VALUE_OF_KEY) {
        return 1; // TODO
    } else if (request.tag == CHORD_REQUEST_SEND_KEYS_VALUES) {
        return 1; // TODO
    } else if (request.tag == CHORD_REQUEST_FETCH_STATISTICS) {
        return 1; // TODO
    } else {
        return E_CHORD_UNEXPECTED;
    }
}

int chord_process_peer_response(
    struct chord *chord, 
    struct chord_peer *peer,
    struct chord_peer_state_data_recv_response *state
) {
    int rc;
    uint8_t erc;
    size_t n_read;

    if (state->request_tag == CHORD_REQUEST_STABILIZE_NOTIFY) {
        struct chord_response_stabilize_notify response;

        if (
            (rc = chord_read_response_stabilize_notify(
                peer->recv_buf,
                peer->n_recv,
                &erc,
                &response,
                &n_read
            ) < 0)
        ) {
            if (rc == E_CHORD_MESSAGE_INCOMPLETE) {
                return 0;
            } else if (rc == E_CHORD_MESSAGE_INVALID) {
                return E_CHORD_PROTOCOL;
            }
            return E_CHORD_UNEXPECTED;
        }

        // update successor list



        return 1;
    } else if (state->request_tag == CHORD_REQUEST_SUCCESSOR_OF_KEY) {
        return 1;
    } else if (state->request_tag == CHORD_REQUEST_GET_VALUE_OF_KEY) {
        return 1;
    } else if (state->request_tag == CHORD_REQUEST_SEND_KEYS_VALUES) {
        return 1;
    } else if (state->request_tag == CHORD_REQUEST_FETCH_STATISTICS) {
        return 1;
    } else {
        return E_CHORD_UNEXPECTED;
    }
}

int chord_read_request(
    uint8_t *buf,
    size_t buf_len,
    struct chord_request *request,
    size_t *n_read
) {
    if (buf_len < 16) {
        return E_CHORD_MESSAGE_INCOMPLETE;
    }

    if (!memcmp(buf, chord_request_stabilize_notify, 16)) {
        if (buf_len < 16 + sizeof(struct chord_node)) {
            return E_CHORD_MESSAGE_INCOMPLETE;
        }
        request->tag = CHORD_REQUEST_STABILIZE_NOTIFY;
        memcpy(&request->data.stabilize_notify.self, buf + 16, sizeof(struct chord_node));
        *n_read = 16 + sizeof(struct chord_node);
    }
    else if (!memcmp(buf, chord_request_successor_of_key, 16)) {
        if (buf_len < 24) {
            return E_CHORD_MESSAGE_INCOMPLETE;
        }
        request->tag = CHORD_REQUEST_SUCCESSOR_OF_KEY;
        memcpy(&request->data.successor_of_key.key, buf + 16, 8);
        *n_read = 24;
    }
    else if (!memcmp(buf, chord_request_get_value_of_key, 16)) {
        if (buf_len < 24) {
            return E_CHORD_MESSAGE_INCOMPLETE;
        }
        request->tag = CHORD_REQUEST_SUCCESSOR_OF_KEY;
        memcpy(&request->data.get_value_of_key.key, buf + 16, 8);
        *n_read = 24;
    }
    else if (!memcmp(buf, chord_request_send_keys_values, 16)) {
        exit(1); // TODO
    }
    else if (!memcmp(buf, chord_request_fetch_statistics, 16)) {
        exit(1); // TODO
    }
    else {
        return E_CHORD_MESSAGE_INVALID;
    }

    return 0;
}

size_t chord_write_request(uint8_t *buf, const struct chord_request* request) {
    if (request->tag == CHORD_REQUEST_STABILIZE_NOTIFY) {
        memcpy(buf, chord_request_stabilize_notify, 16);
        memcpy(buf + 16, &request->data.stabilize_notify.self, sizeof(struct chord_node));
        return 16 + sizeof(struct chord_node);
    }
    else if (request->tag == CHORD_REQUEST_SUCCESSOR_OF_KEY) {
        memcpy(buf, chord_request_successor_of_key, 16);
        memcpy(buf + 16, &request->data.successor_of_key.key, 8);
        return 24;
    }
    else if (request->tag == CHORD_REQUEST_GET_VALUE_OF_KEY) {
        memcpy(buf, chord_request_get_value_of_key, 16);
        memcpy(buf + 16, &request->data.get_value_of_key.key, 8);
        return 24;
    }
    else if (request->tag == CHORD_REQUEST_SEND_KEYS_VALUES) {
        exit(1); // TODO
    }
    else if (request->tag == CHORD_REQUEST_FETCH_STATISTICS) {
        exit(1); // TODO
    }

    return 0;
}

int chord_read_response_stabilize_notify(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_stabilize_notify *response,
    size_t *n_read
) {
    uint8_t i;

    if (buf_len < 1) {
        return E_CHORD_MESSAGE_INCOMPLETE;
    }
    if ((*erc = buf[0])) {
        *n_read = 1;
        return 0;
    }

    if (buf_len < 2) {
        return E_CHORD_MESSAGE_INCOMPLETE;
    }
    if (buf[1] >= CHORD_MAX_SUCCESSOR_LIST_LEN) {
        return E_CHORD_MESSAGE_INVALID;
    }
    response->successor_list_len = buf[1];

    if (buf_len < 2 + (response->successor_list_len + 2) * sizeof(struct chord_node)) {
        return E_CHORD_MESSAGE_INCOMPLETE;
    }
    memcpy(&response->self, buf + 2, sizeof(struct chord_node));
    memcpy(&response->predecessor, buf + 2 + sizeof(struct chord_node), sizeof(struct chord_node));
    for (i = 0; i < response->successor_list_len; i++) {
        memcpy(
            &response->successor_list[i],
            buf + 2 + (i + 2) * sizeof(struct chord_node),
            sizeof(struct chord_node)
        );
    }

    *n_read = 2 + (response->successor_list_len + 2) * sizeof(struct chord_node);
    return 0;
}

size_t chord_write_response_stabilize_notify(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_stabilize_notify *response
) {
    uint8_t i;

    if ((buf[0] = erc)) {
        return 1;
    }

    buf[1] = response->successor_list_len;

    memcpy(buf + 2, &response->self, sizeof(struct chord_node));
    memcpy(buf + 2 + sizeof(struct chord_node), &response->predecessor, sizeof(struct chord_node));

    for (i = 0; i < response->successor_list_len; i++) {
        memcpy(
            buf + 2 + (i + 2) * sizeof(struct chord_node),
            &response->successor_list[i],
            sizeof(struct chord_node)
        );
    }

    return 2 + (response->successor_list_len + 2) * sizeof(struct chord_node);
}

int chord_read_response_successor_of_key(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_successor_of_key *response,
    size_t *n_read
) {
    if (buf_len < 1) {
        return E_CHORD_MESSAGE_INCOMPLETE;
    }
    if ((*erc = buf[0])) {
        *n_read = 1;
        return 0;
    }

    if (buf_len < 1 + 2 * sizeof(struct chord_node)) {
        return E_CHORD_MESSAGE_INCOMPLETE;
    }
    memcpy(&response->self, buf + 1, sizeof(struct chord_node));
    memcpy(&response->self, buf + 1 + sizeof(struct chord_node), sizeof(struct chord_node));

    *n_read = 1 + 2 * sizeof(struct chord_node);
    return 0;
}

size_t chord_write_response_successor_of_key(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_successor_of_key *response
) {
    buf[0] = erc;
    if (erc) {
        return 1;
    }

    memcpy(buf + 1, &response->self, sizeof(struct chord_node));
    memcpy(buf + 1 + sizeof(struct chord_node), &response->result, sizeof(struct chord_node));

    return 1 + 2 * sizeof(struct chord_node);
}

int chord_read_response_get_value_of_key(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_get_value_of_key *response,
    size_t *n_read
);

size_t chord_write_response_get_value_of_key(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_get_value_of_key *response
);

int chord_read_response_send_keys_values(
    uint8_t *buf,
    size_t buf_len,
    uint8_t *erc,
    struct chord_response_send_keys_values *response,
    size_t *n_read
);

size_t chord_write_response_send_keys_values(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_send_keys_values *response
);

size_t chord_write_response_fetch_statistics(
    uint8_t *buf,
    uint8_t erc,
    const struct chord_response_fetch_statistics *response
);

void chord_peer_pool_init(struct chord_peer_pool *pool) {
    pool->next_seg = 0;
    pool->next_elt = 0;
    pool->free_head = 0;
}

void chord_peer_pool_deinit(struct chord_peer_pool *pool) {
    int i;

    for (i = 0; i < pool->next_seg; i++) {
        if (pool->seg_list[i]) {
            free(pool->seg_list[i]);
        }
    }
}

struct chord_peer *chord_peer_pool_it_next(
    struct chord_peer_pool *pool,
    struct chord_peer_pool_it *it
) {
    struct chord_peer *result;

    while (1) {
        if (it->next_elt >= (CHORD_PEER_POOL_FIRST_SEG_SIZE << it->next_seg)) {
            it->next_seg += 1;
            it->next_elt = 0;
        }
        if (
            it->next_seg > pool->next_seg ||
            (it->next_seg == pool->next_seg &&
            it->next_elt > pool->next_elt)
        ) {
            return NULL;
        }
        result = &pool->seg_list[it->next_seg][it->next_elt++];
        if (!result->free_next) {
            return result;
        }
    }
}

struct chord_peer *chord_peer_create(struct chord_peer_pool *pool) {
    struct chord_peer *result;

    if (pool->free_head) {
        result = pool->free_head;
        pool->free_head = pool->free_head->free_next;
    } else {
        if (pool->next_seg >= CHORD_PEER_POOL_NUM_SEG) {
            return NULL;
        }
        if (pool->next_elt >= (CHORD_PEER_POOL_FIRST_SEG_SIZE << pool->next_seg)) {
            pool->next_seg += 1;
            pool->next_elt = 0;
            if (pool->next_seg >= CHORD_PEER_POOL_NUM_SEG) {
                return NULL;
            }
        }
        if (!pool->seg_list[pool->next_seg]) {
            if (
                !(pool->seg_list[pool->next_seg] = calloc(
                CHORD_PEER_POOL_FIRST_SEG_SIZE << pool->next_seg,
                sizeof(struct chord_peer)))
            ) {
                return NULL;
            }
        }
        result = &pool->seg_list[pool->next_seg][pool->next_elt++];
    }

    result->free_next = NULL;
    return result;
}

void chord_peer_destroy(struct chord_peer_pool *pool, struct chord_peer *peer) {
    peer->free_next = pool->free_head;
    pool->free_head = peer;
}

struct chord_node chord_node_from_addr_key(struct sockaddr_in addr, uint64_t key) {
    struct chord_node node;
    uint64_t key_be = htobe64(key);
    memcpy(&node.port, &addr.sin_port, 2);
    memcpy(&node.addr, &addr.sin_addr.s_addr, 4);
    memcpy(&node.key, &key_be, 8);
    return node;
}

struct chord_node chord_node_from_addr(struct sockaddr_in addr) {
    uint64_t port_h = be16toh(addr.sin_port);
    uint64_t addr_h = be32toh(addr.sin_addr.s_addr);
    uint64_t x = addr_h ^ (addr_h << 32) ^ (port_h << 48) ^ port_h;
    /* https://xoshiro.di.unimi.it/splitmix64.c */
	uint64_t z = (x += 0x9e3779b97f4a7c15);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
	return chord_node_from_addr_key(addr, z ^ (z >> 31));
}

struct sockaddr_in chord_node_addr(struct chord_node node) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    memcpy(&node.port, &addr.sin_port, 2);
    memcpy(&node.addr, &addr.sin_addr.s_addr, 4);
    return addr;
}

uint64_t chord_node_key(struct chord_node node) {
    uint64_t key_be;
    memcpy(&key_be, &node.key, 8);
    return be64toh(key_be);
}

int pick_in4_if_addr(
    struct sockaddr_in *chosen_addr,
    const char *hint_ifname,
    const struct in_addr *hint_addr,
    const uint16_t *hint_port
) {
    struct ifaddrs *ifaddrs, *curr, *chosen = NULL;

    if (getifaddrs(&ifaddrs) < 0) {
        fprintf(stderr, "E: getifaddrs\n");
        return E_PICK_IN4_IF_ADDR_PLATFORM;
    }

    for (curr = ifaddrs; curr; curr = curr->ifa_next) {
        /* interface selection rule 
           - filter only interfaces with addresses
           - filter only AF_INET addresses
           - filter only IFF_RUNNING addresses
           - filter only addresses that match address hint
           - filter only interfaces that match interface hint
           - if no interface chosen, then select current interface
           - choose non-IFF_LOOPBACK over IFF_LOOPBACK
           - choose "eth*" / "en*" over "wl*" and choose "wl*" over anything else
        */
        if (
            curr->ifa_addr &&
            curr->ifa_addr->sa_family == AF_INET &&
            (curr->ifa_flags & IFF_RUNNING) &&
            (!hint_addr || hint_addr->s_addr == ((const struct sockaddr_in *)curr->ifa_addr)->
                sin_addr.s_addr) &&
            (!hint_ifname || (curr->ifa_name && !strcmp(hint_ifname, curr->ifa_name))) &&
            (!chosen ||
            (!(curr->ifa_flags & IFF_LOOPBACK) &&
            ((chosen->ifa_flags & IFF_LOOPBACK) ||
            (curr->ifa_name && (!chosen->ifa_name ||
            (!starts_with(chosen->ifa_name, "eth") && !starts_with(chosen->ifa_name, "en") &&
            (starts_with(curr->ifa_name, "eth") || starts_with(curr->ifa_name, "en") ||
            (!starts_with(chosen->ifa_name, "wl") && starts_with(curr->ifa_name, "wl")))))))))
        ) {
            chosen = curr;
        }
    }

    if (!chosen) {
        freeifaddrs(ifaddrs);
        return E_PICK_IN4_IF_ADDR_NO_SUITABLE;
    }

    *chosen_addr = *(const struct sockaddr_in *)chosen->ifa_addr;

    if (hint_port) {
        chosen_addr->sin_port = *hint_port;
    }

    freeifaddrs(ifaddrs);
    return 0;
}

int parse_args(struct args *args, int argc, char *const *argv) {
    int curr_arg;
    const char *subcommand_name, *flag_switch;
    uint64_t val;

    memset(args, 0, sizeof(struct args));
    args->flags.max_successor_list_len = CHORD_DEFAULT_SUCCESSOR_LIST_LEN;
    args->flags.stabilize_period_ms = CHORD_DEFAULT_STABILIZE_PERIOD_MS;
    args->flags.fixfinger_period_ms = CHORD_DEFAULT_FIXFINGER_PERIOD_MS;
    args->flags.predecessor_timeout_ms = CHORD_DEFAULT_PREDECESSOR_TIMEOUT_MS;

    if (argc < 2) {
        return -1;
    }

    curr_arg = 1;
    subcommand_name = argv[curr_arg++];

    /* command: start */
    if (!strcmp(subcommand_name, "start")) {
        args->subcmd.tag = ARGS_COMMAND_START;
    }
    /* command: join */
    else if (!strcmp(subcommand_name, "join")) {
        args->subcmd.tag = ARGS_COMMAND_JOIN;
        if (curr_arg + 2 > argc) 
            return E_PARSE_ARGS_MISSING;
        args->subcmd.data.join.addr.sin_family = AF_INET;
        if (inet_pton(AF_INET, argv[curr_arg++], &args->subcmd.data.join.addr.sin_addr) != 1) {
            return E_PARSE_ARGS_INVALID_IP;
        }
        if (parse_uint64(&val, argv[curr_arg++]) < 0 || val >= 65536) {
            return E_PARSE_ARGS_INVALID_PORT;
        }
        args->subcmd.data.join.addr.sin_port = htons((uint16_t)val);
    } 
    /* command: lookup */
    else if (!strcmp(subcommand_name, "lookup")) {
        args->subcmd.tag = ARGS_COMMAND_LOOKUP;
        if (curr_arg + 3 > argc) 
            return E_PARSE_ARGS_MISSING;
        args->subcmd.data.lookup.addr.sin_family = AF_INET;
        if (inet_pton(AF_INET, argv[curr_arg++], &args->subcmd.data.lookup.addr.sin_addr) != 1) {
            return E_PARSE_ARGS_INVALID_IP;
        }
        if (parse_uint64(&val, argv[curr_arg++]) < 0 || val >= 65536) {
            return E_PARSE_ARGS_INVALID_PORT;
        }
        args->subcmd.data.lookup.addr.sin_port = htons((uint16_t)val);
        if (parse_uint64(&args->subcmd.data.lookup.key, argv[curr_arg++]) < 0) {
            return E_PARSE_ARGS_INVALID_KEY;
        }
    } 
    /* command: stat */
    else if (!strcmp(subcommand_name, "stat")) {
        args->subcmd.tag = ARGS_COMMAND_STAT;
        if (curr_arg + 2 > argc) 
            return E_PARSE_ARGS_MISSING;
        args->subcmd.data.stat.addr.sin_family = AF_INET;
        if (inet_pton(AF_INET, argv[curr_arg++], &args->subcmd.data.stat.addr.sin_addr) != 1) {
            return E_PARSE_ARGS_INVALID_IP;
        }
        if (parse_uint64(&val,  argv[curr_arg++]) < 0 || val >= 65536) {
            return E_PARSE_ARGS_INVALID_PORT;
        }
        args->subcmd.data.stat.addr.sin_port = htons((uint16_t)val);
        curr_arg += 2;
    } 
    /* command: help */
    else if (!strcmp(subcommand_name, "help")) {
        args->subcmd.tag = ARGS_COMMAND_HELP;
        return 0;
    } else {
        return E_PARSE_ARGS_INVALID_SUBCOMMAND;
    }
    
    while (curr_arg < argc) {
        flag_switch = argv[curr_arg++];
        /* flag: address */
        if (!strcmp(flag_switch, "-a") || !strcmp(flag_switch, "--address")) {
            args->flags.mask |= ARGS_FLAG_ADDR;
            if (curr_arg + 1 > argc) {
                return E_PARSE_ARGS_MISSING;
            }
            if (inet_pton(AF_INET, argv[curr_arg++], &args->flags.addr.sin_addr) != 1) {
                return E_PARSE_ARGS_INVALID_IP;
            }
        } else if (starts_with(flag_switch, "-a")) {
            args->flags.mask |= ARGS_FLAG_ADDR;
            if (inet_pton(AF_INET, flag_switch+2, &args->flags.addr.sin_addr) != 1) {
                return E_PARSE_ARGS_INVALID_IP;
            }
        }
        /* flag: port */
        else if (!strcmp(flag_switch, "-p") || !strcmp(flag_switch, "--port")) {
            args->flags.mask |= ARGS_FLAG_PORT;
            if (curr_arg + 1 > argc) {
                return E_PARSE_ARGS_MISSING;
            }
            if (parse_uint64(&val,  argv[curr_arg++]) < 0 || val >= 65536) {
                return E_PARSE_ARGS_INVALID_PORT;
            }
            args->flags.addr.sin_port = htons((uint16_t)val);
        } else if (starts_with(flag_switch, "-p")) {
            args->flags.mask |= ARGS_FLAG_PORT;
            if (parse_uint64(&val,  flag_switch+2) < 0 || val >= 65536) {
                return E_PARSE_ARGS_INVALID_PORT;
            }
            args->flags.addr.sin_port = htons((uint16_t)val);
        }
        /* flag: interface */
        else if (!strcmp(flag_switch, "-i") || !strcmp(flag_switch, "--interface")) {
            args->flags.mask |= ARGS_FLAG_IFACE;
            if (curr_arg + 1 > argc) {
                return E_PARSE_ARGS_MISSING;
            }
            args->flags.iface = argv[curr_arg++];
        } else if (starts_with(flag_switch, "-i")) {
            args->flags.mask |= ARGS_FLAG_IFACE;
            args->flags.iface = flag_switch+2;
        }
        else {
            return E_PARSE_ARGS_INVALID_FLAG;
        }
    }

    return 0;
}

int parse_uint64(uint64_t *val, const char *buf) {
    int i;

    *val = 0;

    if (buf[0] == '0') {
        if (buf[1]) {
            return -1;
        } else {
            return 0;
        }
    }

    for (i = 0; buf[i]; i += 1) {
        if (!isdigit(buf[i])) {
            return -1;
        }
        if (*val > 1844674407370955161 || (*val == 1844674407370955161 && buf[i] >= '6')) {
            return -1;
        }
        *val = (uint64_t)(10) * (*val) + (uint64_t)(buf[i] - '0');
    }

    if (!i) {
        return -1;
    }

    return 0;
}

char *fmt_addr(struct sockaddr_in addr) {
    static char ip_buf[16], fmt_addr_buf[22];
    inet_ntop(AF_INET, &addr.sin_addr, ip_buf, sizeof(ip_buf));
    sprintf(fmt_addr_buf, "%s:%" PRIu16, ip_buf, ntohs(addr.sin_port));
    return fmt_addr_buf;
}

struct timespec timespec_from_duration_ms32(uint32_t duration_ms) {
    struct timespec t;
    t.tv_sec = duration_ms / 1000;
    t.tv_nsec = (duration_ms % 1000) * 1000000;
    return t;
}

struct timespec timespec_add(struct timespec lhs, struct timespec rhs) {
    struct timespec result;

    result.tv_sec = lhs.tv_sec + rhs.tv_sec + (lhs.tv_nsec + rhs.tv_nsec) / 1000000000;
    result.tv_nsec = (lhs.tv_nsec + rhs.tv_nsec) % 1000000000;

    return result;
}

int timespec_cmp(struct timespec lhs, struct timespec rhs) {
    if (lhs.tv_sec < rhs.tv_sec) {
        return -1;
    } 
    if (lhs.tv_sec > rhs.tv_sec) {
        return 1;
    }
    if (lhs.tv_nsec < rhs.tv_nsec) {
        return -1;
    }
    if (lhs.tv_nsec > rhs.tv_nsec) {
        return 1;
    }
    return 0;
}

int key_cmp(uint64_t ref, uint64_t lhs, uint64_t rhs) {
    lhs -= (ref + 1);
    rhs -= (ref + 1);
    if (lhs < rhs) {
        return -1;
    }
    if (lhs > rhs) {
        return 1;
    }
    return 0;
}

int starts_with(const char *str, const char *key) {
    return !strncmp(str, key, strlen(key));
}

const char *help_msg = 
    "Usage: chord <subcommand> [flags...]\n"
    "\n"
    "Description:\n"
    "  Chord is a distributed hash table (DHT) algorithm that provides an efficient,\n"
    "  scalable, and decentralized method to maintain a consistent key-value mapping\n"
    "  across multiple nodes in a network. This program is based of the paper found\n"
    "  at https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf.\n"
    "  This program contains a naive implementation of the Chord algorithm. It is\n"
    "  not designed with security in mind. There are countless easy exploits that a\n"
    "  malicious 3rd-party could take advantage of in order to take down a Chord\n"
    "  ring of nodes running this program.\n"
    "\n"
    "Subcommands:\n"
    "  start                 Start a new one-node ring.\n"
    "  join <ADDR> <PORT>    Join the ring which the node at ADDR:PORT is a part of.\n"
    "                         * ADDR is a IPv4 address.\n"
    "                         * PORT is a 16-bit unsigned integer.\n"
    "  lookup <ADDR> <PORT> <KEY>\n"
    "                        Lookup KEY in the ring which the node at ADDR:PORT is a\n"
    "                        part of.\n"
    "                         * ADDR is a IPv4 address.\n"
    "                         * PORT is a 16-bit unsigned integer.\n"
    "  stat <ADDR> <PORT>    Print some statistics on a node.\n"
    "                         * ADDR is an IPv4 address.\n"
    "                         * PORT is a 16-bit unsigned integer.\n"
    "  help                  Show this message and quit.\n"
    "\n"
    "Network Flags:\n"
    "  -a, --addr <ADDR>     Set IP address used to bind to ADDR. \n"
    "                         * Only valid for `start` and `join`.\n"
    "                         * Must be an IPv4 address.\n"
    "                         * If omitted, bind to any address in the iface list.\n"
    "  -p, --port <PORT>     Set port used to bind to PORT.\n"
    "                         * Only valid for `start` and `join`.\n"
    "                         * Must be a 16-bit unsigned integer.\n"
    "                         * If omitted, bind to port 0 (let kernel decide).\n"
    "  -i, --iface <IFACE>   Set the iface (interface) used to bind to IFACE.\n"
    "                         * Only valid for `start` and `join`.\n"
    "                         * If omitted, bind to any iface in the iface list.\n"
    "\n"
    "Chord Protocol Flags:\n"
    "  -k, --key <KEY>       Set this node's key to KEY.\n"
    "                         * Only valid for `start` and `join`. \n"
    "                         * Must be a 64-bit unsigned integer.\n"
    "                         * If omitted, determine key based on bind address.\n"
    "  -r, --max-successors <VAL>\n"
    "                        Set the max length of the successor list to VAL.\n"
    "                         * Only valid for `start` and `join`.\n"
    "                         * Must satisfy 1 <= VAL <= " 
                                    STRINGIFY(CHORD_MAX_SUCCESSOR_LIST_LEN) ".\n"
    "                         * If omitted, set to "
                                    STRINGIFY(CHORD_DEFAULT_SUCCESSOR_LIST_LEN) ".\n"
    " --sp <TIME_MS>         Set the stabilize period to TIME_MS.\n"
    "                         * Only valid for `start` and `join`.\n"
    "                         * TIME_MS is a duration specified in milliseconds.\n"
    "                         * Must satisfy 0 <= TIME_MS <= "
                                    STRINGIFY(CHORD_MAX_STABILIZE_PERIOD_MS) ".\n"
    "                         * If omitted, set to "
                                    STRINGIFY(CHORD_DEFAULT_STABILIZE_PERIOD_MS) ".\n"
    " --ffp <TIME_MS>        Set the fix-finger period to TIME_MS.\n"
    "                         * Only valid for `start` and `join`.\n"
    "                         * TIME_MS is a duration specified in milliseconds.\n"
    "                         * Must satisfy 0 <= TIME_MS <= " 
                                    STRINGIFY(CHORD_MAX_FIXFINGER_PERIOD_MS) ".\n"
    "                         * If omitted, set to "
                                    STRINGIFY(CHORD_DEFAULT_FIXFINGER_PERIOD_MS) ".\n"
    " --pto <TIME_MS>        Set the predecessor timeout to TIME_MS.\n"
    "                         * Only valid for `start` and `join`.\n"
    "                         *x: TIME_MS is a duration specified in milliseconds.\n"
    "                         * Must satisfy 0 <= TIME_MS <= "
                                    STRINGIFY(CHORD_MAX_PREDECESSOR_TIMEOUT_MS) ".\n"
    "                         * If omitted, set to "
                                    STRINGIFY(CHORD_DEFAULT_PREDECESSOR_TIMEOUT_MS) ".\n";
