#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "common/ds_proto.h"

#define MAXSIZE 1024
#define IPADDRESS "127.0.0.1"
#define FDSIZE 1024
#define EPOLLEVENTS 20

static void handle_connection(int sockfd);
static void handle_events(int epollfd, struct epoll_event *events, int num, int sockfd,
                          char *buf);
static void do_read(int epollfd, int fd, int sockfd, char *buf);
static void do_read(int epollfd, int fd, int sockfd, char *buf);
static void do_write(int epollfd, int fd, int sockfd, char *buf);
static void add_event(int epollfd, int fd, int state);
static void delete_event(int epollfd, int fd, int state);
static void modify_event(int epollfd, int fd, int state);

int proto_len = sizeof(ds_proto_header_t);

int main(int argc, char *argv[]) {
    int sockfd;
    struct sockaddr_in servaddr;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(atoi(argv[1]));
    inet_pton(AF_INET, IPADDRESS, &servaddr.sin_addr);
    int ret = connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
    if (ret != 0) {
        fprintf(stderr, "errno:%d", errno);
    }
    //处理连接
    handle_connection(sockfd);
    close(sockfd);
    return 0;
}

static int get_proto_head(ds_proto_header_t *proto_header, int len) {
    ds_header_t header;

    header.magic_number = DS_PROTO_MAGIC_NUMBER;
    header.version = 100;
    header.body_len = len;
    header.time_out = 1000000;
    header.msg_id = 567;
    header.msg_type = 10;
    header.func_id = 10;
    header.proto_type = 1;
    header.stream_hash = 0;

    ds_serialize_header(&header, proto_header);
    return 0;
}

static void handle_connection(int sockfd) {
    int epollfd;
    struct epoll_event events[EPOLLEVENTS];
    char buf[MAXSIZE];
    int ret;
    epollfd = epoll_create(FDSIZE);
    add_event(epollfd, STDIN_FILENO, EPOLLIN);
    for (;;) {
        ret = epoll_wait(epollfd, events, EPOLLEVENTS, -1);
        handle_events(epollfd, events, ret, sockfd, buf);
    }
    close(epollfd);
}

static void handle_events(int epollfd, struct epoll_event *events, int num, int sockfd,
                          char *buf) {
    int fd;
    int i;
    for (i = 0; i < num; i++) {
        fd = events[i].data.fd;
        if (events[i].events & EPOLLIN) {
            do_read(epollfd, fd, sockfd, buf);
        } else if (events[i].events & EPOLLOUT) {
            do_write(epollfd, fd, sockfd, buf);
        }
    }
}

static void do_read(int epollfd, int fd, int sockfd, char *buf) {
    int nread;

    if (fd == STDIN_FILENO) {
        nread = read(fd, buf + sizeof(ds_proto_header_t), MAXSIZE);
    } else {
        nread = read(fd, buf, MAXSIZE);
    }
    if (nread == -1) {
        perror("read error:");
        close(fd);
    } else if (nread == 0) {
        fprintf(stderr, "server close.\n");
        close(fd);
    } else {
        if (fd == STDIN_FILENO) {
            if (nread > 1) {  // nread == 1 \n
                ds_proto_header_t proto_header;
                get_proto_head(&proto_header, nread - 1);
                buf[proto_len + nread - 1] = 0;
                memcpy(buf, (void *)&proto_header, proto_len);
                add_event(epollfd, sockfd, EPOLLOUT);
            }
        } else {
            delete_event(epollfd, sockfd, EPOLLIN);
            add_event(epollfd, STDOUT_FILENO, EPOLLOUT);
        }
    }
}

static void do_write(int epollfd, int fd, int sockfd, char *buf) {
    int nwrite;
    if (fd == STDOUT_FILENO) {
        nwrite = write(fd, buf + proto_len, strlen(buf + proto_len));
    } else {
        nwrite = write(fd, buf, proto_len + strlen(buf + proto_len));
    }
    if (nwrite == -1) {
        perror("write error:");
        close(fd);
    } else {
        if (fd == STDOUT_FILENO) {
            delete_event(epollfd, fd, EPOLLOUT);
        } else {
            modify_event(epollfd, fd, EPOLLIN);
        }
    }
    memset(buf, 0, MAXSIZE);
}

static void add_event(int epollfd, int fd, int state) {
    struct epoll_event ev;
    ev.events = state;
    ev.data.fd = fd;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev);
}

static void delete_event(int epollfd, int fd, int state) {
    struct epoll_event ev;
    ev.events = state;
    ev.data.fd = fd;
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, &ev);
}

static void modify_event(int epollfd, int fd, int state) {
    struct epoll_event ev;
    ev.events = state;
    ev.data.fd = fd;
    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev);
}
