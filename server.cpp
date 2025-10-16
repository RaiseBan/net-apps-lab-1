#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <vector>

#define MAX_CLIENTS 100

struct Client {
  int socket;
  pthread_t thread;
  bool active;
};

struct ServerData {
  std::vector<Client*> clients;
  pthread_mutex_t clients_mutex;
};

ServerData server_data;

void* handle_client(void* arg);
void broadcast_message(const char* nickname, uint32_t nickname_size,
                       const char* body, uint32_t body_size, int);
void remove_client(int socket);
ssize_t send_all(int socket, const void* buffer, size_t length);
ssize_t recv_all(int socket, void* buffer, size_t length);

int main(int argc, char* argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <port>\n", argv[0]);
    exit(1);
  }

  uint16_t port = (uint16_t)atoi(argv[1]);
  printf("Starting server on port %d...\n", port);
  fflush(stdout);

  // Initialize mutex
  pthread_mutex_init(&server_data.clients_mutex, NULL);
  printf("Mutex initialized\n");
  fflush(stdout);

  // Create socket with IPv6 (supports both IPv4 and IPv6)
  int server_socket = socket(AF_INET6, SOCK_STREAM, 0);
  if (server_socket < 0) {
    perror("ERROR opening socket");
    exit(1);
  }
  printf("Socket created\n");
  fflush(stdout);

  // Set socket options
  int opt = 1;
  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) <
      0) {
    perror("ERROR setsockopt SO_REUSEADDR");
    close(server_socket);
    exit(1);
  }

  // Enable dual-stack (IPv4 and IPv6)
  int ipv6only = 0;
  if (setsockopt(server_socket, IPPROTO_IPV6, IPV6_V6ONLY, &ipv6only,
                 sizeof(ipv6only)) < 0) {
    perror("ERROR setsockopt IPV6_V6ONLY");
    close(server_socket);
    exit(1);
  }

  // Bind socket
  struct sockaddr_in6 server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin6_family = AF_INET6;
  server_addr.sin6_addr = in6addr_any;
  server_addr.sin6_port = htons(port);

  if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) <
      0) {
    perror("ERROR on binding");
    close(server_socket);
    exit(1);
  }
  printf("Socket bound to port %d\n", port);
  fflush(stdout);

  // Listen for connections
  if (listen(server_socket, MAX_CLIENTS) < 0) {
    perror("ERROR on listen");
    close(server_socket);
    exit(1);
  }
  printf("Listening for connections...\n");
  fflush(stdout);

  printf("Server listening on port %d (IPv4 and IPv6)\n", port);
  fflush(stdout);

  // Accept loop
  while (true) {
    struct sockaddr_in6 client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_socket =
        accept(server_socket, (struct sockaddr*)&client_addr, &client_len);
    if (client_socket < 0) {
      perror("ERROR on accept");
      continue;
    }

    printf("New client connected (socket %d)\n", client_socket);
    fflush(stdout);

    // Set TCP_NODELAY to disable Nagle's algorithm for low latency
    int flag = 1;
    setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // Create client structure
    Client* client = new Client();
    client->socket = client_socket;
    client->active = true;

    // Add to clients list
    pthread_mutex_lock(&server_data.clients_mutex);
    server_data.clients.push_back(client);
    pthread_mutex_unlock(&server_data.clients_mutex);

    // Create thread for client
    if (pthread_create(&client->thread, NULL, handle_client, (void*)client) !=
        0) {
      perror("ERROR creating thread");
      pthread_mutex_lock(&server_data.clients_mutex);
      for (auto it = server_data.clients.begin();
           it != server_data.clients.end(); ++it) {
        if (*it == client) {
          server_data.clients.erase(it);
          break;
        }
      }
      pthread_mutex_unlock(&server_data.clients_mutex);
      close(client_socket);
      delete client;
    }
  }

  close(server_socket);
  pthread_mutex_destroy(&server_data.clients_mutex);
  return 0;
}

void* handle_client(void* arg) {
  Client* client = (Client*)arg;
  int socket = client->socket;

  while (client->active) {
    // Read nickname size
    uint32_t nickname_size_net;
    ssize_t n = recv_all(socket, &nickname_size_net, 4);
    if (n != 4) {
      break;
    }
    uint32_t nickname_size = ntohl(nickname_size_net);

    if (nickname_size == 0 || nickname_size > 1024) {
      fprintf(stderr, "Invalid nickname size: %u\n", nickname_size);
      break;
    }

    // Read nickname
    char* nickname = new char[nickname_size + 1];
    n = recv_all(socket, nickname, nickname_size);
    if (n != (ssize_t)nickname_size) {
      delete[] nickname;
      break;
    }
    nickname[nickname_size] = '\0';

    // Read body size
    uint32_t body_size_net;
    n = recv_all(socket, &body_size_net, 4);
    if (n != 4) {
      delete[] nickname;
      break;
    }
    uint32_t body_size = ntohl(body_size_net);

    if (body_size > 65536) {
      fprintf(stderr, "Invalid body size: %u\n", body_size);
      delete[] nickname;
      break;
    }

    // Read body
    char* body = new char[body_size + 1];
    n = recv_all(socket, body, body_size);
    if (n != (ssize_t)body_size) {
      delete[] nickname;
      delete[] body;
      break;
    }
    body[body_size] = '\0';

    printf("Message from %s: %s\n", nickname, body);
    fflush(stdout);

    // Broadcast message to all clients
    broadcast_message(nickname, nickname_size, body, body_size, socket);

    delete[] nickname;
    delete[] body;
  }

  printf("Client disconnected (socket %d)\n", socket);
  fflush(stdout);
  remove_client(socket);
  close(socket);
  return NULL;
}

void broadcast_message(const char* nickname, uint32_t nickname_size,
                       const char* body, uint32_t body_size, int) {
  // Get current time
  time_t now = time(NULL);
  struct tm* tm_info = localtime(&now);
  char time_str[32];
  strftime(time_str, sizeof(time_str), "%H:%M", tm_info);
  uint32_t date_size = strlen(time_str);

  // Prepare message to send
  // [nickname_size][nickname][body_size][body][date_size][date]
  uint32_t nickname_size_net = htonl(nickname_size);
  uint32_t body_size_net = htonl(body_size);
  uint32_t date_size_net = htonl(date_size);

  pthread_mutex_lock(&server_data.clients_mutex);

  for (auto it = server_data.clients.begin(); it != server_data.clients.end();
       ++it) {
    Client* client = *it;
    if (!client->active) continue;

    // Send to all clients including sender
    // Send all fields, ensuring complete transmission
    if (send(client->socket, &nickname_size_net, 4, 0) != 4) {
      client->active = false;
      continue;
    }

    if (send(client->socket, nickname, nickname_size, 0) !=
        (ssize_t)nickname_size) {
      client->active = false;
      continue;
    }

    if (send(client->socket, &body_size_net, 4, 0) != 4) {
      client->active = false;
      continue;
    }

    if (send(client->socket, body, body_size, 0) != (ssize_t)body_size) {
      client->active = false;
      continue;
    }

    if (send(client->socket, &date_size_net, 4, 0) != 4) {
      client->active = false;
      continue;
    }

    if (send(client->socket, time_str, date_size, 0) != (ssize_t)date_size) {
      client->active = false;
      continue;
    }
  }

  pthread_mutex_unlock(&server_data.clients_mutex);
}

ssize_t send_all(int socket, const void* buffer, size_t length) {
  const char* ptr = (const char*)buffer;
  size_t remaining = length;

  while (remaining > 0) {
    ssize_t sent = send(socket, ptr, remaining, MSG_NOSIGNAL);
    if (sent <= 0) {
      return sent;
    }
    ptr += sent;
    remaining -= sent;
  }

  return length;
}

ssize_t recv_all(int socket, void* buffer, size_t length) {
  char* ptr = (char*)buffer;
  size_t remaining = length;

  while (remaining > 0) {
    ssize_t received = recv(socket, ptr, remaining, 0);
    if (received <= 0) {
      return received;
    }
    ptr += received;
    remaining -= received;
  }

  return length;
}

void remove_client(int socket) {
  pthread_mutex_lock(&server_data.clients_mutex);

  for (auto it = server_data.clients.begin(); it != server_data.clients.end();
       ++it) {
    if ((*it)->socket == socket) {
      (*it)->active = false;
      // Don't delete or join here - let the thread finish naturally
      server_data.clients.erase(it);
      break;
    }
  }

  pthread_mutex_unlock(&server_data.clients_mutex);
}