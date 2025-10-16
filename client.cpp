#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>

#include <string>

struct ClientData {
  int socket;
  std::string nickname;
  bool running;
  bool input_mode;
  pthread_mutex_t print_mutex;
};

ClientData client_data;

void* receive_messages(void* arg);
void send_message(int socket, const char* nickname, const char* message);
void set_terminal_mode(bool raw);
void clear_line();

int main(int argc, char* argv[]) {
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <server_address> <port> <nickname>\n", argv[0]);
    exit(1);
  }

  const char* server_address = argv[1];
  const char* port_str = argv[2];
  const char* nickname = argv[3];

  client_data.nickname = nickname;
  client_data.running = true;
  client_data.input_mode = false;
  pthread_mutex_init(&client_data.print_mutex, NULL);

  // Resolve address (supports both IPv4 and IPv6)
  struct addrinfo hints, *result, *rp;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;      // Allow IPv4 or IPv6
  hints.ai_socktype = SOCK_STREAM;  // TCP socket

  int status = getaddrinfo(server_address, port_str, &hints, &result);
  if (status != 0) {
    fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(status));
    exit(1);
  }

  // Try each address until we successfully connect
  int sockfd = -1;
  for (rp = result; rp != NULL; rp = rp->ai_next) {
    sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sockfd == -1) continue;

    if (connect(sockfd, rp->ai_addr, rp->ai_addrlen) != -1) {
      break;  // Success
    }

    close(sockfd);
    sockfd = -1;
  }

  freeaddrinfo(result);

  if (sockfd == -1) {
    fprintf(stderr, "Could not connect to server\n");
    exit(1);
  }

  client_data.socket = sockfd;
  printf("Connected to server as '%s'\n", nickname);
  printf(
      "Press 'm' to enter message mode, then type your message and press "
      "Enter to send.\n");
  printf("Press Ctrl+C to exit.\n\n");

  // Create receive thread
  pthread_t recv_thread;
  if (pthread_create(&recv_thread, NULL, receive_messages, NULL) != 0) {
    perror("ERROR creating receive thread");
    close(sockfd);
    exit(1);
  }

  // Set terminal to raw mode for character-by-character input
  set_terminal_mode(true);

  // Main input loop
  std::string input_buffer;
  char ch;
  while (client_data.running) {
    if (read(STDIN_FILENO, &ch, 1) <= 0) {
      break;
    }

    if (!client_data.input_mode) {
      // Not in input mode - wait for 'm' key
      if (ch == 'm' || ch == 'M') {
        client_data.input_mode = true;
        pthread_mutex_lock(&client_data.print_mutex);
        printf("\r[Message mode] > ");
        fflush(stdout);
        pthread_mutex_unlock(&client_data.print_mutex);
        input_buffer.clear();
      }
    } else {
      // In input mode
      if (ch == '\n' || ch == '\r') {
        // Send message
        if (!input_buffer.empty()) {
          pthread_mutex_lock(&client_data.print_mutex);
          printf("\r");
          clear_line();
          fflush(stdout);
          pthread_mutex_unlock(&client_data.print_mutex);

          send_message(sockfd, nickname, input_buffer.c_str());
          input_buffer.clear();
        }
        client_data.input_mode = false;
      } else if (ch == 127 || ch == 8) {
        // Backspace
        if (!input_buffer.empty()) {
          input_buffer.pop_back();
          pthread_mutex_lock(&client_data.print_mutex);
          printf("\b \b");
          fflush(stdout);
          pthread_mutex_unlock(&client_data.print_mutex);
        }
      } else if (ch >= 32 && ch < 127) {
        // Printable character
        input_buffer += ch;
        pthread_mutex_lock(&client_data.print_mutex);
        printf("%c", ch);
        fflush(stdout);
        pthread_mutex_unlock(&client_data.print_mutex);
      }
    }
  }

  // Cleanup
  set_terminal_mode(false);
  client_data.running = false;
  close(sockfd);
  pthread_join(recv_thread, NULL);
  pthread_mutex_destroy(&client_data.print_mutex);

  printf("\nDisconnected from server.\n");
  return 0;
}

void* receive_messages(void*) {
  int socket = client_data.socket;

  while (client_data.running) {
    // Read nickname size
    uint32_t nickname_size_net;
    ssize_t n = recv(socket, &nickname_size_net, 4, MSG_WAITALL);
    if (n <= 0) {
      break;
    }
    uint32_t nickname_size = ntohl(nickname_size_net);

    if (nickname_size == 0 || nickname_size > 1024) {
      fprintf(stderr, "Invalid nickname size received\n");
      break;
    }

    // Read nickname
    char* nickname = new char[nickname_size + 1];
    n = recv(socket, nickname, nickname_size, MSG_WAITALL);
    if (n <= 0) {
      delete[] nickname;
      break;
    }
    nickname[nickname_size] = '\0';

    // Read body size
    uint32_t body_size_net;
    n = recv(socket, &body_size_net, 4, MSG_WAITALL);
    if (n <= 0) {
      delete[] nickname;
      break;
    }
    uint32_t body_size = ntohl(body_size_net);

    if (body_size > 65536) {
      fprintf(stderr, "Invalid body size received\n");
      delete[] nickname;
      break;
    }

    // Read body
    char* body = new char[body_size + 1];
    n = recv(socket, body, body_size, MSG_WAITALL);
    if (n <= 0) {
      delete[] nickname;
      delete[] body;
      break;
    }
    body[body_size] = '\0';

    // Read date size
    uint32_t date_size_net;
    n = recv(socket, &date_size_net, 4, MSG_WAITALL);
    if (n <= 0) {
      delete[] nickname;
      delete[] body;
      break;
    }
    uint32_t date_size = ntohl(date_size_net);

    if (date_size > 100) {
      fprintf(stderr, "Invalid date size received\n");
      delete[] nickname;
      delete[] body;
      break;
    }

    // Read date
    char* date = new char[date_size + 1];
    n = recv(socket, date, date_size, MSG_WAITALL);
    if (n <= 0) {
      delete[] nickname;
      delete[] body;
      delete[] date;
      break;
    }
    date[date_size] = '\0';

    // Display message
    pthread_mutex_lock(&client_data.print_mutex);
    if (client_data.input_mode) {
      // In input mode - print message above input line
      printf("\r");
      clear_line();
      printf("{%s} [%s] %s\n", date, nickname, body);
      printf("[Message mode] > ");
      fflush(stdout);
    } else {
      // Not in input mode - just print
      printf("{%s} [%s] %s\n", date, nickname, body);
      fflush(stdout);
    }
    pthread_mutex_unlock(&client_data.print_mutex);

    delete[] nickname;
    delete[] body;
    delete[] date;
  }

  client_data.running = false;
  return NULL;
}

void send_message(int socket, const char* nickname, const char* message) {
  // Send nickname size and nickname
  uint32_t nickname_size = strlen(nickname);
  uint32_t nickname_size_net = htonl(nickname_size);
  send(socket, &nickname_size_net, 4, MSG_NOSIGNAL);
  send(socket, nickname, nickname_size, MSG_NOSIGNAL);

  // Send message size and message
  uint32_t message_size = strlen(message);
  uint32_t message_size_net = htonl(message_size);
  send(socket, &message_size_net, 4, MSG_NOSIGNAL);
  send(socket, message, message_size, MSG_NOSIGNAL);
}

void set_terminal_mode(bool raw) {
  static struct termios old_termios;

  if (raw) {
    // Save old settings
    tcgetattr(STDIN_FILENO, &old_termios);

    // Set raw mode
    struct termios new_termios = old_termios;
    new_termios.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &new_termios);
  } else {
    // Restore old settings
    tcsetattr(STDIN_FILENO, TCSANOW, &old_termios);
  }
}

void clear_line() {
  printf("\033[2K");  // Clear entire line
}