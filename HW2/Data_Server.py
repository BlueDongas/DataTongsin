import socket
import threading

def handle_cache(cache_socket, cache_id):
    print("handle_cache")
    return 0

def handle_client(client_socket, client_id):
    print("handle_client")
    return 0

def accept_cache(server_socket, num_cache):
    cache_sockets = []

    for cache_id in range(1, num_cache + 1):
        cache_socket, addr = server_socket.accept()
        cache_sockets.append((cache_socket, addr))
        #log_write(f"Client {client_id} connected from {addr}")
        cache_socket.settimeout(10)

    threads = []
    for cache_id, (cache_socket, addr) in enumerate(cache_sockets, 1):
        thread = threading.Thread(target=handle_cache, args=(cache_socket, cache_id))
        threads.append(thread)
        thread.start()

    return threads     

def accept_clients(server_socket, num_clients):
    client_sockets = []

    for client_id in range(1, num_clients + 1):
        client_socket, addr = server_socket.accept()
        client_sockets.append((client_socket, addr))
        #log_write(f"Client {client_id} connected from {addr}")
        client_socket.settimeout(10)

    threads = []
    for client_id, (client_socket, addr) in enumerate(client_sockets, 1):
        thread = threading.Thread(target=handle_client, args=(client_socket, client_id, A, B))
        threads.append(thread)
        thread.start()

    return threads

def main():
    virtual_files = [i for i in range(1, 10001)] # 1 ~ 10000개의 파일을 크기로 저장 (인덱스를 파일 번호로 사용)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 10000))  
    server_socket.listen(6)  # 캐시 서버 2개 + 클라이언트 4개 수용
    cache_threads = accept_cache(server_socket, 2)
    client_threads = accept_clients(server_socket, 4)
    
    for thread in cache_threads:
        thread.join()
        
    for thread in client_threads:
        thread.join()
    
    server_socket.close()
    
if __name__ == "__main__":
    main()
