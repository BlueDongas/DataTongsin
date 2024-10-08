import socket
import threading
import pickle

# 가상 파일 목록 및 크기 (1 ~ 10000번 파일, 크기는 파일 번호에 비례)
virtual_files = {i: i for i in range(1, 10001)}
Data_Clock = 0
connected_clients = 0  # 연결된 클라이언트 수 추적
client_lock = threading.Lock()



# 캐시 서버가 요청한 파일을 처리하는 함수
def handle_cache(cache_socket, cache_id):
    try:
        cache_socket.sendall(pickle.dumps(cache_id))
    except Exception as e:
        print(f"Error send cache_id : {e}")
    finally:
        cache_socket.close()

# 클라이언트가 요청한 파일을 처리하는 함수
def handle_client(client_socket, client_id):
    try:
        client_socket.sendall(pickle.dumps(client_id)) # 해당 클라이언트한테 고유 ID 전달
    except Exception as e:
        print(f"Error : {e}")    
    finally:
        client_socket.close()

# 캐시 서버를 처리하는 함수
def accept_cache(server_socket, num_cache):
    cache_sockets = []

    for cache_id in range(1, num_cache + 1):
        cache_socket, addr = server_socket.accept()
        cache_sockets.append((cache_socket, addr))
        cache_socket.settimeout(10)

    threads = []
    for cache_id, (cache_socket, addr) in enumerate(cache_sockets, 1):
        thread = threading.Thread(target=handle_cache, args=(cache_socket, cache_id))
        threads.append(thread)
        thread.start()

    return threads     

# 클라이언트를 처리하는 함수
def accept_clients(server_socket, num_clients):
    client_sockets = []

    for client_id in range(1, num_clients + 1):
        client_socket, addr = server_socket.accept()
        client_sockets.append((client_socket, addr))
        client_socket.settimeout(100)

    threads = []
    for client_id, (client_socket, addr) in enumerate(client_sockets, 1):
        thread = threading.Thread(target=handle_client, args=(client_socket, client_id))
        threads.append(thread)
        thread.start()

    return threads

def main():
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
