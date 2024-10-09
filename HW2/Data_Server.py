import socket
import threading
import pickle
import queue

# 가상 파일 목록 및 크기 (1 ~ 10000번 파일, 크기는 파일 번호에 비례)
virtual_files = {i: i for i in range(1, 10001)}
client_lock = threading.Lock()
file_number_queue = queue.Queue()
file_number_queue_semaphore = threading.Semaphore(4)

# 클라이언트가 요청한 파일을 처리하는 함수
def receive_cache_file(cache_socket, cache_id):
    while True:
        try:
            # 파일 번호를 받아서 해당 파일을 전송하는 로직
            receive_file = cache_socket.recv(1024)
            file_number = pickle.loads(receive_file)
            
            file_number_queue.put(file_number)
            print(f"Cache {cache_id} requested file {file_number}.")
            # 클락 + 속도 구하는 로직 추가

        except Exception as e:
            print(f"Error sending file to client {cache_id}: {e}")
            break

def send_cache_file(cache_socket, cache_id):
    while True:
        try:
            file_number = file_number_queue.get()
            download_time = int(file_number) / 2000  # 2Mbps 다운로드 시간 계산
            send_data = pickle.dumps(download_time)
            cache_socket.sendall(send_data)
            
            print(f"Send file {file_number} to {cache_id}")
        except Exception as e:
            print(f"Error sending file to cache {cache_id}: {e}")
            break

# 캐시 서버가 요청한 파일을 처리하는 함수
def handle_cache(cache_socket, cache_id):
    try:
        cache_socket.sendall(pickle.dumps(cache_id))  # 해당 클라이언트한테 고유 ID 전달
        receive_cache_thread = threading.Thread(target=receive_cache_file, args=(cache_socket, cache_id))
        send_cache_thread = threading.Thread(target=send_cache_file, args=(cache_socket, cache_id))

        receive_cache_thread.start()
        send_cache_thread.start()

        receive_cache_thread.join()
        send_cache_thread.join()
    finally:
        cache_socket.close()

def receive_file(client_socket, client_id):
    while True:
        try:
            # 파일 번호를 받아서 해당 파일을 전송하는 로직
            receive_file = client_socket.recv(1024)
            file_number = pickle.loads(receive_file)
            
            file_number_queue.put(file_number)
            print(f"Client {client_id} requested file {file_number}.")
            # 클락 + 속도 구하는 로직 추가

        except Exception as e:
            print(f"Error receive file to client {client_id}: {e}")
            break

def send_file(client_socket, client_id):
    while True:
        try:
            file_number = file_number_queue.get()
            download_time = int(file_number) / 1000  # 1Mbps 다운로드 시간 계산
            send_data = pickle.dumps(download_time)
            client_socket.sendall(send_data)
            
            print(f"Send file {file_number} to {client_id}")
        except Exception as e:
            print(f"Error sending file to client {client_id}: {e}")
            break
        
# 클라이언트가 요청한 파일을 처리하는 함수
def handle_client(client_socket, client_id):
    try:
        client_socket.sendall(pickle.dumps(client_id))  # 해당 클라이언트한테 고유 ID 전달
        receive_thread = threading.Thread(target=receive_file, args=(client_socket, client_id))
        send_thread = threading.Thread(target=send_file, args=(client_socket, client_id))

        receive_thread.start()
        send_thread.start()

        receive_thread.join()
        send_thread.join()
    finally:
        client_socket.close()

# 캐시 서버를 처리하는 함수
def accept_cache(server_socket, num_cache):
    cache_sockets = []

    for cache_id in range(1, num_cache + 1):
        cache_socket, addr = server_socket.accept()
        cache_sockets.append((cache_socket, addr))
        cache_socket.settimeout(100)

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
