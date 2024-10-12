import struct
import socket
import threading
import pickle
import queue

# 가상 파일 목록 및 크기 (1 ~ 10000번 파일, 크기는 파일 번호에 비례)
virtual_files = {i: i for i in range(1, 10001)}
<<<<<<< HEAD

client_queue = {}  # 클라이언트별 요청 큐를 관리하기 위한 딕셔너리
cache_queue = {}  # 캐시 별 요청 큐를 관리하기 위한 딕셔너리
client_queue_lock = threading.Lock()
cache_queue_lock = threading.Lock()

=======
>>>>>>> parent of b651c7c (update 주고받기)
client_lock = threading.Lock()
file_number_queue = queue.Queue()
file_number_queue_semaphore = threading.Semaphore(4)

<<<<<<< HEAD
clock = 0

# 데이터를 주고 받을 때 데이터 크기를 먼저 보내고 해당 크기만큼 데이터를 받는 방식
def send_data(sock, data):
    try:
        serialized_data = pickle.dumps(data)
        data_size = len(serialized_data)
        sock.sendall(struct.pack('Q', data_size))
        sock.sendall(serialized_data)
    except Exception as e:
        print(f"Error while sending data: {e}")

def receive_data(sock):
    try:
        packed_size = sock.recv(8)
        if not packed_size:
            print("No size information received. Closing connection.")
            return None
        data_size = struct.unpack('Q', packed_size)[0]
        data = b""
        while len(data) < data_size:
            packet = sock.recv(4096)
            if not packet:
                print("Connection closed while receiving data.")
                return None
            data += packet
        return pickle.loads(data)
    except pickle.UnpicklingError as e:
        print(f"Error while unpickling data: {e}")
        return None
    except Exception as e:
        print(f"Error while receiving data: {e}")
        return None

=======
>>>>>>> parent of b651c7c (update 주고받기)
# 캐시한테 요청받은 파일을 처리하는 함수
def receive_cache_file(cache_socket, cache_id):
    while True:
        try:
<<<<<<< HEAD
            received_data = receive_data(cache_socket)
            if received_data is None:
                break
            if isinstance(received_data, tuple) and len(received_data) == 2:
                received_cache_id, file_number = received_data
            else:
                print(f"Unexpected data format from cache {cache_id}: {received_data}")
                continue
            with cache_queue_lock:
                if received_cache_id in cache_queue:
                    cache_queue[received_cache_id].put(file_number)
                    print(f"Cache {cache_id} requested file {file_number}.")
=======
            # 파일 번호를 받아서 해당 파일을 전송하는 로직
            receive_file = cache_socket.recv(1024)
            file_number = pickle.loads(receive_file)
            
            file_number_queue.put(file_number)
            print(f"Cache {cache_id} requested file {file_number}.")
            # 클락 + 속도 구하는 로직 추가

>>>>>>> parent of b651c7c (update 주고받기)
        except Exception as e:
            print(f"Error receiving file from cache {cache_id}: {e}")
            break

def send_cache_file(cache_socket, cache_id):
    while True:
        try:
<<<<<<< HEAD
            if cache_id not in cache_queue:
                continue
            if cache_queue[cache_id].empty():
                continue
            with cache_queue_lock:
                file_number = cache_queue[cache_id].get()
            send_data(cache_socket, (cache_id, file_number))
            print(f"Send file {file_number} to Cache {cache_id}")
=======
            file_number = file_number_queue.get()
            download_time = file_number # 2Mbps 다운로드 시간 계산
            send_data = pickle.dumps(download_time)
            cache_socket.sendall(send_data)
            
            print(f"Send file {file_number} to {cache_id}")
>>>>>>> parent of b651c7c (update 주고받기)
        except Exception as e:
            print(f"Error sending file to cache {cache_id}: {e}")
            break

# 캐시 서버가 요청한 파일을 처리하는 함수
def handle_cache(cache_socket, cache_id):
    try:
        cache_socket.sendall(pickle.dumps((cache_id)))  # 해당 클라이언트한테 고유 ID 전달 (튜플 형식 유지)
        receive_cache_thread = threading.Thread(target=receive_cache_file, args=(cache_socket, cache_id))
        send_cache_thread = threading.Thread(target=send_cache_file, args=(cache_socket, cache_id))

        receive_cache_thread.start()
        send_cache_thread.start()

        receive_cache_thread.join()
        send_cache_thread.join()
    finally:
        cache_socket.close()

# 클라이언트가 요청한 파일을 처리하는 함수
def receive_file(client_socket, client_id):
    if client_id not in client_queue:
        client_queue[client_id] = queue.Queue()
    while True:
        try:
            received_data = receive_data(client_socket)
            if received_data is None:
                break
            if isinstance(received_data, tuple) and len(received_data) == 2:
                received_client_id, file_number = received_data
            else:
                print(f"Unexpected data format from client {client_id}: {received_data}")
                continue
            with client_queue_lock:
                if received_client_id in client_queue:
                    client_queue[received_client_id].put(file_number)
                    print(f"Client {client_id} requested file {file_number}.")
        except Exception as e:
            print(f"Error receiving file from client {client_id}: {e}")
            break

def send_file(client_socket, client_id):
    while True:
        try:
            if client_id not in client_queue:
                continue
            if client_queue[client_id].empty():
                continue
            with client_queue_lock:
                file_number = client_queue[client_id].get()
            send_data(client_socket, (client_id, file_number))
            print(f"Send file {file_number} to client {client_id}")
        except Exception as e:
            print(f"Error sending file to client {client_id}: {e}")
            break

# 클라이언트가 요청한 파일을 처리하는 함수
def handle_client(client_socket, client_id):
    try:
        client_socket.sendall(pickle.dumps((client_id)))  # 해당 클라이언트한테 고유 ID 전달 (튜플 형식 유지)
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
