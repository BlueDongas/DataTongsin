import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

# 캐시 메모리와 데이터 서버 연결 정보
cache_memory = {}  # 캐시 메모리에 저장된 파일
cache_size = 200 * 1024  # 200MB 캐시 크기
client_queue = {}  # 클라이언트별 요청 큐
cache_queue = {}  # 캐시별 요청 큐
client_queue_lock = threading.Lock()  # 클라이언트 요청 큐를 위한 락
cache_queue_lock = threading.Lock()  # 캐시 요청 큐를 위한 락

# 데이터를 전송하는 스레드 풀에서 처리하는 함수 (캐시에서 또는 데이터 서버에서 파일을 가져와 전송)
def send_data(client_socket, file_number, data_socket):
    try:
        if file_number in cache_memory:
            # 캐시에 파일이 있으면 캐시에서 데이터를 가져와 전송
            data = cache_memory[file_number]
            serialized_data = pickle.dumps(data)
            data_size = len(serialized_data)
            client_socket.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
            client_socket.sendall(serialized_data)  # 실제 데이터 전송
            print(f"Sent cached file {file_number} to client")
        else:
            # 캐시에 파일이 없으면 데이터 서버에서 요청
            send_data_to_data_server(data_socket, file_number, client_socket)
            print(f"Requested file {file_number} from Data Server")
    except Exception as e:
        print(f"Error while sending data: {e}")

# 데이터 서버에 파일을 요청하고 클라이언트에 전송하는 함수
def send_data_to_data_server(data_socket, file_number, client_socket):
    try:
        # 데이터 서버에 파일 요청
        send_request(data_socket, file_number)
        
        # 데이터 서버로부터 파일 수신
        packed_size = data_socket.recv(8)
        data_size = struct.unpack('Q', packed_size)[0]
        file_data = b""
        while len(file_data) < data_size:
            packet = data_socket.recv(4096)
            file_data += packet
        file_number = pickle.loads(file_data)
        
        # 클라이언트에 파일 전송
        client_socket.sendall(struct.pack('Q', data_size))
        client_socket.sendall(file_data)

        # 캐시에 파일 저장
        cache_memory[file_number] = pickle.loads(file_data)
        print(f"File {file_number} received from Data Server and sent to client")
    except Exception as e:
        print(f"Error requesting data from Data Server: {e}")

# 클라이언트 요청을 처리하는 스레드 풀에서 처리하는 함수
def handle_client(client_socket, client_id, data_socket):
    try:
        while True:
            # 클라이언트로부터 요청을 받음
            packed_size = client_socket.recv(8)
            if not packed_size:
                break  # 연결이 종료되면 루프를 빠져나감
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = b""
            while len(received_data) < data_size:
                packet = client_socket.recv(4096)
                received_data += packet

            # 받은 데이터를 역직렬화하여 파일 번호 얻음
            file_number = pickle.loads(received_data)
            print(f"Client {client_id} requested file {file_number}")

            # 요청 큐에 클라이언트가 요청한 파일 번호를 추가
            with client_queue_lock:
                if client_id not in client_queue:
                    client_queue[client_id] = queue.Queue()
                client_queue[client_id].put((client_socket, file_number))  # 요청을 큐에 넣음

    except Exception as e:
        print(f"Error handling client {client_id}: {e}")
    finally:
        client_socket.close()

# 데이터를 전송하는 스레드가 큐에서 요청을 처리
def send_data_worker(data_socket):
    while True:
        with client_queue_lock:
            # 모든 클라이언트의 큐를 순차적으로 처리
            for client_id, q in list(client_queue.items()):
                if not q.empty():
                    client_socket, file_number = q.get()
                    send_data(client_socket, file_number, data_socket)  # 파일을 전송

# 데이터 서버에 파일 요청을 보내는 함수
def send_request(data_socket, file_number):
    try:
        serialized_data = pickle.dumps(file_number)
        data_size = len(serialized_data)
        data_socket.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
        data_socket.sendall(serialized_data)  # 파일 번호 전송
    except Exception as e:
        print(f"Error while sending request to Data Server: {e}")

# 데이터 서버에 연결하는 함수
def connect_to_data_server():
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect(('localhost', 10000))  # 데이터 서버에 연결
    print("Connected to Data Server")
    return data_socket

# 캐시 서버를 실행하는 함수
def cache_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', port))  # 캐시 서버 포트 설정
    server_socket.listen(4)  # 최대 5개의 클라이언트 연결 대기

    print(f"Cache Server {port} started, waiting for connections...")

    # 데이터 서버와 연결 설정
    data_socket = connect_to_data_server()

    # 스레드 풀 생성: 하나는 클라이언트 요청 처리, 다른 하나는 데이터 전송
    with ThreadPoolExecutor(max_workers=5) as client_executor, ThreadPoolExecutor(max_workers=2) as send_executor:
        # 데이터를 전송하는 스레드 풀에서 작업 처리
        send_executor.submit(send_data_worker, data_socket)  # 전송 작업 처리 스레드

        client_id = 0
        while True:
            client_socket, addr = server_socket.accept()  # 클라이언트 연결 대기
            client_id += 1
            print(f"Connected to Client {addr}")
            # 클라이언트 요청을 처리하는 스레드 풀에서 요청 처리
            client_executor.submit(handle_client, client_socket, client_id, data_socket)

if __name__ == "__main__":
    port = int(input("Enter cache server port (20000 or 30000): "))  # 포트 입력 받기
    cache_server(port)
