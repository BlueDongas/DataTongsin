import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

# 가상의 파일 목록 (1~10000번 파일 생성)
virtual_files = {i: i for i in range(1, 10001)}

# 요청 큐와 락
client_queue = {}  # 클라이언트별 요청 큐
cache_queue = {}  # 캐시별 요청 큐
client_queue_lock = threading.Lock()  # 클라이언트 큐를 위한 락
cache_queue_lock = threading.Lock()  # 캐시 큐를 위한 락

# 클라이언트에게 데이터를 전송하는 스레드 풀에서 처리하는 함수
def send_data(client_socket, file_number):
    try:
        if file_number in virtual_files:
            data = virtual_files[file_number]
            serialized_data = pickle.dumps(data)
            data_size = len(serialized_data)
            client_socket.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
            client_socket.sendall(serialized_data)  # 실제 데이터 전송
            print(f"Sent file {file_number} to client")
        else:
            print(f"File {file_number} not found in virtual files.")
    except Exception as e:
        print(f"Error while sending data: {e}")

# 클라이언트의 요청을 처리하는 스레드 풀에서 처리하는 함수
def handle_client(client_socket, client_id):
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

            # 요청 큐에 클라이언트가 요청한 파일 번호를 추가 (여기서는 단일 파일)
            with client_queue_lock:
                if client_id not in client_queue:
                    client_queue[client_id] = queue.Queue()
                client_queue[client_id].put((client_socket, file_number))  # 요청을 큐에 넣음

    except Exception as e:
        print(f"Error handling client {client_id}: {e}")
    finally:
        client_socket.close()

# 데이터를 전송하는 스레드가 큐에서 요청을 처리
def send_data_worker():
    while True:
        with client_queue_lock:
            # 모든 클라이언트의 큐를 순차적으로 처리
            for client_id, q in list(client_queue.items()):
                if not q.empty():
                    client_socket, file_number = q.get()
                    send_data(client_socket, file_number)

# 메인 서버 함수
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 10000))
    server_socket.listen(5)  # 최대 5개의 클라이언트 연결 허용

    print("Data Server started, waiting for connections...")

    # 두 개의 스레드 풀 생성: 하나는 클라이언트 요청 처리, 다른 하나는 데이터 전송
    with ThreadPoolExecutor(max_workers=5) as client_executor, ThreadPoolExecutor(max_workers=2) as send_executor:
        # 데이터 전송을 처리하는 스레드 풀에서 지속적으로 데이터를 전송
        send_executor.submit(send_data_worker)  # 전송 작업 처리 스레드

        client_id = 0
        while True:
            client_socket, addr = server_socket.accept()  # 클라이언트 연결 대기
            client_id += 1
            print(f"Connected to Client {addr}")
            # 클라이언트 요청을 처리하는 스레드 풀에서 요청 처리
            client_executor.submit(handle_client, client_socket, client_id)

    server_socket.close()

if __name__ == "__main__":
    main()
