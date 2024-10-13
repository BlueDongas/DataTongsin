import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor
import threading
import heapq

# 가상의 파일 목록을 저장 (1~10000 파일 번호와 파일 내용)
virtual_files = {i: i for i in range(1, 10001)}  # 파일 번호와 내용이 동일한 가상 파일

#클라이언트 캐시 동시 동작을 위한 변수
connect_count = 0
total_connect = 6
connect_condition = threading.Condition()
clock = 0

log_file = ""
def log_write(event):
    log_file.write(f"{event}\n")
    print(event)
    log_file.flush()

# 클라이언트에게 파일 데이터를 전송하는 함수
def send_data(client_socket, data):
    try:
        send_to_data = pickle.dumps((clock,data))  # 데이터를 직렬화
        data_size = len(send_to_data)
        client_socket.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
        client_socket.sendall(send_to_data)  # 데이터 전송
        print(f"Sent file {data} to client")
    except Exception as e:
        print(f"Error while sending data: {e}")

# 클라이언트의 요청을 처리하는 함수
def handle_client(client_socket, client_id):
    try:
        while True:
            packed_size = client_socket.recv(8)  # 데이터 크기 수신
            if not packed_size:
                break  # 클라이언트 연결이 종료되면 루프 탈출
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = b""
            while len(received_data) < data_size:
                packet = client_socket.recv(4096)  # 데이터를 수신
                received_data += packet

            recieved_clock,file_number = pickle.loads(received_data)  # 파일 번호를 역직렬화
            if file_number in virtual_files:
                print(f"Client {client_id} requested file {file_number}")
                send_data(client_socket, virtual_files[file_number])  # 요청된 파일 전송
                print(f"Sent file {file_number} to client{client_id}")
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")

# 캐시 서버의 요청을 처리하는 함수
def handle_cache(cache_socket, cache_id):
    try:
        while True:
            packed_size = cache_socket.recv(8)  # 데이터 크기 수신
            if not packed_size:
                break  # 캐시 서버와 연결 종료되면 루프 탈출
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = b""
            while len(received_data) < data_size:
                packet = cache_socket.recv(4096)  # 데이터를 수신
                received_data += packet

            received_clock,file_number = pickle.loads(received_data)  # 파일 번호를 역직렬화
            if file_number in virtual_files:
                send_data(cache_socket, virtual_files[file_number])  # 요청된 파일 전송
                print(f"Cache Server {cache_id} requested file {file_number}")
    except Exception as e:
        print(f"Error handling cache server {cache_id}: {e}")

# 데이터 서버를 실행하는 메인 함수
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 10000))  # 데이터 서버 포트 바인딩
    server_socket.listen(6)  # 최대 6개의 클라이언트 연결 대기

    print("Data Server started, waiting for connections...")

    # 클라이언트와 통신하는 스레드 풀 (최대 4개 클라이언트 처리)
    with ThreadPoolExecutor(max_workers=4) as client_executor, \
        ThreadPoolExecutor(max_workers=2) as cache_executor:  # 캐시 서버와 통신하는 스레드 풀 (최대 2개 캐시 서버 처리)
        count = 0
        client_id = 0
        cache_id = 0
        
        while True:
            client_socket, addr = server_socket.accept()  # 클라이언트 또는 캐시 서버 연결 대기
            if count < 2:  # 캐시 서버일 경우
                cache_id += 1
                print(f"Connected to Cache Server {cache_id}")
                cache_executor.submit(handle_cache, client_socket, cache_id)  # 캐시 서버 처리 스레드
            else:  # 일반 클라이언트일 경우
                client_id += 1
                print(f"Connected to Client {client_id}")
                client_executor.submit(handle_client, client_socket, client_id)  # 클라이언트 처리 스레드
            count+=1

    server_socket.close()

if __name__ == "__main__":
    main()
