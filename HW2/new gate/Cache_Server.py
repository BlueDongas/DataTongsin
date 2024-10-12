import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor

# 캐시 메모리와 데이터 서버 연결 정보
cache_memory = {}  # 캐시에 저장된 파일 정보를 담는 딕셔너리
cache_size = 200 * 1024  # 캐시 서버의 최대 용량을 200MB로 설정

# 클라이언트 또는 데이터 서버로 데이터를 전송하는 함수
def send_data(sock, data):
    try:
        # 데이터를 직렬화한 후 크기를 먼저 전송하고 데이터를 전송
        serialized_data = pickle.dumps(data)
        data_size = len(serialized_data)
        sock.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
        sock.sendall(serialized_data)  # 실제 데이터 전송
        print(f"Sent data: {data}")
    except Exception as e:
        print(f"Error while sending data: {e}")

# 데이터 서버로 파일을 요청하는 함수
def request_file_from_data_server(data_socket, file_number):
    send_data(data_socket, file_number)  # 데이터 서버에 파일 번호를 요청
    print(f"Requested file {file_number} from Data Server")

# 클라이언트의 요청을 처리하는 함수
def handle_client(client_socket, data_socket, client_id):
    try:
        while True:
            # 클라이언트로부터 파일 번호 요청을 수신
            packed_size = client_socket.recv(8)
            if not packed_size:
                break  # 클라이언트 연결이 종료되면 루프를 빠져나감
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = b""
            while len(received_data) < data_size:
                packet = client_socket.recv(4096)
                received_data += packet

            # 받은 데이터를 역직렬화하여 파일 번호를 얻음
            file_number = pickle.loads(received_data)
            print(f"Client {client_id} requested file {file_number}")

            # 캐시 메모리에서 파일을 찾음 (캐시 히트 또는 미스)
            if file_number in cache_memory:
                # 캐시에 파일이 있으면 클라이언트에 파일 전송
                send_data(client_socket, cache_memory[file_number])
                print(f"Cache hit: Sent file {file_number} to client")
            else:
                # 캐시에 파일이 없으면 데이터 서버에 요청
                request_file_from_data_server(data_socket, file_number)
                # 데이터 서버로부터 파일을 수신한 후 캐시에 저장하고 클라이언트에 전송
                packed_size = data_socket.recv(8)
                data_size = struct.unpack('Q', packed_size)[0]
                file_data = data_socket.recv(data_size)
                cache_memory[file_number] = pickle.loads(file_data)
                send_data(client_socket, cache_memory[file_number])
                print(f"Cache miss: Retrieved and sent file {file_number}")
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")
    finally:
        client_socket.close()

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
    server_socket.listen(5)  # 최대 5개의 클라이언트 연결 대기

    print(f"Cache Server {port} started, waiting for connections...")

    # 데이터 서버와 연결 설정
    data_socket = connect_to_data_server()

    # 스레드 풀을 사용하여 클라이언트 요청을 병렬로 처리
    with ThreadPoolExecutor(max_workers=5) as executor:  # 최대 5개의 클라이언트 동시 처리
        client_id = 0
        while True:
            client_socket, addr = server_socket.accept()  # 클라이언트 연결 대기
            client_id += 1
            print(f"Connected to Client {addr}")
            executor.submit(handle_client, client_socket, data_socket, client_id)

if __name__ == "__main__":
    port = int(input("Enter cache server port (20000 or 30000): "))  # 포트 입력 받기
    cache_server(port)
