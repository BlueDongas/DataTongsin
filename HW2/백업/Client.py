import socket
import pickle
import struct
import random
from concurrent.futures import ThreadPoolExecutor

# 클라이언트에서 서버로 파일 요청을 보내는 함수
def send_request(client_socket, file_number):
    try:
        data = pickle.dumps(file_number)  # 파일 번호를 직렬화
        client_socket.sendall(struct.pack('Q', len(data)))  # 데이터 크기 전송
        client_socket.sendall(data)  # 파일 번호 전송
        print(f"Requested file {file_number}")
    except Exception as e:
        print(f"Error sending request for file {file_number}: {e}")

# 서버로부터 파일을 수신하는 함수
def receive_file(client_socket):
    try:
        packed_size = client_socket.recv(8)  # 데이터 크기를 먼저 받음
        if not packed_size:
            return None  # 연결이 종료된 경우 처리
        data_size = struct.unpack('Q', packed_size)[0]
        file_data = b""
        while len(file_data) < data_size:
            packet = client_socket.recv(4096)  # 데이터를 받음
            file_data += packet
        file_number = pickle.loads(file_data)  # 파일 데이터를 역직렬화
        print(f"Received file {file_number}")
        return file_number
    except Exception as e:
        print(f"Error receiving file: {e}")
        return None

# 클라이언트에서 서버로 요청하고 데이터를 처리하는 함수
def client_task(server_address, port, file_numbers, server_type):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_address, port))  # 서버에 연결
    print(f"Connected to {server_type} on port {port}")

    # 요청할 파일 번호 리스트에 대해 서버에 요청
    for file_number in file_numbers:
        send_request(client_socket, file_number)

        # 서버로부터 파일 수신
        received_file = receive_file(client_socket)
        if received_file:
            print(f"Received file from {server_type}: {received_file}")
        else:
            print(f"Failed to receive file {file_number} from {server_type}")

# 클라이언트가 동시에 데이터 서버와 캐시 서버에 파일 요청을 보내는 함수
def client():
    file_numbers = random.sample(range(1, 10001), 10)  # 1~10000 범위의 파일 번호 중 10개 선택
    data_server_address = ('localhost', 10000)  # 데이터 서버 주소
    cache_server1_address = ('localhost', 20000)  # 캐시 서버 1 주소
    cache_server2_address = ('localhost', 30000)  # 캐시 서버 2 주소

    # 스레드 풀을 이용해 캐시 서버와 데이터 서버에 동시에 요청
    with ThreadPoolExecutor(max_workers=3) as executor:
        # 캐시 서버 1에 요청
        executor.submit(client_task, cache_server1_address[0], cache_server1_address[1], file_numbers[:3], 'Cache Server 1')

        # 캐시 서버 2에 요청
        executor.submit(client_task, cache_server2_address[0], cache_server2_address[1], file_numbers[3:6], 'Cache Server 2')

        # 데이터 서버에 요청
        executor.submit(client_task, data_server_address[0], data_server_address[1], file_numbers[6:], 'Data Server')

if __name__ == "__main__":
    client()
    input()  # 프로그램 종료 방지
